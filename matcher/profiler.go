package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type Dtype string

const (
	BigInt      Dtype = "BIGINT"
	Bit         Dtype = "BIT"
	Blob        Dtype = "BLOB"
	Boolean     Dtype = "BOOLEAN"
	Date        Dtype = "DATE"
	Decimal     Dtype = "DECIMAL"
	Double      Dtype = "DOUBLE"
	Float       Dtype = "FLOAT"
	HugeInt     Dtype = "HUGEINT"
	Integer     Dtype = "INTEGER"
	Interval    Dtype = "INTERVAL"
	JSON        Dtype = "JSON"
	SmallInt    Dtype = "SMALLINT"
	Time        Dtype = "TIME"
	TimestampTZ Dtype = "TIMESTAMP WITH TIME ZONE"
	Timestamp   Dtype = "TIMESTAMP"
	TinyInt     Dtype = "TINYINT"
	UBigInt     Dtype = "UBIGINT"
	UHugeInt    Dtype = "UHUGEINT"
	UInteger    Dtype = "UINTEGER"
	USmallInt   Dtype = "USMALLINT"
	UTinyInt    Dtype = "UTINYINT"
	UUID        Dtype = "UUID"
	VarChar     Dtype = "VARCHAR"
)

func IsValidDuckDBType(t Dtype) bool {
	switch t {
	case BigInt, Bit, Blob, Boolean, Date, Decimal, Double, Float,
		HugeInt, Integer, Interval, JSON, SmallInt, Time, TimestampTZ,
		Timestamp, TinyInt, UBigInt, UHugeInt, UInteger, USmallInt,
		UTinyInt, UUID, VarChar:
		return true
	}
	return false
}

type ColumnProfile struct {
	Name      string   `json:"name"`
	DType     Dtype    `json:"dtype"`
	NullPct   float64  `json:"null_pct"`
	UniquePct float64  `json:"unique_pct"`
	Samples   []string `json:"sample_values"`
	Stats     any      `json:"stats"`
}

func (cp ColumnProfile) populateTableInfo(name, dtype string) ColumnProfile {
	cp.Name = name
	if IsValidDuckDBType(Dtype(dtype)) {
		cp.DType = Dtype(dtype)
	}

	return cp
}

func (cp ColumnProfile) populatePcts(nullPct, uniquePct float64) ColumnProfile {
	cp.NullPct = nullPct
	cp.UniquePct = uniquePct
	return cp
}

func (cp ColumnProfile) populateSamples(samples []any) ColumnProfile {
	cp.Samples = make([]string, len(samples))
	for i, s := range samples {
		cp.Samples[i] = fmt.Sprintf("%v", s)
	}
	return cp
}

func runProfile(p ProfileCmd) ([]ColumnProfile, error) {
	return profilePath(p.Path, p.SampleSize)
}

func profilePath(path string, sampleSize int) ([]ColumnProfile, error) {
	filename, err := filename(path)
	if err != nil {
		return nil, err
	}
	db, err := prepareDB(filename)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	cps, err := profile(db, filename, sampleSize)
	if err != nil {
		return []ColumnProfile{}, err
	}

	return cps, nil
}

func prepareDB(filename string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	tableName := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	query := fmt.Sprintf("CREATE TEMP TABLE \"%s\" AS SELECT * FROM read_csv(\"%s\", nullstr = ['null', \"''\"], null_padding = true)", tableName, filename)
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// parallelize the queries
func profile(db *sql.DB, filename string, sampleSize int) ([]ColumnProfile, error) {
	tableName := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))

	cps, err := tableInfo(db, tableName)
	if err != nil {
		return []ColumnProfile{}, err
	}

	cps, err = pcts(db, tableName, cps)
	if err != nil {
		return []ColumnProfile{}, err
	}

	cps, err = samples(db, tableName, sampleSize, cps)
	if err != nil {
		return []ColumnProfile{}, err
	}

	return cps, nil
}

func tableInfo(db *sql.DB, tableName string) ([]ColumnProfile, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, tableName))
	if err != nil {
		return []ColumnProfile{}, err
	}
	defer rows.Close()

	cps := []ColumnProfile{}
	for rows.Next() {
		var name, dtype string
		var dummy any
		if err = rows.Scan(&dummy, &name, &dtype, &dummy, &dummy, &dummy); err != nil {
			return []ColumnProfile{}, err
		}
		cp := ColumnProfile{}.populateTableInfo(name, dtype)
		cps = append(cps, cp)
	}

	return cps, nil
}

func pcts(db *sql.DB, tableName string, cps []ColumnProfile) ([]ColumnProfile, error) {
	var parts []string
	for _, cp := range cps {
		col := cp.Name
		parts = append(parts,
			fmt.Sprintf("100.0 * COUNT(DISTINCT \"%s\") / COUNT(\"%s\") AS \"%s_unique_pct\"", col, col, col),
			fmt.Sprintf("100.0 * SUM(CASE WHEN \"%s\" IS NULL THEN 1 ELSE 0 END)/COUNT(*) AS \"%s_null_pct\"", col, col),
		)
	}
	query := fmt.Sprintf("SELECT %s FROM \"%s\"", strings.Join(parts, ", "), tableName)

	row := db.QueryRow(query)
	vals := make([]any, len(parts))
	for i := range vals {
		var v float64
		vals[i] = &v
	}
	if err := row.Scan(vals...); err != nil {
		return []ColumnProfile{}, err
	}

	for j, cp := range cps {
		uniquePct := *(vals[2*j].(*float64))
		nullPct := *(vals[2*j+1].(*float64))
		cps[j] = cp.populatePcts(nullPct, uniquePct)
	}

	return cps, nil
}

func samples(db *sql.DB, tableName string, sampleSize int, cps []ColumnProfile) ([]ColumnProfile, error) {
	query := fmt.Sprintf("SELECT * FROM \"%s\" USING SAMPLE %d ROWS", tableName, sampleSize)
	rows, err := db.Query(query)
	if err != nil {
		return []ColumnProfile{}, err
	}

	cum := make([][]any, len(cps))
	for rows.Next() {
		vals := make([]any, len(cps))
		ptrs := make([]any, len(cps))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		for i, val := range vals {
			cum[i] = append(cum[i], val)
		}
	}

	for i, samples := range cum {
		cps[i] = cps[i].populateSamples(samples)
	}
	return cps, nil
}
