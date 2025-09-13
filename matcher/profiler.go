package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	_ "github.com/marcboeker/go-duckdb/v2"
)

type ProfileCmd struct {
	Path       string `arg:"" required:"" name:"path" help:"Path to CSV profile" type:"path"`
	SampleSize int    `arg:"" help:"Rows to sample" default:"-1"`
}

func (p *ProfileCmd) Run() error {
	return runProfile(*p)
}

var cli struct {
	Profile ProfileCmd `cmd:"" help:"Create a profile for CSV"`
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}

type ColumnProfile struct {
	Name        string   `json:"name"`
	DType       string   `json:"dtype"`
	NullPct     float64  `json:"null_pct"`
	UniqueCount float64  `json:"unique_count"`
	Samples     []string `json:"sample_values"`
	Stats       any      `json:"stats"`
}

func (cp ColumnProfile) populateTableInfo(name, dtype string) ColumnProfile {
	cp.Name = name
	cp.DType = dtype
	return cp
}

func (cp ColumnProfile) populateCounts(nullPct, uniqueCount float64) ColumnProfile {
	fmt.Println(nullPct, uniqueCount)
	cp.NullPct = nullPct
	cp.UniqueCount = uniqueCount
	return cp
}

func (cp ColumnProfile) populateSamples(samples []any) ColumnProfile {
	cp.Samples = make([]string, len(samples))
	for i, s := range samples {
		cp.Samples[i] = fmt.Sprintf("%v", s)
	}
	return cp
}

func runProfile(p ProfileCmd) error {
	abs, err := resolvePath(p.Path)
	if err != nil {
		return err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	tableName := strings.TrimSuffix(filepath.Base(abs), filepath.Ext(abs))
	query := fmt.Sprintf("CREATE TABLE %s AS FROM '%s'", tableName, abs)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	cps, err := profile(db, tableName)
	fmt.Println(cps)
	return err
}

// parallelize the queries
func profile(db *sql.DB, tableName string) ([]ColumnProfile, error) {
	cps, err := tableInfo(db, tableName)
	if err != nil {
		return []ColumnProfile{}, err
	}

	cps, err = counts(db, tableName, cps)
	if err != nil {
		return []ColumnProfile{}, err
	}

	cps, err = samples(db, tableName, cps)
	if err != nil {
		return []ColumnProfile{}, err
	}

	for _, cp := range cps {
		fmt.Println(cp)
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

func counts(db *sql.DB, tableName string, cps []ColumnProfile) ([]ColumnProfile, error) {
	var parts []string
	for _, cp := range cps {
		col := cp.Name
		parts = append(parts,
			fmt.Sprintf("COUNT(DISTINCT \"%s\") AS \"%s_unique\"", col, col),
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
		unique := *(vals[2*j].(*float64))
		nullPct := *(vals[2*j+1].(*float64))
		cps[j] = cp.populateCounts(nullPct, unique)
	}

	return cps, nil
}

func samples(db *sql.DB, tableName string, cps []ColumnProfile) ([]ColumnProfile, error) {
	query := fmt.Sprintf("SELECT * FROM \"%s\" USING SAMPLE 5 ROWS", tableName)
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

func resolvePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	if filepath.Ext(abs) != ".csv" {
		return "", fmt.Errorf("not a CSV file: %s", abs)
	}
	return abs, nil
}
