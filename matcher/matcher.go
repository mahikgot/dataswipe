package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
)

type ColumnProfileID string

func NewColumnProfileID(cp ColumnProfile) ColumnProfileID {
	b, err := json.Marshal(cp)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return ColumnProfileID(hex.EncodeToString(h[:]))
}

type ColumnProfilePair struct {
	Left, Right ColumnProfileID
}

func NewColumnProfilePair(left, right ColumnProfile) ColumnProfilePair {
	leftID := NewColumnProfileID(left)
	rightID := NewColumnProfileID(right)
	return ColumnProfilePair{leftID, rightID}
}

type ColumnProfilePairScores struct {
	Score       float64
	Left, Right ColumnProfile
}

func runMatch(m MatchCmd) ([]ColumnProfilePairScores, error) {
	leftCps, err := profilePath(m.LeftPath, m.SampleSize)
	if err != nil {
		return []ColumnProfilePairScores{}, err
	}
	rightCps, err := profilePath(m.RightPath, m.SampleSize)
	if err != nil {
		return []ColumnProfilePairScores{}, err
	}

	scores := matchProfile(leftCps, rightCps)
	return scores, nil
}

func matchProfile(leftCps, rightCps []ColumnProfile) []ColumnProfilePairScores {
	scores := make(map[ColumnProfilePair]ColumnProfilePairScores)
	for _, left := range leftCps {
		for _, right := range rightCps {
			cpp := NewColumnProfilePair(left, right)
			if _, exists := scores[cpp]; exists {
				continue
			}
			scores[cpp] = match(left, right)
		}
	}
	var results []ColumnProfilePairScores
	for _, v := range scores {
		results = append(results, ColumnProfilePairScores{Left: v.Left, Right: v.Right, Score: v.Score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score // descending
	})
	return results
}

func match(left, right ColumnProfile) ColumnProfilePairScores {
	typeScore := baseTypeScore(left.DType, right.DType)
	nullScore := nullSimilarityScore(left.NullPct, right.NullPct)
	score := typeScore * nullScore
	return ColumnProfilePairScores{score, left, right}
}

func nullSimilarityScore(left, right float64) float64 {
	return 1 - (math.Abs(left-right) / 100)
}

func baseTypeScore(a, b Dtype) float64 {
	if a == b {
		return 1.0
	}

	if sameFamily(a, b) {
		return 0.8
	}

	if castableLossy(a, b) {
		return 0.3
	}

	return 0.0
}

func sameFamily(a, b Dtype) bool {
	numeric := map[Dtype]bool{
		BigInt: true, HugeInt: true, Integer: true, SmallInt: true, TinyInt: true,
		UBigInt: true, UHugeInt: true, UInteger: true, USmallInt: true, UTinyInt: true,
		Decimal: true, Double: true, Float: true,
	}
	text := map[Dtype]bool{
		VarChar: true, UUID: true, JSON: true,
	}
	temporal := map[Dtype]bool{
		Date: true, Timestamp: true, TimestampTZ: true, Time: true, Interval: true,
	}

	switch {
	case numeric[a] && numeric[b]:
		return true
	case text[a] && text[b]:
		return true
	case temporal[a] && temporal[b]:
		return true
	}
	return false
}

func castableLossy(a, b Dtype) bool {
	// Allow string ↔ numeric, string ↔ temporal as "lossy"
	text := map[Dtype]bool{
		VarChar: true, UUID: true, JSON: true,
	}
	numeric := map[Dtype]bool{
		BigInt: true, HugeInt: true, Integer: true, SmallInt: true, TinyInt: true,
		UBigInt: true, UHugeInt: true, UInteger: true, USmallInt: true, UTinyInt: true,
		Decimal: true, Double: true, Float: true,
	}
	temporal := map[Dtype]bool{
		Date: true, Timestamp: true, TimestampTZ: true, Time: true, Interval: true,
	}

	if (text[a] && numeric[b]) || (numeric[a] && text[b]) {
		return true
	}
	if (text[a] && temporal[b]) || (temporal[a] && text[b]) {
		return true
	}
	return false
}
