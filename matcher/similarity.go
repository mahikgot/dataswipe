package main

import (
	"strings"

	lev "github.com/texttheater/golang-levenshtein/levenshtein"
)

func tokenize(name string) []string {
	name = strings.ToLower(name)
	// split by underscore and camelCase
	parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	// crude camelCase splitter
	var tokens []string
	for _, p := range parts {
		var cur strings.Builder
		for i, r := range p {
			if i > 0 && r >= 'A' && r <= 'Z' {
				tokens = append(tokens, strings.ToLower(cur.String()))
				cur.Reset()
			}
			cur.WriteRune(r)
		}
		if cur.Len() > 0 {
			tokens = append(tokens, strings.ToLower(cur.String()))
		}
	}
	return tokens
}

func jaccard(tokens1, tokens2 []string) float64 {
	set1 := make(map[string]struct{})
	for _, t := range tokens1 {
		set1[t] = struct{}{}
	}
	set2 := make(map[string]struct{})
	for _, t := range tokens2 {
		set2[t] = struct{}{}
	}

	intersect := 0
	union := make(map[string]struct{})
	for t := range set1 {
		union[t] = struct{}{}
		if _, ok := set2[t]; ok {
			intersect++
		}
	}
	for t := range set2 {
		union[t] = struct{}{}
	}

	if len(union) == 0 {
		return 0.0
	}
	return float64(intersect) / float64(len(union))
}

func columnNameScore(name1, name2 string) float64 {
	n1 := strings.ToLower(name1)
	n2 := strings.ToLower(name2)

	// Exact match or substring
	if n1 == n2 {
		return 1.0
	}
	if strings.Contains(n1, n2) || strings.Contains(n2, n1) {
		return 0.8
	}

	// Token overlap
	t1 := tokenize(name1)
	t2 := tokenize(name2)
	tokenScore := jaccard(t1, t2)

	// Edit distance similarity
	dist := lev.DistanceForStrings([]rune(n1), []rune(n2), lev.DefaultOptions)
	maxLen := float64(len([]rune(n1)))
	if len([]rune(n2)) > len([]rune(n1)) {
		maxLen = float64(len([]rune(n2)))
	}
	editScore := 1.0 - float64(dist)/maxLen

	// Pick the best score
	best := tokenScore
	if editScore > best {
		best = editScore
	}
	return best
}
