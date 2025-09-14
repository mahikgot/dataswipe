package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/alecthomas/kong"
)

var cli struct {
	Profile ProfileCmd `cmd:"" help:"Create a profile for CSV"`
}

type ProfileCmd struct {
	Path       string `arg:"" required:"" name:"path" help:"Path to CSV profile" type:"path"`
	SampleSize int    `arg:"" help:"Rows to sample" default:"1000"`
}

func (p *ProfileCmd) Run() error {
	cps, err := runProfile(*p)
	if err != nil {
		return err
	}

	data, err := json.Marshal(cps)
	fmt.Println(string(data))

	return err
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}

func filename(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	if filepath.Ext(abs) != ".csv" {
		return "", fmt.Errorf("not a CSV file: %s", abs)
	}
	return abs, nil
}
