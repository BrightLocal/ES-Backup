package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"

	gzip "github.com/klauspost/pgzip"
)

type Item struct {
	ID     string           `json:"id"`
	Type   string           `json:"type"`
	Source *json.RawMessage `json:"source"`
}

func main() {
	var (
		hosts string
		index string
		files string
	)
	flag.StringVar(&hosts, "hosts", "", "List of ElasticSearch hosts")
	flag.StringVar(&index, "index", "", "Index to restore")
	flag.StringVar(&files, "files", "", "Files to use")
	flag.Parse()
	if hosts == "" {
		flag.Usage()
		os.Exit(1)
	}
	if index == "" {
		flag.Usage()
		os.Exit(1)
	}
	if files == "" {
		flag.Usage()
		os.Exit(1)
	}
	list, err := filepath.Glob(files)
	if err != nil {
		log.Fatalf("Error getting files list: %s", err)
	}
	if len(list) == 0 {
		log.Fatalf("No files found")
	}
	log.Printf("%# v", list)
	for _, fileName := range list {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Error opening file %q: %s", fileName, err)
		}
		gz, err := gzip.NewReader(file)
		if err != nil {
			log.Fatalf("Error creating uncompressor: %s", err)
		}
		decoder := json.NewDecoder(gz)
		i := 0
		for {
			var line Item
			if err := decoder.Decode(&line); err != nil {
				if err == io.EOF {
					break
				} else if err != nil {
					log.Printf("Error reading from file %q: %s", fileName, err)
					break
				}
			}
			i++
			// TODO
			log.Printf("Line id: %s", line.ID)
		}
		log.Printf("Lines in %q: %d", fileName, i)
	}
}
