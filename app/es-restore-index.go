package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"log"
	"os"
	"path/filepath"
	"io"
)

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
		r := bufio.NewReader(gz)
		for {
			line, err := r.ReadBytes(byte('\n'))
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Error reading from file: %s", err)
			}
			// TODO
			log.Printf("Line: %s", line)
		}
	}
}
