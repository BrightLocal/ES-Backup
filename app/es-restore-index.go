// Copyright 2017 BrightLocal Ltd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BrightLocal/ES-Backup/app/item"
	gzip "github.com/klauspost/pgzip"
	"github.com/olivere/elastic/v7"
)

var appVersion = "<none>"

func main() {
	var (
		hosts string
		index string
		files string
	)
	flag.StringVar(&hosts, "hosts", "", "List of ElasticSearch hosts")
	flag.StringVar(&index, "index", "", "Index to restore")
	flag.StringVar(&files, "files", "", "Files to use")

	getVersion := flag.Bool("version", false, "Get version")
	flag.Parse()
	if *getVersion {
		fmt.Println(appVersion)
		return
	}

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
	log.Printf("Importing from %d file(s)", len(list))
	args := []elastic.ClientOptionFunc{elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewConstantBackoff(5 * time.Second)))}
	for _, h := range strings.Split(hosts, ",") {
		args = append(args, elastic.SetURL(h))
	}
	esClient, err := elastic.NewClient(args...)
	if err != nil {
		log.Fatalf("Error connecting to ElasticSearch at %q: %s", hosts, err)
	}
	start := time.Now()
	total := 0
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
		bs := elastic.NewBulkService(esClient)
		for {
			var line item.Record
			if err := decoder.Decode(&line); err != nil {
				if err == io.EOF {
					break
				} else if err != nil {
					log.Printf("Error reading from file %q: %s", fileName, err)
					break
				}
			}
			i++
			total++
			bs.Add(elastic.NewBulkUpdateRequest().Index(index).Type(line.Type).Id(line.ID).DocAsUpsert(true).Doc(line.Source))
			if bs.EstimatedSizeInBytes() > 10*1024*1024 {
				if resp, err := bs.Do(context.TODO()); err != nil {
					log.Fatalf("Error during bulk upsert: %s", err)
				} else if resp.Errors {
					for _, rr := range resp.Failed() {
						log.Printf("Error: %s", rr.Error.Reason)
					}
					log.Fatal()
				}
				log.Printf("Records inserted: %d", total)
			}
		}
		if resp, err := bs.Do(context.TODO()); err != nil {
			log.Fatalf("Error during bulk upsert: %s", err)
		} else if resp.Errors {
			for _, rr := range resp.Failed() {
				log.Printf("Error: %s", rr.Error.Reason)
			}
			log.Fatal()
		}
		log.Printf("Records in %q: %d", fileName, i)
	}
	log.Printf("%d records processed in %s", total, time.Now().Sub(start).String())
}
