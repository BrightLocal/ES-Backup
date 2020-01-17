// Copyright 2017 BrightLocal Ltd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// ElasticSearch index dumper
// Example usage arguments:
//  -hosts=http://host07:9200,http://host06:9200                          # ES hosts to connect to
//  -index=lpf                                                            # index to dump
//  -out=lpf-2017                                                         # file prefix to use
//  -split=100000                                                         # how many records per file (omit for single file)
//  -query="{\"range\":{\"dateCrawled\":{\"gte\":\"2017-01-01 0:0:0\"}}}" # ES query to use for partial dumps
// Will produce files like:
//  lpf-2017.00000000.json.gz
//  lpf-2017.00000001.json.gz
//  lpf-2017.00000002.json.gz
//  etc

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/BrightLocal/ES-Backup/app/item"
	gzip "github.com/klauspost/pgzip"
	"gopkg.in/olivere/elastic.v5"
)

const (
	ext = "json.gz"
)

var appVersion = "<none>"

func main() {
	var (
		hosts    string
		index    string
		out      string
		split    int64
		pageSize int
		query    string
	)
	flag.StringVar(&hosts, "hosts", "", "List of ElasticSearch hosts")
	flag.StringVar(&index, "index", "", "Index name")
	flag.StringVar(&out, "out", "", "Output file name")
	flag.Int64Var(&split, "split", 0, "How many records per file")
	flag.IntVar(&pageSize, "page", 5000, "Scroll page size")
	flag.StringVar(&query, "query", "", "Query")

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
	if out == "" {
		flag.Usage()
		os.Exit(1)
	}
	args := []elastic.ClientOptionFunc{elastic.SetMaxRetries(10)}
	for _, h := range strings.Split(hosts, ",") {
		args = append(args, elastic.SetURL(h))
	}
	esClient, err := elastic.NewClient(args...)
	if err != nil {
		log.Fatalf("Error connecting to ElasticSearch at %q: %s", hosts, err)
	}
	fileName := fmt.Sprintf("%s."+ext, out)
	if split > 0 {
		fileName = fmt.Sprintf("%s.%08d."+ext, out, 0)
	}
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating file: %s", err)
	}
	var (
		total   int64
		page    int64
		curPage int64
		buf     bytes.Buffer
	)
	encoder := json.NewEncoder(&buf)
	scroller := esClient.Scroll(index)
	scroller.Scroll("1m").Size(pageSize)
	if query != "" {
		log.Printf("%s", query)
		scroller.Body(`{"query":` + query + `}`)
	}
	start := time.Now()
	for {
		results, err := scroller.Do(context.TODO())
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalf("Error scrolling: %s", err)
			}
		}
		total += int64(len(results.Hits.Hits))
		for _, row := range results.Hits.Hits {
			if err := encoder.Encode(item.Record{
				ID:     row.Id,
				Type:   row.Type,
				Source: row.Source,
			}); err != nil {
				log.Fatalf("Error marshalling: %s", err)
			}
			curPage++
			if split == curPage {
				page++
				curPage = 0
				gz, err := gzip.NewWriterLevel(file, gzip.BestCompression)
				if err != nil {
					log.Fatalf("Error creating compressor: %s", err)
				}
				if _, err := buf.WriteTo(gz); err != nil {
					log.Fatalf("Error writing to compressor: %s", err)
				}
				if err := gz.Close(); err != nil {
					log.Fatalf("Error closing compressor: %s", err)
				}
				if err := file.Close(); err != nil {
					log.Fatalf("Error closing file: %s", err)
				}
				file, err = os.Create(fmt.Sprintf("%s.%08d."+ext, out, page))
				if err != nil {
					log.Fatalf("Error creating file: %s", err)
				}
				buf.Reset()
			}
		}
		tf := float64(total)
		log.Printf(
			"Processed %d (%.4f%%) records (%.0f records per second)",
			total,
			tf*100/float64(results.TotalHits()),
			tf/time.Now().Sub(start).Seconds(),
		)
	}
	gz, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		log.Fatalf("Error creating compressor: %s", err)
	}
	if _, err := buf.WriteTo(gz); err != nil {
		log.Fatalf("Error writing to compressor: %s", err)
	}
	if err := gz.Close(); err != nil {
		log.Fatalf("Error closing compressor: %s", err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("Error closing file: %s", err)
	}
	log.Printf("Dumped %d records in %s", total, time.Now().Sub(start).String())
}
