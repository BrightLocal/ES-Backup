// ElasticSearch index dumper
// Example usage arguments:
//  -hosts=http://host07:9200,http://host06:9200                          # ES hosts to connect to
//  -index=lpf                                                            # index to dump
//  -out=lpf-2017                                                         # file prefix to use
//  -split=100000                                                         # how many records per file (omit for single file)
//  -query="{\"range\":{\"dateCrawled\":{\"gte\":\"2017-01-01 0:0:0\"}}}" # ES query to use for partial dumps
// Will produce files like:
//  lpf-2017.00000000.gz
//  lpf-2017.00000001.gz
//  lpf-2017.00000002.gz
//  etc
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v5"
)

type Item struct {
	ID     string           `json:"id"`
	Type   string           `json:"type"`
	Source *json.RawMessage `json:"source"`
}

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
	flag.Parse()
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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	args := []elastic.ClientOptionFunc{elastic.SetMaxRetries(10)}
	for _, h := range strings.Split(hosts, ",") {
		args = append(args, elastic.SetURL(h))
	}
	esClient, err := elastic.NewClient(args...)
	if err != nil {
		log.Fatalf("Error connecting to ElasticSearch at %q: %s", hosts, err)
	}
	fileName := fmt.Sprintf("%s.gz", out)
	if split > 0 {
		fileName = fmt.Sprintf("%s.%08d.gz", out, 0)
	}
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating file: %s", err)
	}
	compressor, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
	encoder := json.NewEncoder(compressor)
	var (
		total   int64
		page    int64
		curPage int64
	)
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
		for _, item := range results.Hits.Hits {
			if err := encoder.Encode(Item{
				ID:     item.Id,
				Type:   item.Type,
				Source: item.Source,
			}); err != nil {
				log.Fatalf("Error marshalling: %s", err)
			}
			curPage++
			if split > 0 && split == curPage {
				page++
				curPage = 0
				if err := compressor.Close(); err != nil {
					log.Printf("Error closing compressor file: %s", err)
				}
				if err := file.Close(); err != nil {
					log.Printf("Error closing file: %s", err)
				}
				file, err = os.Create(fmt.Sprintf("%s.%08d.gz", out, page))
				if err != nil {
					log.Fatalf("Error creating file: %s", err)
				}
				compressor, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
				encoder = json.NewEncoder(compressor)
			}
		}
		tf := float64(total)
		log.Printf(
			"Written out %d (%.4f%%) records (%.4f sec per 1k records)",
			total,
			tf*100/float64(results.TotalHits()),
			time.Now().Sub(start).Seconds()/tf*1000,
		)
	}
	compressor.Close()
	file.Close()
	log.Printf("Dumped %d records in %s", total, time.Now().Sub(start).String())
}
