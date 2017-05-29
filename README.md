# ElasticSearch index dumper

Easily dump/restore complete or partial indices to/from compressed JSON files.

Each index entry will be represented by a line:
```
{"id":"<id>","type":"<type>","source":{<source>}}
```

You can use `query` argument to specify ElasticSearch query in JSON format to filter out records you want to backup.

Example usage arguments:
```
-hosts=http://host07:9200,http://host06:9200                          # ES hosts to connect to
-index=my-index                                                       # index to dump
-out=my-index-2017                                                    # file prefix to use
-split=100000                                                         # how many records per file (omit for single file, see note below)
-page=5000                                                            # how many results per scroll to process
-query="{\"range\":{\"dateCrawled\":{\"gte\":\"2017-01-01 0:0:0\"}}}" # ES query to use for partial dumps
```
Note, that records are kept in memory before they are written to file in compressed form, so it is feasible to specify decent `split` argument to avoid memory exhaustion for really big indices. 

Will produce files like:
```
lpf-2017.00000000.json.gz
lpf-2017.00000001.json.gz
lpf-2017.00000002.json.gz
...
```

# Index restorer

Example usage arguments:
```
-hosts=http://host07:9200,http://host06:9200                          # ES hosts to connect to
-index=lpf                                                            # index to restore
-files=index.*.json.gz                                                # glob file mask
```
