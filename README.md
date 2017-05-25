# ElasticSearch index dumper

Example usage arguments:
```
  -hosts=http://host07:9200,http://host06:9200                          # ES hosts to connect to
  -index=lpf                                                            # index to dump
  -out=lpf-2017                                                         # file prefix to use
  -split=100000                                                         # how many records per file (omit for single file)
  -page=5000                                                            # how many results per scroll to process
  -query="{\"range\":{\"dateCrawled\":{\"gte\":\"2017-01-01 0:0:0\"}}}" # ES query to use for partial dumps
```
Will produce files like:
```
  lpf-2017.00000000.gz
  lpf-2017.00000001.gz
  lpf-2017.00000002.gz
  ...
```
