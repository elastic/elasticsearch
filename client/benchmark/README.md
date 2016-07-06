Steps to execute the benchmark:

1. Start Elasticsearch on the target host (ideally *not* on the same machine)
2. Create an empty index with the mapping you want to benchmark
3. Start either the RestClientBenchmark class or the TransportClientBenchmark
4. Delete the index
5. Repeat steps 2. - 4. for multiple iterations. The first iterations are intended as warmup for Elasticsearch itself. Always start the same benchmark in step 3!
4. After the benchmark: Shutdown Elasticsearch and delete the data directory

Repeat all steps above for the other benchmark candidate.

Example benchmark:

* Download benchmark data from http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2 and decompress
* Use the mapping file https://github.com/elastic/rally-tracks/blob/master/geonames/mappings.json to create the index