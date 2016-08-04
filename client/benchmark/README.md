Steps to execute the benchmark:

1. Start Elasticsearch on the target host (ideally *not* on the same machine)
2. Create an empty index with the mapping you want to benchmark
3. Build an uberjar with `gradle :client:benchmark:shadowJar` and execute it.
4. Delete the index
5. Repeat steps 2. - 4. for multiple iterations. The first iterations are intended as warmup for Elasticsearch itself. Always start the same benchmark in step 3!
4. After the benchmark: Shutdown Elasticsearch and delete the data directory

Repeat all steps above for the other benchmark candidate.

Example benchmark:

* Download benchmark data from http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2 and decompress
* Use the mapping file https://github.com/elastic/rally-tracks/blob/master/geonames/mappings.json to create the index

Example command line parameter list:

```
rest 192.168.2.2 /home/your_user_name/.rally/benchmarks/data/geonames/documents.json geonames type 8647880 5000 "{ \"query\": { \"match_phrase\": { \"name\": \"Sankt Georgen\" } } }\""
```

The parameters are in order:

* Client type: Use either "rest" or "transport"
* Benchmark target host IP (the host where Elasticsearch is running)
* full path to the file that should be bulk indexed
* name of the index
* name of the (sole) type in the index 
* number of documents in the file
* bulk size
* a search request body (remember to escape double quotes). The `TransportClientBenchmark` uses `QueryBuilders.wrapperQuery()` internally which automatically adds a root key `query`, so it must not be present in the command line parameter. 
 
You should also define a few GC-related settings `-Xms4096M -Xmx4096M  -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails` and keep an eye on GC activity. You can also define `-XX:+PrintCompilation` to see JIT activity.
