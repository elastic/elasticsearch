### Steps to execute the benchmark

1. Build `client-benchmark-noop-api-plugin` with `gradle :client:client-benchmark-noop-api-plugin:assemble`
2. Install it on the target host with `bin/elasticsearch-plugin install file:///full/path/to/client-benchmark-noop-api-plugin.zip`
3. Start Elasticsearch on the target host (ideally *not* on the same machine)
4. Build an uberjar with `gradle :client:benchmark:shadowJar` and execute it.

Repeat all steps above for the other benchmark candidate.

### Example benchmark

In general, you should define a few GC-related settings `-Xms8192M -Xmx8192M -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails` and keep an eye on GC activity. You can also define `-XX:+PrintCompilation` to see JIT activity.

#### Bulk indexing

Download benchmark data from http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2 and decompress them.

Example command line parameters:

```
rest bulk 192.168.2.2 ./documents.json geonames type 8647880 5000
```

The parameters are in order:

* Client type: Use either "rest" or "transport"
* Benchmark type: Use either "bulk" or "search"
* Benchmark target host IP (the host where Elasticsearch is running)
* full path to the file that should be bulk indexed
* name of the index
* name of the (sole) type in the index 
* number of documents in the file
* bulk size


#### Bulk indexing

Example command line parameters:

```
rest search 192.168.2.2 geonames "{ \"query\": { \"match_phrase\": { \"name\": \"Sankt Georgen\" } } }\"" 500,1000,1100,1200
```

The parameters are in order:

* Client type: Use either "rest" or "transport"
* Benchmark type: Use either "bulk" or "search"
* Benchmark target host IP (the host where Elasticsearch is running)
* name of the index
* a search request body (remember to escape double quotes). The `TransportClientBenchmark` uses `QueryBuilders.wrapperQuery()` internally which automatically adds a root key `query`, so it must not be present in the command line parameter.
* A comma-separated list of target throughput rates


