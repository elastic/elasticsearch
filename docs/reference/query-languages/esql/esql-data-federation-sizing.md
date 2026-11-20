---
navigation_title: "Sizing and performance"
description: "Size your Elasticsearch cluster for ES|QL Data Federation workloads, by file format, compression, and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Size your {{es}} cluster for {{esql}} Data Federation workloads

With [data federation](esql-data-federation.md), you can query tabular data held in object storage — CSV, TSV, NDJSON and Parquet files — without ingesting it into {{es}} first.

This guide covers how much heap and how many cores a node needs to run your queries at the concurrency you expect, and how to lay out your files so those cores stay busy.

For data held in {{es}} indices, refer to the [Size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) guide instead.

## General guidelines [general-guidelines]

* **Prefer Parquet to the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays splittable when compressed.
* **Prefer mid-sized files**, from tens of megabytes to several hundred megabytes.
* **Specify a mapping** on text datasets, with `dynamic` set to `false`.
* **Start at 2 GB of heap** for Parquet, **2 GB to 4 GB** for uncompressed text, and **4 GB** for compressed text. Add a further **2 GB to 4 GB** for a text dataset whose schema is inferred.
* **Allocate 1 GB to 2 GB of heap per core.**
* **Add 1 core per 3 to 4 concurrent queries** on text, and 1 core per 8 to 10 on Parquet.
* **Do not raise heap past 31 GB.** Scale horizontally across nodes instead.

These figures are starting points. They assume cold reads, where every query fetches its data from object storage rather than from a cache. How a dataset behaves then depends on characteristics of your own data — how well it compresses, how its files split, the row group sizes in your Parquet files, the width of your schema, and the size and number of your objects — so a configuration that suits one dataset may not suit another. Size against your own queries, at the concurrency you expect, and watch how the cluster behaves.

## Background [background]

Data federation and index workloads share the same query engine, but their read paths differ. Aggregation, filtering, sorting and the rest of the {{esql}} pipeline are the same code, so a dataset supports the same commands and functions an index does, with [known exceptions](esql-data-federation-querying.md#limitations).

What differs is the work of reading data that lives outside the cluster. A few stages exist only on this path, and they are what affect memory allocation and the performance of your queries:

* **Schema inference.** Text files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset [specifies a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Split distribution.** The files a query reads are divided into units of work called splits, sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and those splits are distributed across the data nodes. How many a node reads at once follows its core count.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed split expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

The formats then diverge. Parquet carries its schema in the file footers, so no sampling is needed, and it is read chunk by chunk: a reader takes one row group at a time and, within it, fetches and decodes only the columns the query names. Both the bytes read and the work done follow your projection rather than the width of the file, so narrowing a query with [`KEEP`](/reference/query-languages/esql/commands/keep.md) reads less.

The text formats behave the other way round. A file is read as a continuous stream, and a row must be parsed in full before any of its columns can be used. Narrowing a text query therefore does not read less: the same bytes are fetched, decompressed and parsed either way, and a projection saves only the building of the result columns.

Parallelization works differently depending on the format, because each one divides a file into a different unit of work. For Parquet that unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

It is important to note that the heap a cluster needs does not depend on the size of your dataset. Data is streamed through the cluster rather than loaded into it, so what matters is how much is in flight at any moment. A dataset of 10,000 objects needs much the same heap as one of 100. What the size of the dataset affects is the time a query takes, and therefore the cores you need to meet a latency target.

## Size your files for your workload [file-layout]

File layout is a property of your data rather than of your cluster.

Aim for mid-sized files, from tens of megabytes to several hundred megabytes. Too few leave cores idle, and for compressed text that is a limit rather than a preference, because one file is one reader. Too many cost more than they return, because every object is reached by its own high-latency request. The object count is also capped by [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files), past which a query fails rather than running slowly.

Compression reduces what every query fetches, which is both faster and cheaper, but for text it costs parallelism. Compressed Parquet stays splittable, and for Parquet the lever is the row group rather than the file, so leave row group sizes at their defaults unless your files are very wide.

## Sizing your cluster [sizing]

The general model is that you allocate memory for {{esql}} itself and the rest for {{es}}, and enough memory on the coordinating node to execute planning and schema inference. Further memory is needed to hold the concurrent streams a query reads.

Parquet currently trades memory for performance quite flexibly. The text formats require additional heap to be allocated for concurrent queries.

Adding cores increases parallelism and increases memory allocation with it, so heap must be adjusted per core. For compressed text the per-core requirement itself rises as cores are added, so heap and cores cannot be scaled independently.

### Where to start [heap-per-node]

Start with:

* **2 GB of heap** for Parquet
* **2 GB to 4 GB of heap** for uncompressed text
* **4 GB of heap** for compressed text
* **A further 2 GB to 4 GB** for a text dataset whose schema is inferred

Then allocate 1 GB to 2 GB of heap per core.

### Sizing for concurrency [concurrency]

Query execution time depends on how many queries run at once, and scales with the cores available to them. The total time a query takes follows that concurrency and the size of the dataset it reads.

On a text dataset, add **1 core per 3 to 4 concurrent queries**, with the heap that core requires. For uncompressed text a wider node serves that concurrency. For compressed text it does not, because the per-core requirement rises as cores are added, so add nodes instead.

Parquet needs fewer cores for the same concurrency: **1 core per 8 to 10 concurrent queries**.

Scale horizontally. More nodes give more parallelism, and individual nodes should not be scaled past 31 GB of heap.

### Specify a mapping on text datasets [declare-schema]

[Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) on text datasets, and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`, which terminates the inference path.

Inference is the largest single cost you can remove outright, and on a wide schema it is the first thing to fail.

### Lower `esql.external.max_concurrent_requests` before adding heap [lower-concurrency]

This setting controls how many storage requests a node keeps in flight. Lowering it is the reliable remedy when text queries are being refused, at the cost of some wall-clock time.

The default of [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is derived from the node's cores. The setting is node-scoped, so a change takes effect after a restart.

## Sizing for performance [performance]

Data federation queries scale with the number of cores, and their scan time is proportional to the size of the dataset being read. This is where dataset size enters: it sets the work, not the heap.

If you want your queries to run faster, add cores, and scale the heap with them at 1 GB to 2 GB per core. Cores added without the heap to match will not help. For compressed text, add those cores as more nodes rather than as a wider one.

## Example configurations [examples]

Both of these size a cluster for 10 concurrent queries.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as 1 Parquet file per month, of 50 MB to 110 MB. 5 years is about 60 files.

Parquet, so the initial heap is 2 GB. At 1 core per 8 to 10 concurrent queries, 10 concurrent queries need 2 cores. At 1 GB to 2 GB of heap per core, 2 cores need 2 GB to 4 GB.

Start with 1 node of 2 cores and 4 GB of heap.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as 1 gzipped CSV file per day, with a wide schema of SMART attributes.

Compressed text, so the initial heap is 4 GB. At 1 core per 3 to 4 concurrent queries, 10 concurrent queries need 3 to 4 cores. At 1 GB to 2 GB of heap per core, 4 cores need 4 GB to 8 GB. Compressed text serves fewer concurrent queries as a node widens, so those cores belong on separate nodes.

Start with 2 nodes of 2 cores and 8 GB of heap each.

With the schema inferred, add 2 GB to 4 GB of heap on the coordinating node.

## Troubleshoot [troubleshoot]

Because the figures on this page are starting points, the configuration you end up with should be the one your own data asks for. Watch for the following as you load it up:

* **Circuit breaker rejections**, which mean the node is holding more than it can collect.
* **Queries slowing as concurrency rises** without any rejection, which means readers are contended rather than exhausted.
* **Query times that do not improve when you add capacity**, which points at the file layout rather than the cluster: too few files, or compressed text files too large to divide, leave the extra cores with nothing to pick up.
* **Planning time growing with your file count**, which points at schema inference or, on Parquet, at footer reads on the coordinating node.

### `circuit_breaking_exception` with `Data too large` [breaker]

A circuit breaker has rejected the query because the node is holding more than it can collect. Give the instance more memory, and raise the heap with it — and if you add cores at the same time, scale the heap with those too, at 1 GB to 2 GB per core.

Two things are worth checking before you do. On a text dataset, the usual cause is schema inference at planning time, so [specify a mapping](#declare-schema) with `dynamic` set to `false` first — that removes the cost rather than paying for it. And lowering [`esql.external.max_concurrent_requests`](#lower-concurrency) will clear the condition at the cost of some wall-clock time, which is useful if more memory is not available to you.

Not all of the memory this workload holds is attributed to the query that allocated it, so the error may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### Queries are slow under load [slow-under-load]

Queries that still succeed but take longer as load rises are contended rather than exhausted: the readers are shared, and there are not enough of them to go round.

To improve query performance under load:

* **Add cores, and scale the heap with them** at 1 GB to 2 GB per core. For compressed text, add those cores as more nodes rather than as a wider one.
* **Check your file layout.** Too few files, or compressed text files too large to divide, leave cores idle whatever the size of the node.
* **Narrow the query.** On Parquet, [`KEEP`](/reference/query-languages/esql/commands/keep.md) cuts both the bytes fetched and the work done. On text it saves less, because whole rows are read either way.
* **Specify a mapping** on text datasets, which takes schema inference out of planning.
* **Lower [`esql.external.max_concurrent_requests`](#lower-concurrency)** if the slowdown comes with breaker rejections, trading some wall-clock time for stability.

## Related pages [related]

- [Query external datasets](esql-data-federation-querying.md): how the engine reduces storage reads, and how to query datasets alongside indices
- [Schema inference](esql-data-federation-schema.md): how {{es}} discovers schemas, and how to specify a mapping instead
- [Dataset settings](esql-data-federation-dataset-settings.md): file discovery, parsing, and reader behavior, including [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size)
- [Cluster settings](esql-data-federation-cluster-settings.md): request concurrency, file-discovery limits, and caching
- [Optimize {{esql}} query performance](esql-query-performance.md): general {{esql}} tuning, which applies to datasets too
- [Costs](esql-data-federation-costs.md): what your storage provider bills for the reads a query generates
