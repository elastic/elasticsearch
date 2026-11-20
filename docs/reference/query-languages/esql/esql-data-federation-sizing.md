---
navigation_title: "Sizing and performance"
description: "Size your Elasticsearch cluster for ES|QL Data Federation workloads, by file format, compression, and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# Size your {{es}} cluster for {{esql}} Data Federation workloads

With [data federation](esql-data-federation.md), you can query tabular data held in object storage, in CSV, TSV, NDJSON and Parquet files, without ingesting it into {{es}} first.

This guide covers how much heap and how many cores your cluster needs to run your queries at the concurrency you expect, and how to lay out your files so those cores stay busy.

For data held in {{es}} indices, refer to the [Size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) guide instead.

## General guidelines [general-guidelines]

* **Prefer Parquet to the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays splittable when compressed.
* **Prefer mid-sized files**, from tens of megabytes to several hundred megabytes.
* **Prefer an explicitly declared schema for text datasets.** [Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`.
* **Start at 2 GB of heap** for Parquet, **2 GB to 4 GB** for uncompressed text, and **4 GB** for compressed text. Add a further **2 GB to 4 GB** for a text dataset whose schema is inferred.
* **Allocate 1 GB to 2 GB of heap per core.**
* **Add 1 core per 3 to 4 concurrent queries** on text, and 1 core per 8 to 10 on Parquet.
* **Do not raise heap past 31 GB.** Scale horizontally across nodes instead.

These figures are starting points. They assume cold reads, where every query fetches its data from object storage. How a dataset behaves then depends on characteristics of your own data: how well it compresses, how its files split, the row group sizes in your Parquet files, the width of your schema, and the size and number of your objects. A configuration that suits one dataset may not suit another. Size against your own queries, at the concurrency you expect, and watch how the cluster behaves.

## Background [background]

Data federation and index workloads share the same query engine, but their read paths differ. Aggregation, filtering, sorting and the rest of the {{esql}} pipeline are the same code, so a dataset supports the same commands and functions an index does, with [known exceptions](esql-data-federation-querying.md#limitations).

What differs is the work of reading data that lives outside the cluster. A few stages exist only on this path, and they are what affect memory allocation and the performance of your queries:

* **Schema inference.** Text files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset [specifies a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Split distribution.** The files a query reads are divided into units of work called splits, sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and those splits are distributed across the data nodes. How many a node reads at once follows its core count.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed split expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

The formats then diverge. Parquet carries its schema in the file footers, so no sampling is needed, and it is read chunk by chunk: a reader takes one row group at a time and, within it, fetches and decodes only the columns the query names. Both the bytes read and the work done follow your projection rather than the width of the file, so narrowing a query with [`KEEP`](/reference/query-languages/esql/commands/keep.md) reads less.

The text formats work differently. A file is read as a continuous stream, and a row must be parsed in full before any of its columns can be used. Narrowing a text query therefore does not read less: the same bytes are fetched, decompressed and parsed either way, and a projection saves only the building of the result columns.

Parallelization works differently depending on the format, because each one divides a file into a different unit of work. For Parquet that unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

The heap a cluster needs does not depend on the size of your dataset. Data is streamed through the cluster rather than loaded into it, so what matters is how much is in flight at any moment. A dataset of 10,000 objects needs much the same heap as one of 100. What the size of the dataset affects is the time a query takes, and therefore the cores you need to meet a latency target.

## Size your files for your workload [file-layout]

File layout is a property of your data rather than of your cluster.

Aim for mid-sized files, from tens of megabytes to several hundred megabytes.

Object storage has high latency on every request, so a query gets its throughput from reading many files in parallel rather than from any single read being fast. That cuts both ways. If a dataset holds too few files, there is not enough work to share out and some of your cores sit idle however large the cluster is. If it holds too many, each one still costs a full request, and a query over very small files spends more of its time waiting on those requests than reading data.

There is also a hard limit on how many files one dataset can hold, set by [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files). A query over a dataset above that limit fails, so either keep the dataset within it, by narrowing the resource pattern or splitting the data across several datasets, or raise the setting.

Compressing your files reduces what every query has to fetch, which makes queries faster and reduces what your storage provider bills you. For the text formats it costs you parallelism, because a compressed text file cannot be read from the middle and so is handled by a single reader.

Parquet is not affected, because it compresses each column chunk inside the file and stays divisible. For Parquet the unit to think about is the row group rather than the file, so leave row group sizes at their defaults unless your files are very wide.

## Sizing your cluster [sizing]

A node running federated queries needs memory for three things at once: for {{esql}} and the rest of {{es}}, for the planning and schema inference that happen on the coordinating node before any data is read, and for the splits the node is reading at that moment. The sections below size each of them.

The format you query makes a large difference here. Parquet needs less heap than the text formats to do the same work, and less again for each query you run alongside it.

Cores matter as much as heap, and the two move together. Adding cores lets a node read more splits at once, and each of those splits needs memory to hold, so a node given more cores needs more heap to go with them.

### Where to start [heap-per-node]

Start with:

* **2 GB of heap** for Parquet
* **2 GB to 4 GB of heap** for uncompressed text
* **4 GB of heap** for compressed text
* **A further 2 GB to 4 GB** for a text dataset whose schema is inferred

Then allocate 1 GB to 2 GB of heap per core, on at least 2 cores per node.

### Sizing for concurrency [concurrency]

Queries that run at the same time share a node's cores between them, so the way to serve more of them is to add cores, and the heap those cores need.

On a text dataset, allow about **1 core for every 3 to 4 concurrent queries**. For uncompressed text you can add those cores to a node you already have. For compressed text you should not: what a node needs per core grows as the node gets wider, so a larger node ends up serving fewer concurrent queries rather than more. Add nodes instead.

Parquet is less demanding, and needs about **1 core for every 8 to 10 concurrent queries**.

Whichever format you are reading, more nodes give you more parallelism, and no single node should be given more than 31 GB of heap.

### Specify a mapping on text datasets [declare-schema]

CSV, TSV and NDJSON files do not describe their own columns, so {{es}} works the schema out by sampling rows before the query runs. You can avoid that cost entirely by declaring the schema yourself.

[Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) on the dataset and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`. Both are needed: a mapping on its own still leaves the rest of the schema to be inferred, and it is `dynamic: false` that stops the inference.

### Increase read concurrency for more parallelism [read-concurrency]

[`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) sets how many requests to the object store a node has outstanding at any moment. Its default is worked out from the node's core count.

Raising it gives you more read parallelism. Raise the heap at the same time: every request in flight is holding data that is being staged and decompressed, so a node given more read concurrency and no more memory will trip a [circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#parent-circuit-breaker). This is a [static setting](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), so the node has to be restarted before a change takes effect.

### Sizing for {{ech}} [sizing-ech]

On {{ech}} the unit you choose is a size per zone for each data tier, given in RAM, together with a hardware profile for the deployment. Heap and cores both follow from that choice: heap is half the node's RAM, and the cores come from the profile's vCPU to RAM ratio.

To apply the figures above, **select a size per zone of twice the heap you need**, then check that the vCPU count shown for that size meets your core figure. Sizing this way leaves you with more heap per core than this page asks for, so on {{ech}} it is the cores that bind rather than the memory.

For the Parquet example, 2 cores and 4 GB of heap is an 8 GB size per zone, in 1 zone. For the compressed CSV example, 2 nodes of 2 cores and 4 GB of heap each is an 8 GB size per zone across 2 zones.

Hardware profiles differ in how many cores come with a given amount of RAM. A CPU optimized profile gives the most and a storage optimized profile the fewest, so a storage optimized deployment needs a larger size per zone to reach the same core count.

The smallest sizes run on temporarily boosted CPU, and briefly have more processing available than they report. Size on the core count rather than on the boost.

### Sizing for {{es-serverless}} [sizing-serverless]

On [{{serverless-full}}](docs-content://deploy-manage/deploy/elastic-cloud/serverless.md) you do not choose nodes, heap or cores. The unit is the virtual compute unit, and the Search tier scales with your load. There is no published conversion from heap or cores to VCUs, so the figures above do not translate directly.

What you can set is a floor. [Minimum Search VCUs](docs-content://deploy-manage/deploy/elastic-cloud/project-settings.md#minimum-search-vcus) stops the Search tier scaling below a value you choose, even while the project is idle, which is how you hold capacity ready for a known query concurrency. Raise it 30 to 60 minutes ahead of an expected increase in federated query load. You are billed for at least that many Search VCUs for as long as it is set, so lower it again once the load has passed.

If you set a high minimum, raise [Search Power](docs-content://deploy-manage/deploy/elastic-cloud/project-settings.md#elasticsearch-manage-project-search-power-settings) alongside it, to make use of the capacity you have pre-scaled.

The guidance on [file layout](#file-layout) and on [specifying a mapping](#declare-schema) applies on serverless unchanged, because both are properties of your data rather than of the cluster.

## Sizing for performance [performance]

Data federation queries scale with the number of cores, and their scan time is proportional to the size of the dataset being read.

If you want your queries to run faster, add cores, and scale the heap with them at 1 GB to 2 GB per core, plus the budget for your [concurrent queries](#concurrency). Cores added without the heap to match will not help. For compressed text, add those cores as more nodes rather than as a wider one.

## Example configurations [examples]

Both of these size a cluster for 10 concurrent queries.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as 1 Parquet file per month, of 50 MB to 110 MB. 5 years is about 60 files.

Parquet, so the initial heap is 2 GB. At 1 core per 8 to 10 concurrent queries, 10 concurrent queries need 1 to 2 cores, so the 2-core minimum governs. At 1 GB to 2 GB of heap per core, 2 cores need 2 GB to 4 GB.

Start with 1 node, with at least 2 cores and at least 4 GB of heap.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as 1 gzipped CSV file per day, with a wide schema of SMART attributes.

Compressed text, so the initial heap is 4 GB. At 1 core per 3 to 4 concurrent queries, 10 concurrent queries need 3 to 4 cores. Compressed text serves fewer concurrent queries as a node widens, so those cores belong on separate nodes: 2 nodes of 2 cores each. At 1 GB to 2 GB of heap per core, 2 cores need 2 GB to 4 GB, so the initial 4 GB governs.

Start with 2 nodes, each with at least 2 cores and at least 4 GB of heap.

With the schema inferred, add 2 GB to 4 GB of heap.

## Troubleshoot [troubleshoot]

Because the figures on this page are starting points, the configuration you end up with should be the one your own data asks for. Watch for the following as you load it up:

* **Circuit breaker rejections**, which mean there is insufficient memory for the workload.
* **Queries slowing as concurrency rises** without any rejection, which means readers are contended rather than exhausted.
* **Query times that do not improve when you add capacity**, which points at the file layout rather than the cluster: too few files, or compressed text files too large to divide, leave the extra cores with nothing to pick up.
* **Planning time growing with your file count**, which points at schema inference or, on Parquet, at footer reads on the coordinating node.

### `circuit_breaking_exception` with `Data too large` [breaker]

A circuit breaker has rejected the query because there is insufficient memory for the workload. Give the instance more memory, and raise the heap with it. If you add cores at the same time, scale the heap with those too, at 1 GB to 2 GB per core.

On a text dataset, the usual cause is schema inference at planning time. [Specify a mapping](#declare-schema) with `dynamic` set to `false` first, which removes that cost instead of paying for it.

If you have raised [`esql.external.max_concurrent_requests`](#read-concurrency), raise the heap to match it.

The figures on this page cover reading the external data. {{esql}} needs further memory for the query itself, for aggregations, sorting and grouping, so allow for that on top. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### Queries are slow under load [slow-under-load]

Queries that still return results but take longer as load rises are competing with each other for readers. Nothing has run out; there are simply not enough readers to go round, so each query waits for its turn.

To improve query performance under load:

* **Add cores, and scale the heap with them** at 1 GB to 2 GB per core. For compressed text, add those cores as more nodes rather than as a wider one.
* **Check your file layout.** Too few files, or compressed text files too large to divide, leave cores idle whatever the size of the node.
* **Specify a mapping** on text datasets, which takes schema inference out of planning.
* **Raise [`esql.external.max_concurrent_requests`](#read-concurrency)**, with the corresponding heap increase.

## Related pages [related]

- [Query external datasets](esql-data-federation-querying.md): how the engine reduces storage reads, and how to query datasets alongside indices
- [Schema inference](esql-data-federation-schema.md): how {{es}} discovers schemas, and how to specify a mapping instead
- [Dataset settings](esql-data-federation-dataset-settings.md): file discovery, parsing, and reader behavior, including [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size)
- [Cluster settings](esql-data-federation-cluster-settings.md): request concurrency, file-discovery limits, and caching
- [Optimize {{esql}} query performance](esql-query-performance.md): general {{esql}} tuning, which applies to datasets too
- [Costs](esql-data-federation-costs.md): what your storage provider bills for the reads a query generates
