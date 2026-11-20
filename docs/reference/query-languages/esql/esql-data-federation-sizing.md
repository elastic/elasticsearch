---
navigation_title: "Sizing and performance"
description: "Size your Elasticsearch cluster for ES|QL Data Federation workloads, by file format, compression, and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Size your {{es}} cluster for {{esql}} Data Federation

With [data federation](esql-data-federation.md), you can query tabular data held in object storage — CSV, TSV, NDJSON and Parquet files — without ingesting it into {{es}} first.

This guide covers how much heap and how many cores a node needs to run your queries at the concurrency you expect, and how to lay out your files so those cores stay busy.

For data held in {{es}} indices, refer to [size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) instead.

## General guidelines [general-guidelines]

* **Prefer Parquet to the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays splittable when compressed.
* **Prefer mid-sized files**, from tens of megabytes to a couple of hundred megabytes, and enough of them to keep every core busy.
* **Specify a mapping** on text datasets, with `dynamic` set to `false`.
* **Start at 2 GB of heap** for Parquet, **2 GB to 4 GB** for uncompressed text, and **4 GB** for compressed text. Add a further **2 GB to 4 GB** for a text dataset whose schema is inferred.
* **Allocate 1 GB to 2 GB of heap per core.**
* **Add about one core per three or four concurrent queries** on text, and about one per eight to ten on Parquet.
* **Do not raise heap past 31 GB.** Scale horizontally across nodes instead.

These figures are starting points, not targets. How a dataset behaves depends on characteristics of your own data — how well it compresses, how its files split, the row group sizes in your Parquet files, the width of your schema, and the size and number of your objects — so a configuration that suits one dataset may not suit another. Size against your own queries, at the concurrency you expect, and watch how the cluster behaves.

## Background [background]

Data federation and index workloads share the same query engine, but their read paths differ. Aggregation, filtering, sorting and the rest of the {{esql}} pipeline are the same code, so a dataset supports the same commands and functions an index does, with [known exceptions](esql-data-federation-querying.md#limitations).

What differs is the work of reading data that lives outside the cluster. A few stages exist only on this path, and they are what affect memory allocation and the performance of your queries:

* **Schema inference.** Text files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset [specifies a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Split distribution.** The files a query reads are divided into units of work called splits, sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and those splits are distributed across the data nodes. How many a node reads at once follows its core count.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed split expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

The formats then diverge. Parquet carries its schema in the file footers, so no sampling is needed, and it is read chunk by chunk: a reader takes one row group at a time and, within it, fetches and decodes only the columns the query names. Both the bytes read and the work done follow your projection rather than the width of the file, which is why narrowing a query with [`KEEP`](/reference/query-languages/esql/commands/keep.md) is the strongest lever on a wide Parquet dataset.

The text formats behave the other way round. A file is read as a continuous stream, and a row must be parsed in full before any of its columns can be used. Narrowing a text query therefore does not read less: the same bytes are fetched, decompressed and parsed either way, and a projection saves only the building of the result columns.

Parallelization works differently depending on the format, because each one divides a file into a different unit of work. For Parquet that unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

It is important to note that the heap a cluster needs does not depend on the size of your dataset. Data is streamed through the cluster rather than loaded into it, so what matters is how much is in flight at any moment. A dataset of ten thousand objects needs much the same heap as one of a hundred. What the size of the dataset affects is the time a query takes, and therefore the cores you need to meet a latency target.

## Size your files for your workload [file-layout]

File layout is a property of your data rather than of your cluster, so you can get it right without knowing what will query it.

Aim for mid-sized files, from tens of megabytes to a couple of hundred megabytes, and for enough of them to keep every core busy. Too few leave cores idle, and for compressed text that is a limit rather than a preference, because one file is one reader. Too many cost more than they return, because every object is reached by its own high-latency request. The object count is also capped by [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files), past which a query fails rather than running slowly.

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
* **A further 2 GB to 4 GB** for a text dataset whose schema is inferred rather than specified

Then allocate 1 GB to 2 GB of heap per core.

### Sizing for concurrency [concurrency]

Query execution time depends on how many queries run at once, and scales with the cores available to them. The total time a query takes follows that concurrency and the size of the dataset it reads.

On a text dataset, add about **one core per three or four concurrent queries**, with the heap that core requires. For uncompressed text a wider node serves that concurrency. For compressed text it does not, because the per-core requirement rises as cores are added, so add nodes instead.

Parquet needs fewer cores for the same concurrency, about **one per eight to ten concurrent queries**, because it reads only the columns a query names. Its reads on the data nodes draw on a shared node-wide budget, so added concurrency tends to show up as queries waiting for that budget rather than as more heap.

Prefer more nodes to wider ones throughout, and scale horizontally rather than raising heap past 31 GB.

### Specify a mapping on text datasets [declare-schema]

[Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) on text datasets, and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`, which terminates the inference path.

Inference is the largest single cost you can remove outright, and on a wide schema it is the first thing to fail.

### Lower `esql.external.max_concurrent_requests` before adding heap [lower-concurrency]

This setting controls how many storage requests a node keeps in flight. Lowering it is the reliable remedy when text queries are being refused, at the cost of some wall-clock time.

The default of [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is derived from the node's cores. The setting is node-scoped, so a change takes effect after a restart.

## Sizing for performance [performance]

Data federation queries scale with the number of cores, and their scan time is proportional to the size of the dataset being read. This is where dataset size enters: it sets the work, not the heap.

If you want your queries to run faster, add cores and the heap that goes with them. For compressed text, add those cores as more nodes rather than as a wider one.

## Example configurations [examples]

Each of these is sized for **twelve concurrent queries**.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as one Parquet file per month, of roughly 50 MB to 110 MB. Five years is about 60 files.

| | |
|---|---|
| Starting heap | 2 GB |
| Cores for twelve concurrent queries | 2, so use a 4-core node |
| Heap at 4 cores | 4 GB to 8 GB |
| Each further concurrent query | about 0.1 core, and little extra heap |

Twelve queries at one core per eight to ten needs only two cores, so the node's minimum size governs rather than the concurrency. Sixty files is fewer than this guide suggests, and for Parquet that is fine, because a Parquet file divides on row groups: the file count is a preference rather than a limit.

### GH Archive, gzipped NDJSON [example-gharchive]

[GH Archive](https://www.gharchive.org/) publishes one gzipped NDJSON file per hour, so a year is roughly 8,800 files of a few tens of megabytes. That file layout suits this workload well.

| | |
|---|---|
| Starting heap | 4 GB, compressed text |
| Cores for twelve concurrent queries | 3 to 4 |
| Heap at 4 cores | 4 GB to 8 GB |
| Each further concurrent query | about 0.3 core, and 1 GB to 2 GB of heap with it |

Because this is compressed text, spread those four cores over two or three nodes rather than buying one wider node: a wider node would serve fewer concurrent queries, not more.

Three years of hourly files is about 26,000 objects, past the [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) default. Narrow the resource pattern, or raise the setting.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as one CSV file per day, with a wide schema of SMART attributes.

| | With a mapping | Schema inferred |
|---|---|---|
| Starting heap | 4 GB | 6 GB to 8 GB |
| Cores for twelve concurrent queries | 3 to 4 | 3 to 4 |
| Heap at 4 cores | 4 GB to 8 GB | 6 GB to 12 GB |
| Each further concurrent query | about 0.3 core | about 0.3 core |

Inference is what separates the two columns, and on a schema this wide it is the first thing to fail. Specifying a mapping with `dynamic` set to `false` is worth more here than any hardware change on this page.

## What these figures rest on [provenance]

These figures come from a number of experiments and from the architecture of the read paths themselves.

They are given for cold reads: every query is assumed to fetch its data from object storage rather than from a cache.

## Troubleshoot [troubleshoot]

Because the figures on this page are starting points rather than targets, the configuration you end up with should be the one your own data asks for. Watch for the following as you load it up:

* **Circuit breaker rejections**, which mean the node is holding more than it can collect. These are the loud failure and are covered below.
* **Queries slowing as concurrency rises** without any rejection, which means readers are contended rather than exhausted.
* **Query times that scale worse than the data they read**, which usually points at too few files, or at compressed text files too large to divide.
* **Planning time growing with your file count**, which points at schema inference or, on Parquet, at footer reads on the coordinating node.

### `circuit_breaking_exception` with `Data too large` [breaker]

The node is holding more than it can collect. This is the same condition whether the error names the `[parent]` or the `[request]` breaker, and the remedy is the same: give the instance more memory, and raise the heap with it.

Two things are worth checking before you do. On a text dataset, the usual cause is schema inference at planning time, so [specify a mapping](#declare-schema) with `dynamic` set to `false` first — that removes the cost rather than paying for it. And lowering [`esql.external.max_concurrent_requests`](#lower-concurrency) will clear the condition at the cost of some wall-clock time, which is useful if more memory is not available to you.

Not all of the memory this workload holds is attributed to the query that allocated it, so the error may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### Queries are slow under load [slow-under-load]

Queries that still succeed but take longer as load rises are contended rather than exhausted: the readers are shared, and there are not enough of them to go round.

To improve query performance under load:

* **Add cores**, with the heap that goes with them. For compressed text, add those cores as more nodes rather than as a wider one.
* **Check your file layout.** Too few files, or compressed text files too large to divide, leave cores idle whatever the size of the node.
* **Narrow the query.** On Parquet, [`KEEP`](/reference/query-languages/esql/commands/keep.md) cuts both the bytes fetched and the work done. On text it saves less, because whole rows are read either way.
* **Specify a mapping** on text datasets, which takes schema inference out of planning.
* **Lower [`esql.external.max_concurrent_requests`](#lower-concurrency)** if the slowdown comes with breaker rejections, trading some wall-clock time for stability.
