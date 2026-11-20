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

* **Prefer Parquet over the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays splittable when compressed.
* **Specify a mapping** on text datasets, with `dynamic` set to `false`.
* **Start at 2 GB of heap** for Parquet, **2 GB to 4 GB** for uncompressed text, and **4 GB** for compressed text. A text dataset with no mapping specified starts at **8 GB** and rises with the width of its schema.
* **Allocate 1 GB to 2 GB of heap per core**, and take whichever of that and the starting point is greater.
* **On text datasets, add about one core, with its heap, for every three or four concurrent queries.**
* **Prefer more nodes to wider ones**, and keep heap at or below 31 GB. Data federation parallelizes well across nodes.
* **Size your files** to tens of megabytes up to about 1 GB, and write a few hundred of them per dataset.

## Background [background]

Data federation and index workloads share the same query engine, but their read paths differ. Aggregation, filtering, sorting and the rest of the {{esql}} pipeline are the same code, so a dataset supports the same commands and functions an index does. What differs is the work of reading data that lives outside the cluster.

A few stages exist only on this path, and each one holds memory:

* **Schema inference.** Text files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset [specifies a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Split distribution.** The files a query reads are divided into units of work called splits, sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and those splits are distributed across the data nodes. How many a node reads at once follows its core count.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed split expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

### Parquet reads only what it needs [parquet-reads]

Parquet carries its schema in the file footers, so no sampling is needed. It is a columnar format and is read chunk by chunk: a reader takes one row group at a time, and within it fetches and decodes only the columns the query names. Both the bytes read and the work done therefore follow your projection rather than the width of the file.

This is why narrowing a query with [`KEEP`](/reference/query-languages/esql/commands/keep.md) is the strongest lever available on a wide Parquet dataset.

### Text formats read whole rows [text-reads]

The text formats behave the other way round. A file is read as a continuous stream, and a row must be parsed in full before any of its columns can be used, because the parse has to walk the whole record to find its field boundaries.

Narrowing a text query therefore does not read less. The same bytes are fetched, decompressed and parsed either way, and what a projection saves is only the building of the result columns.

### Parallelization differs too [parallelization]

The three cases divide a file differently. For Parquet the unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

### Dataset size does not drive heap [dataset-size]

It is worth noting that the heap a cluster needs does not depend on the size of your dataset. Data is streamed through the cluster rather than loaded into it, so what matters is how much is in flight at any moment: the splits being read, the queries running together, and the schema being inferred. A dataset of ten thousand objects needs much the same heap as one of a hundred.

What the size of the dataset does affect is the time a query takes, and therefore the cores you need to meet a latency target. The number of objects is a separate matter again, bounded by [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) rather than by memory.

## Size your files for your workload [file-layout]

File layout is a property of your data rather than of your cluster, so you can get it right without knowing what will query it.

Aim for files of tens of megabytes up to about 1 GB, and for a few hundred of them per dataset.

Too few files leave readers idle. For compressed text this is a limit rather than a preference, because one file is one reader: a dataset held in a handful of large compressed objects cannot keep a node busy however large the node is. Too many files cost more than they return, because every object is reached by its own high-latency request, so a query over very small objects spends more time asking for data than reading it. A dataset is also capped at [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files), after which a query over it fails rather than running slowly.

Compression reduces what every query fetches from storage, which is both faster and cheaper, but for text it costs parallelism. Compressed Parquet stays splittable.

For Parquet the equivalent lever is the row group rather than the file, because one large Parquet file still divides. Leave row group sizes at their defaults unless your files are very wide.

## Sizing your cluster [sizing]

The general model is that you allocate memory for {{esql}} itself and the rest for {{es}}, and enough memory on the coordinating node to execute planning and schema inference. Further memory is needed to hold the concurrent streams a query reads.

Parquet currently trades memory for performance well: its read path draws on a fixed node-wide budget shared by every query, so pressure becomes waiting rather than failure. Text has no such budget and must be sized explicitly.

Adding cores increases parallelism and increases memory allocation with it, so heap must be adjusted per core. For compressed text the per-core requirement itself rises as cores are added, so heap and cores cannot be scaled independently.

### Where to start [heap-per-node]

Start with:

* **2 GB of heap** for Parquet
* **2 GB to 4 GB of heap** for uncompressed text
* **4 GB of heap** for compressed text
* **8 GB of heap** for a text dataset with no mapping specified, which is set by schema inference rather than by reading, and rises with the width of the schema

Then allocate 1 GB to 2 GB of heap per core, and take whichever figure is greater. The starting points are floors for small nodes; past about four cores the per-core figure is the one that governs.

Do not exceed 31 GB of heap. Past that point {{es}} loses compressed object pointers, so each object takes more space and a larger heap holds less. Beyond it the only way to stay within budget is to stop adding cores rather than to add memory.

### Sizing for concurrency [concurrency]

Concurrency is sized in cores and heap together, not in memory alone. On a text dataset, allow about **one additional core for every three or four concurrent queries**, with the heap that core requires.

For uncompressed text, a wider node serves that concurrency. For compressed text it does not: the per-core requirement rises as cores are added, so add nodes instead.

Parquet behaves differently again. Its read path draws on a node-wide budget shared by every query, so concurrency there shows up as queries waiting for the budget rather than as a larger heap requirement. What does grow per query is planning on the coordinating node, which reads every file's footer.

Prefer more nodes to wider ones throughout. Past 31 GB of heap, adding nodes is the only option left.

### Specify a mapping on text datasets [declare-schema]

[Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) on text datasets, and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`, which terminates the inference path.

Inference is the largest single cost you can remove outright, and on a wide schema it is the first thing to fail. Setting `dynamic` to `false` removes it entirely.

### Lower `esql.external.max_concurrent_requests` before adding heap [lower-concurrency]

This setting controls how many storage requests a node keeps in flight. Lowering it is the reliable remedy when text queries are being refused, at the cost of some wall-clock time.

The default of [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is derived from the node's cores. The setting is node-scoped, so a change takes effect after a restart.

## Sizing for performance [performance]

Data federation queries scale with the number of cores, and their scan time is proportional to the size of the dataset being read. This is where dataset size enters: it sets the work, not the heap.

If you want your queries to run faster, add cores and the heap that goes with them. For compressed text, add those cores as more nodes rather than as a wider one. Past 31 GB of heap, adding nodes is the only option in any case.

Two things cut this short. Your files must divide into more splits than the node is already reading, or the extra cores have nothing to pick up. And for compressed text, one file is one split on one thread, so more cores never make a single compressed object read faster — they only allow more objects to be read at the same time.

## Troubleshoot [troubleshoot]

### `circuit_breaking_exception` with `[parent] Data too large` [parent-breaker]

The node is holding more than it can collect. Lower [`esql.external.max_concurrent_requests`](#lower-concurrency) first. Adding heap may not help on its own.

Not all of the memory this workload holds is attributed to the query that allocated it, so the error may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### `circuit_breaking_exception` with `[request] Data too large` [request-breaker]

On a text format, this is usually schema inference at planning time. [Specify a mapping](#declare-schema) with `dynamic` set to `false`. If you already have, this is the case that does need more heap.

### Queries slow under load but still succeeding [contended]

The file readers are contended rather than exhausted. Lower [`esql.external.max_concurrent_requests`](#lower-concurrency), or add nodes.

## Example configurations [examples]

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as one Parquet file per month, of roughly 50 MB to 110 MB. Five years is about 60 files.

Start at **2 GB of heap**, and at four cores the per-core figure governs instead, so 4 GB to 8 GB.

On the data nodes, Parquet's reads draw on a shared node-wide budget, so pressure under concurrency shows up as queries waiting for that budget rather than as a larger heap requirement. Sixty files is below the few hundred this guide suggests, and for Parquet that is fine, because a Parquet file divides on row groups.

### GH Archive, gzipped NDJSON [example-gharchive]

[GH Archive](https://www.gharchive.org/) publishes one gzipped NDJSON file per hour, so a year is roughly 8,800 files of a few tens of megabytes. That file layout suits this workload well.

This is compressed text, the most demanding path here. Start at **4 GB of heap**. A wider node will not serve more concurrent queries, so scale this one out: for twelve concurrent queries, prefer three or four smaller nodes to one large one.

Note that three years of hourly files is about 26,000 objects, which is past the [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) default. Narrow the resource pattern, or raise the setting.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as one CSV file per day, with a wide schema of SMART attributes.

With no mapping specified, start at **8 GB of heap**, set by schema inference rather than by reading any data. A schema this wide needs more than that starting point, because what inference costs follows the number of columns.

Specify a mapping with `dynamic` set to `false` and the starting point drops to **4 GB**, the figure for compressed text. That one setting is worth more than any hardware change on this page.

### Reading these examples [examples-reading]

Across all three, the number of bytes in the dataset never enters the heap figure. What moves it is the format, whether the files are compressed, the width of the schema, and how many queries run at once. Dataset size sets how long the queries take, and the object count is bounded separately.

These figures are approximate, and the dataset descriptions above are illustrative rather than current. Measure your own if the number decides a purchase.

## What these figures rest on [provenance]

These figures come from a number of experiments and from the architecture of the read paths themselves.

They are given for cold reads: every query is assumed to fetch its data from object storage rather than from a cache.
