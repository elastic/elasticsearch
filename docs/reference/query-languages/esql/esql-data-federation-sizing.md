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

With [data federation](esql-data-federation.md), you can query tabular data held in object storage — CSV, TSV, NDJSON and Parquet files — without indexing it into {{es}} first.

Sizing for this workload has two dimensions, and this guide covers both: how much heap a node needs to run your queries at the concurrency you expect, and how much read parallelism the node and your files can sustain. The file format you choose affects both of them more than anything else here.

For data held in {{es}} indices, refer to [size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) instead.

## Background [background]

Data federation runs as part of the {{esql}} query engine. The infrastructure the engine already has for aggregations, filtering, sorting and the rest is reused, which is how every {{esql}} feature available on an index is available on a dataset.

What differs is the work needed to read data that lives outside the cluster. A few execution paths exist only for federated data, and those are what affect query performance and memory pressure:

* **Schema inference.** CSV, TSV and NDJSON files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset [declares an explicit mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Split distribution.** The files a query reads are divided into units of work called splits, sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and those splits are distributed across the data nodes. How many a node reads at once follows its processor count, bounded by how many splits its share of the files divides into.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed unit expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

How much memory a query needs, and how fast it runs, both depend on the file format.

Parquet carries its schema in the file footers, so it is read directly and no sampling is needed. It is a columnar format and is taken chunk by chunk: a reader works on one row group at a time, and only the columns a query asks for are fetched and decoded. Both the bytes read and the work done therefore follow your projection rather than the width of the file.

The text formats carry no schema of their own, and a file is read as a continuous stream: a reader works through it from the beginning, and a row must be parsed in full before any of its columns can be used. Asking for fewer columns does not read less, because the parse still has to walk the whole record to find its field boundaries. What a narrow projection saves is the building of the result columns, not the reading or the parsing.

Another difference is parallelization, because the three cases divide a file differently. For Parquet, the unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

It is worth noting that the sizing of a cluster does not depend on the size of your dataset, either in memory or in CPU. Data is streamed through the cluster rather than loaded into it, so from a safety point of view what matters is how much is in flight at any moment: the splits being read, the queries running together, and the schema being inferred. A reader holds the split it is working on, not the file that split came from, and splits waiting their turn cost nothing, so a dataset of ten thousand objects needs much the same heap as one of a hundred.

What the size of the dataset does affect is the time a query takes.

## General guidelines [general-guidelines]

* **Prefer Parquet over the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays divisible when compressed.
* **Size your files** to tens of megabytes up to about 1 GB, and write a few hundred of them per dataset. It matters most for compressed text, whose unit of parallel work is the whole file.
* **Prefer compressed files**, but note that a compressed *text* file cannot be split across readers, while an uncompressed one can. Compressed Parquet stays splittable and has no such trade-off.
* **Declare a schema** on text datasets with the `mappings` block. It skips schema inference entirely, which is the single largest heap cost for CSV and TSV.
* **Start at 2 GB of heap** for Parquet and uncompressed CSV and TSV, **4 GB** for compressed text, and **8 GB** for CSV or TSV whose schema is not declared.
* **Allocate at least 2 GB of heap per core**, which is roughly 4 GB of RAM per core.
* **Add memory for each concurrent query**, by format — see [Concurrent queries](#concurrency).
* **Keep heap at or below 31 GB** and add nodes rather than growing one past it. Data federation parallelizes well across nodes.

## Sizing your cluster [sizing]

The general model is that you allocate a certain amount of memory for {{esql}} itself and the rest for {{es}}, and enough memory on the coordinating node to execute planning and schema inference. Further memory is needed to hold the concurrent streams a query reads.

Parquet currently trades memory for performance well: its read path draws on a fixed node-wide budget shared by every query, so pressure becomes waiting rather than failure. Text has no such budget and must be sized explicitly.

Adding cores increases parallelism and increases memory allocation with it, so heap must be adjusted per core. For compressed text the per-core requirement itself rises as cores are added, so heap and cores cannot be scaled independently.

### Where to start [heap-per-node]

Start with:

* **2 GB of heap** for Parquet and for uncompressed CSV and TSV
* **4 GB of heap** for compressed text, and for uncompressed NDJSON
* **8 GB of heap** for CSV or TSV whose schema is not declared, which is set by schema inference rather than by reading

Then allocate at least 2 GB of heap per core, which is roughly 4 GB of RAM per core: the standard 16 vCPU, 64 GB node.

Do not exceed 31 GB of heap. Past that point {{es}} loses compressed object pointers, so each object takes more space and a larger heap holds less. Beyond it the only way to stay within budget is to stop adding cores rather than to add memory.

### Concurrent queries [concurrency]

For concurrent queries, add memory per query, and prefer more nodes over wider ones. Budget roughly:

* **0.5 GB per concurrent query** for uncompressed CSV and TSV
* **1 GB per concurrent query** for uncompressed NDJSON
* **2 GB to 3.5 GB per concurrent query** for compressed text
* **close to nothing for Parquet on data nodes**, because its budget is shared — but allow for planning on the coordinating node, which does grow per query

Adding a core per three or four concurrent queries works for uncompressed text. It does not work for compressed text, where a wider node serves fewer concurrent queries rather than more. For compressed text, add nodes.

### Declare a schema on CSV and TSV datasets [declare-schema]

Declare a schema on CSV and TSV datasets, and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`. Both are required: [declaring a schema](esql-data-federation-schema.md#declare-a-schema-explicitly) alone has no effect, and the mapping is still fully inferred.

Inference costs more memory than any read path in this guide, and declaring removes it entirely.

Declaring a schema does not remove every planning-time read. A query that needs statistics still reads metadata from each file.

### Lower `esql.external.max_concurrent_requests` before adding heap [lower-concurrency]

This setting controls how many storage requests the node keeps in flight. Lowering it is the reliable remedy when text queries are being refused, at the cost of some wall-clock time.

The default of [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is derived from the node's processors. It is further limited so that the read buffers stay within a quarter of heap, or half of `indices.breaker.request.limit` when that is tighter. The setting is node-scoped, so a change takes effect after a restart.

The same remedy is less effective for Parquet, whose reads are governed by a byte budget rather than by this permit count.

## Size for parallelism [size-parallelism]

How fast a query runs is a separate question from whether it runs, and depends chiefly on how many units the node reads at once. It has two sides: the readers the node opens, and the pieces of files your dataset gives those readers to work on.

### More cores read more at once, up to the heap they need [processor-count]

Query time scales roughly with how many splits a node reads at once, which follows the core count. A query can be expected to run faster on a wider node, provided the heap rises with the cores, in the proportion [Where to start](#heap-per-node) gives.

Two limits cut that short. Your files must divide into more splits than the node is already reading, or the extra cores have nothing to pick up. And for compressed text, one file is one split on one thread, so more cores never make a single compressed object read faster — they only allow more objects to be read at the same time, while raising the per-core requirement as they go.

Past 31 GB of heap the lever is gone, and the remaining move is to stop adding cores and add nodes instead.

### Concurrent queries divide the read rate [concurrency-latency]

Because queries share one pool of readers, running N queries at once takes roughly N times as long as running one:

    time for N queries  ~  time for one query  x  N

To size for a latency target, time one query on its own and multiply. For more query throughput, add nodes: adding processors to one node makes each query faster, but does not let that node serve proportionally more of them.

### The number of files limits parallelism [file-count]

A reader needs a piece of a file to work on, so the shape of your data sets a ceiling on how much of the pool can be used at all:

    readers actually working  =  min( units the node opens, units your files divide into )

How hard that ceiling is depends on whether your format can be split within a file.

### Size your files for your format [file-size]

File sizing is a trade-off between parallelism and per-object overhead, and the right answer is in the middle.

Dividing a dataset into more, smaller units gives a query more work it can do at once. But every object in an object store is reached by its own request, and those requests have high latency. Past some point the query spends more of its time asking for data than reading it, and the files are also more numerous: a dataset is capped at [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) files, after which a query over it fails rather than running slowly.

Pulling the other way, too few units leave readers idle. A dataset held in a handful of very large compressed objects cannot keep a node busy however large the node is.

Aim for files of **tens of megabytes up to about 1 GB**, and for a few hundred of them. These are properties of the dataset rather than of the cluster, so you do not need to know what will query the data in order to lay it out well.

#### Example [file-size-example]

| Dataset | Files | Size each |
|---|---|---|
| 20 GB | 300 | 68 MB |
| 200 GB | 300 | 683 MB |
| 500 GB | 500 | 1 GB |

Small datasets are governed by the file count and large ones by the file size. If the count would push your files below roughly 10 MB, write fewer and larger ones: a dataset that small does not need the parallelism.

### Parquet [parallelism-parquet]

* **Treat the file count as a preference.** Parquet can be split within a file, so a dataset below the count still parallelizes.
* **Compress your files.** Compression reduces what has to be fetched on every query, and it costs Parquet no parallelism: it compresses each column chunk separately and stays splittable.
* **Leave row group sizes at their defaults.** If your files have very many columns, larger row groups keep each read fuller.

### Text formats [parallelism-text]

* **For compressed text, treat the file count as a requirement.** A compressed text file cannot be read from the middle, so one file is one reader, and too few files leave readers idle whatever the node is.
* **For uncompressed text, treat it as a preference.** It can be split within a file, so a dataset below the count still parallelizes.

## Troubleshoot [troubleshoot]

### `circuit_breaking_exception` with `[parent] Data too large` [parent-breaker]

The node is holding more than it can collect. Lower [`esql.external.max_concurrent_requests`](#lower-concurrency) first. Adding heap may not help on its own.

Not all of the memory this workload holds is attributed to the query that allocated it, so the error may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### `circuit_breaking_exception` with `[request] Data too large` [request-breaker]

On a text format, this is usually schema inference at planning time. [Declare a schema](#declare-schema) on the dataset. If you have already declared one with `dynamic: false`, this is the case that does need more heap.

### Queries slow under load but still succeeding [contended]

The file readers are contended rather than exhausted. Lower [`esql.external.max_concurrent_requests`](#lower-concurrency), or add nodes.

## Example configurations [examples]

Three public datasets on the standard node shape above: 16 vCPU, 64 GB of RAM, 31 GB of heap.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as one Parquet file per month, of roughly 50 MB to 110 MB. Five years is about 60 files; the full history is about 180.

**Start at 2 GB of heap.** On the data nodes, Parquet draws on a shared node-wide budget, so concurrent queries cost close to nothing extra there and pressure shows up as queries waiting rather than failing. What grows per query is planning on the coordinating node, which reads every file's footer, so it follows your file count and core count rather than how much data the query reads.

Sixty files is below the few hundred this guide suggests, and for Parquet that is fine: a Parquet file divides on row groups, so the file count is a preference rather than a limit.

### GH Archive, gzipped NDJSON [example-gharchive]

[GH Archive](https://www.gharchive.org/) publishes one gzipped NDJSON file per hour, so a year is roughly 8,800 files of a few tens of megabytes.

This is compressed text, the most demanding path here. **Start at 4 GB of heap**, and budget 2 GB to 3.5 GB for each concurrent query. A 31 GB heap therefore serves something like ten concurrent queries, and a wider node would serve fewer rather than more. To serve more, add nodes.

The file layout suits this workload well. Note that three years of hourly files is about 26,000 objects, which is past the [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) default of 25,000. Narrow the resource pattern, or raise the setting.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as one CSV file per day, with a wide schema of SMART attributes.

With the schema declared, this is compressed text: **start at 4 GB of heap**, and budget 2 GB to 3.5 GB per concurrent query.

Without a declared schema, the starting point is **8 GB**, set by schema inference rather than by reading any data, and the limit is the width of the table rather than its size. On a wide schema that is the first thing to fail, and declaring the schema removes it outright. It is the single largest effect on this page.

### Reading these examples [examples-reading]

Across all three, dataset size never enters. What moves the answer is the format, whether the files are compressed, the width of the schema, and how many queries run at once.

These figures are approximate, and the dataset sizes above are illustrative rather than current. A number of conditions in your own environment can move them. Measure your own if the number decides a purchase.

## What these figures rest on [provenance]

The mechanisms described here are read from the {{es}} source. The magnitudes are a model of those mechanisms rather than a measurement of a running cluster: uncompressed text and CSV inference are the best constrained, while compressed text and Parquet carry wider margins pending measurement.

One part has been checked against a real run: the schema inference rule predicts the failures an 8 GB soak actually saw.

Not yet measured end to end: compressed text under concurrency, Parquet planning on the coordinating node, clusters of more than one node, and uncompressed files.
