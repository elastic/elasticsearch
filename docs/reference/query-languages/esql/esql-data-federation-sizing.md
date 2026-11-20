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

* **Schema inference.** CSV, TSV and NDJSON files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens on the coordinating node at planning time, before any data is read, unless the dataset declares an explicit mapping with `dynamic` set to `false`. Its cost follows the number of columns rather than the size of the dataset, and because it falls on the coordinating node alone, adding data nodes does not divide it.
* **Dispatch across nodes.** The files a query reads are divided into units of work, and those units are distributed across the data nodes. How many a node reads at once follows its processor count, bounded by how many units its share of the files divides into.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from the object store, and further memory to decompress it and to parse it into the columnar form the query engine works on. A compressed unit expands in memory before it can be read, so data that compresses well costs *more* heap per reader rather than less.

How much memory a query needs, and how fast it runs, both depend on the file format.

Parquet carries its schema in the file footers, so it is read directly and no sampling is needed. It is a columnar format and is taken chunk by chunk: a reader works on one row group at a time, and only the columns a query asks for are fetched and decoded. Both the bytes read and the work done therefore follow your projection rather than the width of the file.

The text formats carry no schema of their own, and a file is read as a continuous stream: a reader works through it from the beginning, and a row must be parsed in full before any of its columns can be used. Asking for fewer columns does not read less, because the parse still has to walk the whole record to find its field boundaries. What a narrow projection saves is the building of the result columns, not the reading or the parsing.

That difference shows up again in the unit of work a file is divided into for parallel reading. For Parquet, that unit is a row group, so one large file still parallelizes. For uncompressed text it is a split, so a larger file is read as several units; the size is set by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size). For compressed text it is the whole file, which cannot be read from the middle and so cannot be divided at all: one file is one reader, decompressing on one thread.

One consequence is worth stating plainly, because it is the opposite of sizing for indexed data. **The safety of a configuration does not depend on the size of your dataset.** It depends on how much is held in memory at the same time: the units being read, the queries running together, and the schema being inferred. A reader holds the unit it is working on, not the file that unit came from, and units waiting their turn cost nothing, so a dataset of ten thousand objects needs much the same heap as one of a hundred. What the size of the dataset affects is mostly query execution time.

## General guidelines [general-guidelines]

* **Prefer Parquet over the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays divisible when compressed.
* **Size your files** to tens of megabytes up to about 1 GB, and write a few hundred of them per dataset. It matters most for compressed text, whose unit of parallel work is the whole file.
* **Prefer compressed files**, but note that a compressed *text* file cannot be split across readers, while an uncompressed one can. Compressed Parquet stays splittable and has no such trade-off.
* **Declare a schema** on text datasets with the `mappings` block. It skips schema inference entirely, which is the single largest heap cost for CSV and TSV.
* **Start with 4 GB to 8 GB of heap per node for text datasets, and 2 GB to 4 GB for Parquet**, then refine with the [formula below](#heap-per-node).
* **Allow 75 MB to 200 MB of heap per processor** on text datasets. The low end is CSV and TSV, which expand least when decompressed; the high end is zstd-compressed NDJSON, which expands most.
* **On text datasets, allow about one processor for every three concurrent queries**, and the heap that processor brings with it.
* **Keep heap at or below 31 GB** and add nodes rather than growing one past it.

## Size heap for concurrency [size-heap]

Heap determines whether your queries run at all: on a node with too little heap, they are rejected by a circuit breaker rather than running slowly.

Heap on a node divides up like this:

    heap per node  =  Elasticsearch itself
                   +  schema inference     (text formats, unless you declare a schema)
                   +  the units being read (x readers in flight)
                   +  per-query state      (x queries running together)

    processors     =  at least  concurrent queries / 3   (text formats)

Two of those four terms apply only to the text formats, which is why Parquet is cheaper and why the recommendations below differ by format.

### Memory decides how many queries run together [memory-and-concurrency]

The first three terms are fixed once you have chosen a format and a node size: they are what a node needs to run a query of that kind at all. Only the last grows with concurrency, and it grows far more slowly than the query count, because queries share one pool of readers rather than each being given their own.

That is why concurrency is sized in processors rather than in gigabytes. Roughly one processor serves three concurrent queries on a text dataset, and the heap that processor needs is the per-processor allowance. Going from three concurrent queries to thirty costs about nine more processors and their allowance, not ten times the whole figure.

### Start at 4 GB to 8 GB for text, then allow per processor [heap-per-node]

A text dataset starts at 4 GB to 8 GB of heap per node. Refine that starting point against the processors the node has and the queries you expect to run at once:

```
processors     =  at least  concurrent queries / 3

heap per node  =  2 GB  +  ( allowance x processors )
```

Concurrency is served by processors rather than by a separate memory allowance: three concurrent queries need roughly one processor between them, and the heap that processor needs is already the per-processor allowance below.

| Dataset | Allowance per processor |
|---|---|
| NDJSON, zstd | 200 MB |
| NDJSON, gzip | 165 MB |
| CSV, TSV, zstd | 85 MB |
| CSV, TSV, gzip | 75 MB |
| Uncompressed text | 15 MB |

The allowance follows your compression ratio, because what a reader holds is the decompressed bytes rather than the stored ones. That is what separates the two ends of the range: CSV and TSV sit at the low end because they expand least when decompressed, and zstd-compressed NDJSON sits at the high end because it expands most. Better compression raises the figure in exact proportion, so NDJSON costs more per processor than CSV for the same rows precisely because it compresses better. Find your own ratio by running `gzip -l` or `zstd -l` on one file from your bucket, and scale the row accordingly. The ranges are wide, so measuring is worth more here than any table.

These are floors rather than targets. A node at these figures will run your queries; a node below them may refuse them. Round up to the next heap size, and if you have heap to spare, give it.

Below the recommended figures the behaviour is not a clean threshold. The same query on the same node can succeed once and be refused the next time. The figures here are set where that stops, which is also why they are not cut fine.

#### Heap has a practical ceiling at 31 GB [scale-out]

Past about 31 GB the JVM stops compressing object pointers, so each object takes more space and a larger heap holds less. {{es}} does not recommend going beyond it.

Where the formula asks for more, add nodes rather than growing one. Each node has its own heap and reads its own share of the files. Adding nodes does not reduce the schema inference term, which falls on the coordinating node alone.

### Parquet [heap-parquet]

Parquet pays neither of the two text-only terms. Its schema is in the file, so there is no inference, and it needs no conversion buffers.

Parquet is not in the table above because it does not size the same way. Its reads are bounded across the node rather than growing with each processor you add, so a per-processor allowance over-states it, and the one-processor-per-three-queries rule does not apply in the same way either.

Start a Parquet dataset at 2 GB to 4 GB of heap per node. It needs less than the text formats, not more.

### Text formats [heap-text]

The text formats pay all four terms. You can remove the largest of them.

#### Declare a schema on text datasets [declare-schema]

Schema inference is the single largest heap cost for CSV and TSV. A CSV or TSV dataset whose schema is inferred needs about 31 GB per node regardless of the formula above, because inference dominates everything else.

Declare a schema instead with the [`mappings` block](esql-data-federation-schema.md#declare-a-schema-explicitly), setting `dynamic` to `false` so the declaration is treated as the complete schema, and inference is skipped. A declared mapping without `dynamic: false` is still fully inferred.

Declaring a schema does not remove every planning-time read. A query that needs statistics still reads metadata from each file.

### Lower `esql.external.max_concurrent_requests` before adding heap [lower-concurrency]

This setting controls how many storage requests the node keeps in flight. Lowering it is the reliable remedy when text queries are being refused, at the cost of some wall-clock time. Adding heap is the less reliable one, because refusal below the [heap figures](#heap-per-node) is not a clean threshold.

The default of [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is derived from the node's processors. It is further limited so that the read buffers stay within a quarter of heap, or half of `indices.breaker.request.limit` when that is tighter. The setting is node-scoped, so a change takes effect after a restart.

The same remedy is less effective for Parquet, whose reads are governed by a byte budget rather than by this permit count.

## Size for parallelism [size-parallelism]

How fast a query runs is a separate question from whether it runs, and depends chiefly on how many units the node reads at once. It has two sides: the readers the node opens, and the pieces of files your dataset gives those readers to work on.

### Processors buy speed while there are units waiting [processor-count]

Query time scales roughly linearly with processors, so a query can be expected to run about twice as fast on an instance twice the size. When the instance grows, grow the heap with it, in the proportion the [formula above](#heap-per-node) gives.

This holds while your files divide into more units than the node is already reading. Once every unit is in flight, more processors have nothing to pick up. For compressed text the limit binds sooner than you might expect, because one file is one unit on one thread: more processors never make a single compressed object read faster, they only allow more objects to be read at the same time.

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

Three public datasets, worked through the rules above on a 16-processor node, which serves about 48 concurrent text queries.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as one Parquet file per month, of roughly 50 MB to 110 MB. Five years is about 60 files; the full history is about 180.

**Allow 2 GB to 4 GB of heap per node.** The figure does not change between five years of data and fifteen, because a reader holds the row group it is working on rather than the dataset.

Sixty files is below the few hundred this guide suggests, and for Parquet that is fine: a Parquet file divides on row groups, so the file count is a preference rather than a limit.

### GH Archive, gzipped NDJSON [example-gharchive]

[GH Archive](https://www.gharchive.org/) publishes one gzipped NDJSON file per hour. Taking files of about 25 MB that compress about 6:1, a year is roughly 8,800 files.

The per-processor allowance for NDJSON at that ratio is about 75 MB, so the formula gives `2 + (0.075 x 16)` = 3.2 GB. That is below the starting point for text, so **allow 4 GB of heap per node**.

The file layout suits this workload well: files of tens of megabytes, and enough of them to keep every reader busy. Note that three years of hourly files is about 26,000 objects, which is past the [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) default of 25,000. Narrow the resource pattern, or raise the setting.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as one CSV file per day, with a wide schema of SMART attributes.

With the schema declared, the allowance for CSV at about 5:1 is roughly 65 MB per processor, giving `2 + (0.065 x 16)` = 3 GB, so **allow 4 GB of heap per node**.

With the schema inferred, the same dataset needs about **31 GB**. That difference is schema inference on a wide table, and it is the single largest effect on this page. It is why declaring a schema is the first recommendation for CSV and TSV.

### Reading these examples [examples-reading]

In all three, the starting point governs rather than the per-processor allowance: a 16-processor node lands at the floor. The allowance starts to bind above roughly 12 to 27 processors, depending on how well the data compresses, and that is where the table in [Allow per processor](#heap-per-node) earns its place.

These figures are approximate, and the dataset sizes above are illustrative rather than current. A number of conditions in your own environment can move them, in particular the width of your tables, the shape of your queries, and your compression ratio. Measure your own if the number decides a purchase.

## What these figures rest on [provenance]

Measured on a single node against one corpus of compressed files, with a deliberately expensive query: a full scan with an aggregate over every column, at four concurrent queries.

The per-processor allowances are derived from how much a single reader holds at a measured compression ratio, multiplied by the number of readers a processor supports. They are not measured across processor counts directly. Against the one configuration where an independent floor was measured, the formula over-provisions by about a quarter.

Not yet measured: uncompressed files, clusters of more than one node, concurrency other than four queries, and the heap a text dataset needs once its schema is declared.
