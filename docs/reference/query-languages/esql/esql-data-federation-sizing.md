---
navigation_title: "Sizing and performance"
description: "Size nodes and lay out files for ES|QL Data Federation, by file format, compression, and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Size nodes for {{esql}} Data Federation

A dataset is queried where it lives, in object storage, rather than from a local index. Nothing is stored in {{es}}, so the resources a node needs don't follow from the size of your data. They follow from how much of it the node holds in memory at one time.

Sizing for this workload has two parts, and this page covers both: how much heap a node needs to run your queries at the concurrency you expect, and how much read parallelism the node and your files can sustain. For data held in {{es}} indices, refer to [size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) instead.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## General guidelines [federation-sizing-guidelines]

Use the following as starting points, then refer to [sizing considerations](#federation-sizing-considerations) for the reasoning behind them:

* Prefer Parquet to the text formats where you have the choice, especially on wide data
* Declare a schema on CSV, TSV, and NDJSON datasets
* Aim for compressed files in the tens to hundreds of megabytes, and write enough of them to keep your nodes busy
* Allow `[[N]]` GB of heap per node, plus `[[N]]` GB for each query you expect to run concurrently
* Keep heap at or below 31 GB and add nodes beyond that

There is no one-size-fits-all configuration. A setup that works for one dataset may not for another, because file format, compression ratio, and column count each change the result. The best way to size this workload is to run your own queries, at your expected concurrency, against your own files.

## Sizing considerations [federation-sizing-considerations]

Keep the following things in mind when sizing nodes for federated queries.

### Datasets use the {{esql}} engine, with extra stages for reading [extra-read-stages]

Datasets run on the same query engine as indices. Aggregation, filtering, sorting, and the rest of the {{esql}} pipeline are the same code, which is why a dataset supports the same commands and functions an index does.

What differs is the work of reading data from outside the cluster. Three stages exist only on this path, and each one holds memory:

* **Schema inference.** CSV, TSV, and NDJSON files don't describe their own columns, so {{es}} [infers a schema](esql-data-federation-schema.md) by sampling rows. This runs on the coordinating node at planning time, before any data is read, and its cost grows with the number of columns rather than with the size of the dataset. Adding data nodes doesn't divide it.
* **The units being read.** {{es}} divides your files into units of work and reads several at a time. A reader holds the bytes of the unit it's working on, expanded if they were compressed, until it has finished with them. What a unit is depends on the format and the compression, and that difference is the largest single influence on how much heap a node needs.
* **Decompression and parsing buffers.** A compressed unit expands in memory before it can be read, so a file that compresses well costs *more* heap per reader, not less. Rows that have been parsed queue as pages ahead of the rest of the query.

How many units a node reads at once follows its processor count, bounded by how many units its share of your files divides into. [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) bounds the storage requests those readers have in flight, per storage scheme and per node, which is a separate and looser limit.

### Parquet follows your projection, and the text formats don't [format-read-differences]

Parquet carries its schema in the file, so no inference is needed. It fetches and decodes only the columns a query names, so both the bytes read and the work done scale with your projection rather than with how wide the file is. A reader decodes one row group at a time and holds the groups it has already fetched ahead of that one, so budget for more than a single row group per reader.

The text formats are row-oriented, and a narrow projection buys almost nothing on the read. Every byte of a unit is fetched and decompressed, and every record is parsed in full to find its field boundaries. Selecting fewer columns with [`KEEP`](/reference/query-languages/esql/commands/keep.md) saves only the building of the result columns, not the reading or the parsing.

That is the case for Parquet on wide data in one sentence.

### Compressed text files can't be split [compression-and-splitting]

Compression reduces what every query fetches from storage, which is both faster and cheaper. For text, it costs you parallelism.

An uncompressed text file is cut at record boundaries into splits sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and each split is read as several ranges in parallel.

A gzip or zstd file can't be read from the middle, so the whole file is one unit, read by one reader and decompressed on one thread. No setting divides it. A single large compressed text file is a single-threaded decompression however many processors the node has, so for compressed text, parallelism comes from the number of files rather than from the size of the node.

Parquet is unaffected: it compresses each column chunk separately and is divided on row groups.

### Dataset size sets query time, not the heap you need [dataset-size-vs-heap]

A reader holds the unit it's working on, not the file that unit came from, and units waiting their turn cost nothing. A dataset of many objects needs roughly the same heap as one of few.

What sets the heap a node needs is how much is held at one time: the units being read, the queries running together, and the schema being inferred. Dataset size decides how long a query takes, not whether it succeeds.

One exception is worth knowing for Parquet. A Parquet file is divided on row groups rather than at a fixed stride, so the list of units the coordinating node plans grows with the total size of the dataset. Very large Parquet datasets therefore cost the coordinating node more at planning time than text datasets of the same size do.

## Best practices [federation-size-best-practices]

Where applicable, use the following best practices as starting points.

### Declare a schema on CSV, TSV, and NDJSON datasets [declare-a-schema]

Schema inference is the largest single memory cost on the coordinating node for the text formats, and it's the one you can remove outright. Set `dynamic` to `false` in a [declared mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) to treat the declaration as the complete schema and skip inference.

Because inference runs on the coordinating node, adding data nodes doesn't reduce it. Declaring a schema is the only remedy.

### Aim for compressed files in the tens to hundreds of megabytes [file-size-recommendation]

File layout is a property of your data rather than of your cluster, so you can get it right without knowing what will query it.

Too few files leave readers idle, and for compressed text this is a hard ceiling rather than a preference, because one file is one reader. Too many files cost more than they return, because each object is fetched over its own high-latency requests, so a query over very small objects spends more time asking for data than reading it. A dataset is also capped at [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files).

For Parquet, the equivalent lever is the row group rather than the file, because one large Parquet file still parallelizes. Leave row group sizes at their defaults unless your files are very wide.

### Allow enough heap for the units being read and for concurrent queries [heap-recommendation]

The mechanisms above interact, but a single rule of thumb is enough to provision against:

```
heap  >=  [[base]]  +  [[per reader]] x readers  +  [[per query]] x concurrent queries
```

`readers` is how many units the node reads at once: a few per processor by default, capped by how many units your files divide into.

For compressed text, what a reader holds is decompressed chunks and the pages parsed from them, so the per-reader term scales both with your compression ratio and with the node's processor count. Measure the ratio on one of your own files with `gzip -l` or `zstd -l`. It varies widely by data, and better compression raises the heap a reader needs in proportion: storing your data in half the space roughly doubles it.

For example, `[[a worked configuration: dataset, format, processors, concurrency, and the heap the rule gives]]`.

This rule is calibrated for the text formats. Parquet isn't driven by the same term, because a reader holds row groups it has fetched ahead rather than chunks of a stream, so its floor follows from how your files were written. `[[Parquet coefficient and its denominator, pending measurement.]]`

Note that this rule defines a floor rather than a target. Below it, {{es}} rejects queries with a circuit breaker rather than running them slowly, and close to it the outcome isn't reliably repeatable from one run to the next. Round up, and give the node more if you have it.

Not all of this memory is attributed to the query that allocated it, so the error reporting the failure may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

If queries are being rejected on a text dataset, lowering `esql.external.max_concurrent_requests` is a more reliable remedy than adding heap, at the cost of some wall-clock time. The setting is static, so each node needs a restart.

To check the configured size of each node's heap, use the [cat nodes API]({{es-apis}}operation/operation-cat-nodes).

```console
GET _cat/nodes?v=true&h=name,heap.max
```

### Add nodes rather than heap above 31 GB [scale-out-recommendation]

Heap has a ceiling independent of this workload. Past about 31 GB the JVM stops compressing object pointers, so each object takes more space and a larger heap holds less. Keep heap at or below 31 GB.

When the rule above asks for more than that, add nodes instead of growing one. Each node has its own heap and reads its own share of the files.

### Add processors while there are units waiting to be read [processor-recommendation]

Query time is set chiefly by how many units a node reads in parallel, and that count follows the processor count. As a rule of thumb, to make a query roughly twice as fast, double the size of the instance and raise the heap in the same proportion as the rule above.

This holds only while your files divide into more units than the node is already reading. Once every unit is in flight, more processors have nothing to pick up, and more throughput comes from more nodes or from more files rather than from larger instances.

For compressed text, the second limit binds sooner than you might expect, because one file is one unit on one thread. More processors never make a single gzip object read faster. They let more objects be read at the same time, which is why file count is the lever for that format.

Concurrent queries divide this capacity rather than adding to it. Queries share the node's read capacity, so running N queries at once takes each of them roughly N times as long as running one on its own.

To reduce the work itself rather than add capacity to it, refer to [how external data is read](esql-data-federation-querying.md#how-external-data-is-read) and [optimize {{esql}} query performance](esql-query-performance.md).
