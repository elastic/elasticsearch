---
navigation_title: "Sizing and performance"
description: "Size heap, processors, and nodes for ES|QL Data Federation workloads, by file format and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Size your {{es}} cluster for {{esql}} Data Federation

A dataset is queried where it lives, in object storage, rather than from a local index. Nothing is stored in {{es}}, so the resources a node needs don't follow from how much data you have. They follow from how much of it the node holds in memory at one time, and from how fast you need your queries to come back.

For data held in {{es}} indices, refer to [size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) instead.

## General guidelines [federation-sizing-guidelines]

Balancing heap, processors, and the layout of your files is what makes this workload stable and fast:

* Too little heap and queries are rejected by a circuit breaker rather than running slowly.
* Too few processors, or too few files, and queries run but take much longer than they need to.

To avoid either, implement the following guidelines:

### General sizing guidelines

* Size every node to at least 8 GB of heap, whatever the format
* Then allow 150 MB to 400 MB of heap per processor, depending on format and compression
* Add 1 GB of heap for each query you expect to run concurrently
* Declare a schema on CSV and TSV datasets, or allow 31 GB per node
* Provision twice the heap in machine memory
* Keep heap at or below 31 GB, and add nodes rather than growing one past it
* Write compressed files of tens to hundreds of megabytes

### Create a sizing strategy [create-a-sizing-strategy]

There is no one-size-fits-all configuration. The figures on this page come from one corpus, and your file format, compression ratio, and column count each move them.

The best way to size this workload is to run your own queries, at the concurrency you expect, against your own files. The figures here are deliberately worst-case — every column selected, a cold node, and an aggregate on top of the scan — so they over-provision a narrower query by a margin that has not been measured. Treat them as starting points, and measure your own if the number decides a purchase.

## Sizing considerations [federation-sizing-considerations]

Keep the following things in mind when sizing for federated queries.

### Datasets use the {{esql}} engine, with extra stages for reading [extra-read-stages]

Datasets run on the same query engine as indices, which is why a dataset supports the same commands and functions an index does. What differs is the work of reading data from outside the cluster. Three stages exist only on this path, and each one holds memory:

* **Schema inference.** CSV, TSV, and NDJSON files don't describe their own columns, so {{es}} [infers a schema](esql-data-federation-schema.md) by sampling rows. This runs on the coordinating node at planning time, before any data is read, and its cost grows with the number of columns rather than with the size of the dataset. Adding data nodes doesn't divide it, and the coordinating node reads many files' metadata at once, so this is a node-level cost rather than a per-file one. It is what drives the 31 GB figure above.
* **The units being read.** {{es}} divides your files into units of work and reads several at a time. A reader holds the bytes of the unit it's working on, expanded if they were compressed, until it has finished with them.
* **Decompression and parsing buffers.** A compressed unit expands in memory before it can be read, so a file that compresses well costs *more* heap per reader, not less.

### Parquet follows your projection, and the text formats don't [format-read-differences]

Parquet carries its schema in the file, so no inference is needed. It fetches and decodes only the columns a query names, so both the bytes read and the work done scale with your projection rather than with how wide the file is.

The text formats are row-oriented, and a narrow projection buys almost nothing on the read. Every byte of a unit is fetched and decompressed, and every record is parsed in full to find its field boundaries. Selecting fewer columns with [`KEEP`](/reference/query-languages/esql/commands/keep.md) saves only the building of the result columns, not the reading or the parsing.

That is the case for Parquet on wide data in one sentence.

### Compressed text files can't be split [compression-and-splitting]

Compression reduces what every query fetches from storage, which is both faster and cheaper. For text, it costs you parallelism.

An uncompressed text file is cut at record boundaries into splits sized by [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size), and each split is read as several ranges in parallel. A gzip or zstd file can't be read from the middle, so the whole file is one unit, read by one reader and decompressed on one thread. No setting divides it, so for compressed text, parallelism comes from the number of files rather than from the size of the node.

Parquet is unaffected: it compresses each column chunk separately and is divided on row groups.

### Dataset size sets query time, not the heap you need [dataset-size-vs-heap]

A reader holds the unit it's working on, not the file that unit came from, and units waiting their turn cost nothing. A dataset of many objects needs roughly the same heap as one of few.

What sets the heap a node needs is how much is held at one time: the units being read, the queries running together, and the schema being inferred. Dataset size decides how long a query takes, which is what the [processor count](#processor-recommendation) is for.

## Best practices [federation-size-best-practices]

Where applicable, use the following best practices as starting points.

### Size every node to at least 8 GB, then allow for each processor [heap-recommendation]

Two things set the heap a node needs: a floor it needs to run this workload at all, and an allowance for each processor, because processors are what decide how many units the node reads at once.

```
heap per node  =  max( 8 GB , 2 GB + allowance x processors )  +  1 GB x concurrent queries
```

| Dataset | Allowance per processor |
|---|---|
| NDJSON, zstd | 400 MB |
| NDJSON, gzip | 330 MB |
| CSV, TSV, zstd | 170 MB |
| CSV, TSV, gzip | 150 MB |
| Uncompressed text | 30 MB |

The allowance follows your compression ratio, because what a reader holds is the decompressed bytes rather than the stored ones. Better compression raises it in proportion: NDJSON costs more per processor than CSV for the same rows because it compresses better. Measure your own ratio with `gzip -l` or `zstd -l` and scale the row accordingly.

The allowance stops growing at about 34 processors, where the node reaches the most reads it will keep in flight. Beyond that, more processors need no more heap.

Parquet isn't in the table. Its reads are bounded across the node rather than growing with each processor you add, so the 8 GB floor is what governs it. How far that holds on large nodes is still being measured.

A CSV or TSV dataset with an inferred schema needs 31 GB per node regardless of this rule, because schema inference dominates everything else. [Declare a schema](#declare-a-schema) and the rule above applies instead.

Note that these are floors rather than targets, and that the two terms were measured separately: the per-processor allowance over-provisions a large node, asking for about 13 GB on a 48-processor NDJSON node where one measured configuration ran in 8 GB. The rule takes the higher of the two on purpose. Below these figures {{es}} rejects queries with a circuit breaker rather than running them slowly, and close to them the outcome isn't reliably repeatable from one run to the next.

Provision twice the heap in machine memory, following the standing guidance that heap should not exceed half a node's memory.

Not all of this memory is attributed to the query that allocated it, so the error reporting the failure may name an unrelated operation rather than the read that filled the heap. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

To check the configured size of each node's heap, use the [cat nodes API]({{es-apis}}operation/operation-cat-nodes).

```console
GET _cat/nodes?v=true&h=name,heap.max
```

### Declare a schema on CSV and TSV datasets [declare-a-schema]

Schema inference is what makes CSV and TSV need 31 GB where Parquet and NDJSON need 8 GB, and it's the one cost you can remove outright. Set `dynamic` to `false` in a [declared mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) to treat the declaration as the complete schema and skip inference.

Because inference runs on the coordinating node, adding data nodes doesn't reduce it. Declaring a schema is the only remedy.

Declaring a schema doesn't remove every planning-time read. A query that needs statistics still reads metadata from each file, whatever the schema resolution strategy.

### Size processors from the query time you'll accept [processor-recommendation]

Processors are what buy query speed. As a rule of thumb:

```
processors  =  stored GB  x  concurrent queries  x  K  /  target seconds
```

`K` is the cost in processor-seconds per stored GB: **300** for gzipped NDJSON, **400** for gzipped CSV, and **250** for gzipped TSV. Parquet is priced per column rather than per byte, because it reads only the columns a query names.

For example, a 19 GB NDJSON dataset, four queries at once, answered inside a minute, needs `19 x 4 x 300 / 60` = 380 processors, so eight 48-processor nodes. Accept ten minutes instead and the same workload needs 38 processors, which is one node.

This holds only while your files divide into more units than the node is already reading. For compressed text, one file is one unit on one thread, so more processors never make a single object read faster. They let more objects be read at the same time, which is why file count is the lever for that format.

### Aim for compressed files of tens to hundreds of megabytes [file-size-recommendation]

File layout is a property of your data rather than of your cluster, so you can get it right without knowing what will query it.

Too few files leave readers idle, and for compressed text this is a hard ceiling rather than a preference, because one file is one reader. Too many files cost more than they return, because each object is fetched over its own high-latency requests, so a query over very small objects spends more time asking for data than reading it. A dataset is also capped at [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files).

For Parquet, the equivalent lever is the row group rather than the file, because one large Parquet file still parallelizes. Leave row group sizes at their defaults unless your files are very wide.

### Add nodes rather than heap above 31 GB [scale-out-recommendation]

Heap has a ceiling independent of this workload. Past about 31 GB the JVM stops compressing object pointers, so each object takes more space and a larger heap holds less.

When the figures above ask for more than that, add nodes instead of growing one. Each node has its own heap and reads its own share of the files.

### Lower read concurrency before adding heap [lower-concurrency]

If queries are being rejected on a text dataset, lowering [`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) is a more reliable remedy than adding heap, at the cost of some wall-clock time. The setting is static, so each node needs a restart.

## Where these figures come from [provenance]

The figures on this page were measured on a single node of 48 processors, against one corpus of gzipped files with an inferred schema, running a full scan with an aggregate over every column at four concurrent queries.

The per-processor allowances are derived from how much each reader holds at a measured compression ratio, rather than measured across processor counts directly.

Not yet measured: Parquet's per-processor allowance, uncompressed files, clusters of more than one node, concurrency other than four queries, and the heap a text dataset needs once its schema is declared.

To reduce the work a query does rather than add capacity to it, refer to [how external data is read](esql-data-federation-querying.md#how-external-data-is-read) and [optimize {{esql}} query performance](esql-query-performance.md).
