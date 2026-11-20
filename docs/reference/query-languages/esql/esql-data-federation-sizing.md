---
navigation_title: "Sizing and performance"
description: "Size your Elasticsearch cluster for ES|QL Data Federation workloads on Amazon S3, by file format, compression, and query concurrency."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# Size your {{es}} cluster for {{esql}} Data Federation workloads

With [data federation](esql-data-federation.md), you can query tabular data held on [Amazon S3](esql-data-federation-s3.md), in CSV, TSV, NDJSON and Parquet files, without ingesting it into {{es}} first. The guidance on this page is for those formats on Amazon S3.

This guide covers how much heap and how many cores your cluster needs to run your queries at the concurrency you expect, and how to lay out your files so those cores stay busy.

For data held in {{es}} indices, refer to the [Size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md) guide instead.

## General guidelines [general-guidelines]

* **Prefer Parquet to the text formats** where you have the choice. It needs no schema inference, reads only the columns a query names, and stays splittable when compressed.
* **Prefer mid-sized files**, from tens of megabytes to several hundred megabytes.
* **Prefer an explicitly declared schema for text datasets.** [Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false`.
* **Allocate 1 GB to 2 GB of heap per core**, and never less than **2 GB** for Parquet, **2 GB to 4 GB** for uncompressed text, or **4 GB** for compressed text. A text dataset whose schema is inferred needs **2 GB to 4 GB** on top.
* **Add 1 core per 3 to 4 concurrent queries** on text, and 1 core per 8 to 10 on Parquet.
* **Keep heap below the compressed object pointer threshold**, which is 26 GB on most systems and as much as 30 GB on some. Scale horizontally across nodes instead.

These figures are recommended sizings rather than requirements, and queries may still run successfully on smaller configurations. They assume cold reads, where every query fetches its data from Amazon S3 rather than from a warmed cache. How a dataset behaves then depends on characteristics of your own data: how well it compresses, how its files split, the row group sizes in your Parquet files, the width of your schema, and the size and number of your objects. A configuration that suits one dataset may not suit another. Size against your own queries, at the concurrency you expect, and watch how the cluster behaves.

## Background [background]

Data federation and index workloads share the same query engine, but their read paths differ. Aggregation, filtering, sorting and the rest of the {{esql}} pipeline are the same code, so a dataset supports the same commands and functions an index does, with [known exceptions](esql-data-federation-querying.md#limitations).

What differs is the work of reading data that lives outside the cluster. A few stages exist only on this path, and these are what drive the memory a query needs and the time it takes:

* **Schema inference.** Text files carry no description of their own columns, so {{es}} establishes one by [sampling rows](esql-data-federation-schema.md) from the dataset's files. This happens before the scan, and it reads from S3, so it costs time and storage requests. How much depends on the number of columns and, with the `union_by_name` and `strict` [resolution strategies](esql-data-federation-schema.md#choose-a-schema-resolution-strategy), on how many files the query reads, because both sample every one of them. `first_file_wins`, the default from 9.6, samples one. [Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) with [`dynamic`](esql-data-federation-schema.md#mapping-properties) set to `false` to skip it.
* **Split distribution.** The files a query reads are divided into units of work called splits, which are distributed across the data nodes. For uncompressed text, [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size) sets how large each one is. Parquet takes its splits from the row groups listed in the file footer instead, and a compressed text file is one split whatever that setting says. How many a node reads at once follows its core count.
* **Staging and decompression buffers.** {{es}} allocates memory to hold the data fetched from Amazon S3, and further memory to decompress it and to parse it into the columnar form the query engine works on. Compressed data is decompressed into bounded, recycled buffers rather than expanded whole, so what a reader holds follows how many buffers are in flight rather than how well the data compresses.

The formats then diverge. Parquet carries its schema in the file footers, so no sampling is needed. It is read chunk by chunk: a reader takes one row group at a time and, within that group, fetches and decodes only the columns the query asks for. Both the bytes read and the work done therefore follow your projection rather than the width of the file, so narrowing a query with [`KEEP`](/reference/query-languages/esql/commands/keep.md) reads less.

The text formats work the other way round, because a file is read as a continuous stream and and a row has to be parsed in full before any of its columns can be used. Narrowing a text query therefore does not read less: the same bytes are fetched and decompressed however few columns you ask for. What the projection saves is the decoding of the columns you left out, and the work of building the result.

Parallelization works differently by format too, because each one divides a file into a different unit of work. For Parquet that unit is a row group, so a file holding several of them is read by several readers at once, while a file written as a single row group is not. For uncompressed text it is a split, so a larger file is read as several of them. For compressed text it is the whole file: a compressed file cannot be read from the middle, so it cannot be divided at all. One reader decompresses it, though the rows it produces are parsed in parallel.

The heap a cluster needs to read data does not depend on how large your dataset is, because data is streamed through the cluster rather than loaded into it. One thing does grow with it: the coordinating node reserves planning memory for every file a query discovers, so many small objects cost more there than the same bytes in fewer. What the size of the dataset does affect is how long a query takes, and so the number of cores you need to meet a latency target.

## Size your files for your workload [file-layout]

File layout is a property of your data rather than of your cluster.

Aim for mid-sized files, from tens of megabytes to several hundred megabytes.

Amazon S3 has high latency on every request, so a query gets its throughput from reading many files in parallel rather than from any single read being fast. Both extremes cost throughput, in different ways. If a dataset yields too few splits, whether from too few files or from compressed text files that are one split each, there is not enough work to share out and some of your cores sit idle however large the cluster is. If it holds too many, each one still costs a full request, and a query over very small files spends more of its time waiting on those requests than reading data.

There is also a hard limit on how many files one query may keep after its listing filters have run, set by [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files). A query that keeps more than that fails, and pruning is the first remedy, because a partition filter or a narrower resource pattern can bring a large dataset under the limit without changing anything else. Beyond that, split the data across several datasets, or raise the setting on a self-managed cluster.

Compressing your files reduces what every query has to fetch, which makes queries faster and reduces what your storage provider bills you. For the text formats it costs you parallelism, because a compressed text file cannot be read from the middle and so is handled by a single reader.

Parquet is not affected, because it compresses each column chunk inside the file and stays divisible. For Parquet, what matters is the row group rather than the file, so leave row group sizes at their defaults unless your files are very wide.

## Sizing your cluster [sizing]

A node running federated queries needs memory for three things at once: for {{esql}} and the rest of {{es}}, for the planning and schema inference that happen before the scan, and for the splits the node is reading at that moment. The figures below cover the reading and the planning, so allow for {{esql}} itself on top of them.

The format you query makes a large difference here. Parquet needs less heap than the text formats to do the same work, and less again for each query you run alongside it.

Cores matter as much as heap, and the two move together. Adding cores lets a node read more splits at once, and each of those splits needs memory to hold, so a node given more cores needs more heap to go with them.

### Where to start [heap-per-node]

Allocate 1 GB to 2 GB of heap per core, on at least 2 cores per node, and never less than the figure for your format:

* **2 GB** for Parquet
* **2 GB to 4 GB** for uncompressed text
* **4 GB** for compressed text

A text dataset whose schema is inferred needs 2 GB to 4 GB on top of that.

### Sizing for concurrency [concurrency]

Queries that run at the same time share a node's cores between them, so the way to serve more of them is to add cores, and the heap those cores need.

On a text dataset, allow about **1 core for every 3 to 4 concurrent queries**. For uncompressed text you can add those cores to a node you already have. For compressed text you should not: the memory a node needs per core grows as the node gets wider, so a larger node needs more heap than the extra cores give back. Add the cores as additional nodes instead.

Parquet is less demanding, and needs about **1 core for every 8 to 10 concurrent queries**.

Whichever format you are reading, more nodes give you more parallelism, and no single node should be given more heap than its compressed object pointer threshold allows. That threshold is 26 GB on most systems and as much as 30 GB on some, and [Set the JVM heap size](/reference/elasticsearch/jvm-settings.md#set-jvm-heap-size) explains how to check it on yours.

### Specify a mapping on text datasets [declare-schema]

CSV, TSV and NDJSON files do not describe their own columns, so {{es}} works the schema out by sampling rows before the query runs. You can avoid that cost entirely by declaring the schema yourself.

[Specify a mapping](esql-data-federation-schema.md#declare-a-schema-explicitly) on the dataset and set [`dynamic`](esql-data-federation-schema.md#mapping-properties) to `false`. Both are needed: a mapping on its own still leaves the rest of the schema to be inferred, and it is `dynamic: false` that stops the inference.

### Increase read concurrency for more parallelism [read-concurrency]

[`esql.external.max_concurrent_requests`](esql-data-federation-cluster-settings.md#esql-external-max-concurrent-requests) sets how many requests to Amazon S3 a node has outstanding at any moment. Its default is worked out from the node's core count, its heap, and the request circuit breaker limit, whichever is lowest.

Raising it does not on its own give you more read parallelism. The value you set is capped by a memory term derived from the node's heap and the request circuit breaker limit, so a node raises its reads in flight only when it has the memory to hold them. Raise the heap, and the limit rises with it. It is a [static setting](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), so the node has to be restarted before a change takes effect. Static settings cannot be changed on {{ech}} or on {{es-serverless}}, so this applies to self-managed clusters. On the other two, size with [Sizing for {{ech}}](#sizing-ech) or [Sizing for {{es-serverless}}](#sizing-serverless) instead.

### Sizing for {{ech}} [sizing-ech]

On {{ech}} you choose a size per zone in RAM and a hardware profile for the deployment. Heap is half that RAM, and the core count is the RAM times a ratio set by the profile, so a CPU optimized profile gives about 1 GB of heap per core, general purpose about 2 GB, and storage optimized about 3.5 GB.

Use a general purpose profile, which gives the 1 GB to 2 GB per core this page asks for, and select a size per zone of twice the heap you need. Avoid CPU optimized, where the heap each core gets leaves nothing spare for compressed text or an inferred schema.

### Sizing for {{es-serverless}} [sizing-serverless]

On [{{serverless-full}}](docs-content://deploy-manage/deploy/elastic-cloud/serverless.md) you do not choose nodes, heap or cores. Capacity is managed for you, and a project is scaled from the search load it sees, which does not take data federation into account. To follow this guide, [create an {{es-serverless}} project](docs-content://deploy-manage/deploy/elastic-cloud/create-serverless-project.md), which is the only type that lets you set a capacity floor, through [Minimum Search VCUs](docs-content://deploy-manage/deploy/elastic-cloud/project-settings.md#minimum-search-vcus). Raise it ahead of an expected increase in query load and lower it afterwards, because you are billed for at least that many [Search VCUs](docs-content://deploy-manage/cloud-organization/billing/elasticsearch-billing-dimensions.md#elasticsearch-billing-information-about-the-vcu-types-search-ingest-and-ml) while it is set.

Work out the heap and the cores this page gives you, then allow 2 Search VCUs for every GB of heap and 2 Search VCUs for every core, and set the minimum to whichever of the two figures is larger. You can run federated queries from the other project types as well, but their capacity follows their own configuration, and data federation is not currently one of the factors in it.

## Sizing for performance [performance]

Data federation queries scale with the number of cores available to them, and the time a scan takes is proportional to the amount of data it reads.

If you want your queries to run faster, add cores, and scale the heap alongside them at 1 GB to 2 GB per core, plus the budget for the [concurrent queries](#concurrency) you expect. Cores added without the memory to match will not help, because each one needs somewhere to put the data it reads. For compressed text, add those cores as more nodes rather than as a wider one.

## Example configurations [examples]

Both of these size a cluster for 10 concurrent queries.

### NYC TLC trip records, Parquet [example-taxi]

The [TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is published as 1 Parquet file per month, of 50 MB to 110 MB. 5 years is about 60 files.

This is a Parquet dataset, so the initial heap is 2 GB. At 1 core per 8 to 10 concurrent queries, 10 concurrent queries need 1 to 2 cores, which is below the 2-core minimum, so use 2. At 1 GB to 2 GB of heap per core, 2 cores need 2 GB to 4 GB.

Start with 1 node, with at least 2 cores and at least 4 GB of heap. On {{ech}} that is an 8 GB size per zone in 1 zone, and on {{es-serverless}} it is a minimum of 8 Search VCUs.

### Backblaze drive stats, gzipped CSV [example-backblaze]

[Backblaze drive stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) is published as one CSV file per day, with a wide schema of SMART attributes. This example assumes those daily files repacked as `.csv.gz`.

This is compressed text, so the initial heap is 4 GB. At 1 core per 3 to 4 concurrent queries, 10 concurrent queries need 3 to 4 cores. Compressed text serves fewer concurrent queries as a node widens, so those cores belong on separate nodes: 2 nodes of 2 cores each. At 1 GB to 2 GB of heap per core, 2 cores need 2 GB to 4 GB, which is less than the 4 GB this format starts at, so use 4 GB.

Start with 2 nodes, each with at least 2 cores and at least 4 GB of heap. On {{ech}} that is an 8 GB size per zone across 2 zones, and on {{es-serverless}} it is a minimum of 16 Search VCUs.

If the schema is inferred rather than specified, add a further 2 GB to 4 GB of heap to each node.

## Troubleshoot [troubleshoot]

Because the figures on this page are starting points, the configuration you end up with should be the one your own data asks for. Watch for the following as you load it up:

* **Circuit breaker rejections**, which mean there is insufficient memory for the workload.
* **Queries slowing as concurrency rises**, with nothing being rejected, which means your queries are competing with each other for readers.
* **Query times that do not improve when you add capacity**, which points at the file layout rather than the cluster: too few splits leave the extra cores with nothing to pick up.
* **Planning time growing with your file count**, which points at schema inference or, on Parquet, at footer reads on the coordinating node.

### `circuit_breaking_exception` with `Data too large` [breaker]

A circuit breaker has rejected the query because there is insufficient memory for the workload. Give the node more memory, and raise the heap with it. If you add cores at the same time, scale the heap with those too, at 1 GB to 2 GB per core.

On a text dataset, the usual cause is schema inference at planning time. [Specify a mapping](#declare-schema) with `dynamic` set to `false` first, which removes that cost instead of paying for it.

The figures on this page cover reading the external data. {{esql}} needs further memory for the query itself, for aggregations, sorting and grouping, so allow for that on top. Refer to [circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

### Queries are slow under load [slow-under-load]

Queries that still return results but take longer as load rises are competing with each other for readers. Nothing has run out, there are simply not enough readers for every query at once, so each one waits its turn.

To improve query performance under load:

* **Add cores, and scale the heap with them** at 1 GB to 2 GB per core. For compressed text, add those cores as more nodes rather than as a wider one.
* **Check your file layout.** A query can only use as many readers as your data gives it work for. If the dataset yields too few splits, some cores will sit idle no matter how large you make the node, because a compressed text file is one split however large it is. Refer to [Size your files for your workload](#file-layout).
* **Specify a mapping** on text datasets, which takes schema inference out of planning.
* **On a self-managed cluster, raise [`esql.external.max_concurrent_requests`](#read-concurrency)**, together with the heap increase it needs to take effect.

## Related pages [related]

- [Query external datasets](esql-data-federation-querying.md): how the engine reduces storage reads, and how to query datasets alongside indices
- [Schema inference](esql-data-federation-schema.md): how {{es}} discovers schemas, and how to specify a mapping instead
- [Dataset settings](esql-data-federation-dataset-settings.md): file discovery, parsing, and reader behavior, including [`target_split_size`](esql-data-federation-dataset-settings.md#target-split-size)
- [Cluster settings](esql-data-federation-cluster-settings.md): request concurrency, file-discovery limits, and caching
- [Optimize {{esql}} query performance](esql-query-performance.md): general {{esql}} tuning, which applies to datasets too
- [Costs](esql-data-federation-costs.md): what your storage provider bills for the reads a query generates
