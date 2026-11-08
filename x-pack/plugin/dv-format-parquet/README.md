# dv-format-parquet

A prototype that serves Parquet files as read-only indices. A mount creates an index whose segments are stubs that reference the file,
and the doc values of the index (`NUMERIC` and `BINARY`) are read from the file by the ES|QL Parquet reader of the
`esql-datasource-parquet` plugin, which this plugin extends. Nothing in the file is copied into the index.

It is a spike, not a merge-ready change. The limitations are at the end.

## How to run it locally

Start a node from this branch with a trial license (a `GET _doc` of a synthetic-source index needs one) and with a repository
location that holds the file. The repository is how the node reads the file, so credentials stay in the repository settings.

```sh
./gradlew run -Drun.license_type=trial -Dtests.heap.size=6g \
  -Dtests.es.xpack.security.enabled=false -Dtests.es.path.repo=$HOME/temp
```

Register the repository (a read-only `fs` repository over the directory that holds the file; an `s3` repository works the same way),
look at what a mount would do, and then mount. The ClickBench file stores its times as bare integers, so the request says what they
are.

```
PUT /_snapshot/lake
{ "type": "fs", "settings": { "location": "<HOME>/temp", "readonly": true } }

POST /_parquet/_preview
POST /_parquet/_mount
{
  "repository": "lake",
  "paths": ["hits.parquet"],
  "index": "hits",
  "fields": {
    "EventTime": { "type": "date", "conversion": "seconds_to_millis" },
    "ClientEventTime": { "type": "date", "conversion": "seconds_to_millis" },
    "LocalEventTime": { "type": "date", "conversion": "seconds_to_millis" },
    "EventDate": { "type": "date", "conversion": "days_to_millis" }
  },
  "index_settings": { "index.number_of_replicas": 0 }
}
```

The preview returns the files, the mapping that their schema gives, the columns that have no field (with the reason), and the indices
that a mount would create. A mount creates one index (`"index"`) or a data stream over indices (`"data_stream"`, which needs a
`@timestamp` field, so a column named `timestamp_field` with a type override). The other keys of the body are `exclude`,
`max_rows_per_index` and `index_settings`, and `fields` can also rename a column (`name`) or choose another type.

### Mounting through an ES|QL data source

Instead of a repository, a mount can read its files through an ES|QL data source (`PUT /_query/data_source/<name>`), which holds the
type and the settings of the store: the authentication, an endpoint. Give `data_source` instead of `repository`; the paths are then
complete URIs, because a data source has no base path. The data source API needs the license that ES|QL external sources need (a trial
works).

```
PUT /_query/data_source/clickbench
{ "type": "s3", "settings": { "auth": "anonymous" } }

POST /_parquet/_mount
{
  "data_source": "clickbench",
  "paths": ["s3://clickhouse-public-datasets/hits_compatible/hits.parquet"],
  "index": "hits",
  "fields": { "EventTime": { "type": "date", "conversion": "seconds_to_millis" } },
  "index_settings": { "index.number_of_replicas": 0 }
}
```

`auth: anonymous` reads the public ClickBench bucket without credentials (a snapshot repository of type `s3` cannot do that, because the
`repository-s3` module always signs its requests). The index gets `index.parquet.data_source` in the place of `index.parquet.repository`,
and a segment stores `datasource://<name>/<uri>`. Notes:

- the mount checks each URI with the validator that ES|QL applies to a dataset of that data source type (the scheme, so `gs://` is rejected for
  an `s3` data source, and the endpoint allow-list), before it reads anything;
- a data source with a custom endpoint needs the endpoint in the node setting `esql.external.allowed_endpoint_hosts`;
- the `region` of a data source is ignored by ES|QL (it belongs to a dataset); with no region and no endpoint, the S3 client signs for
  `us-east-1` and follows redirects to the region of the bucket;
- a segment resolves the data source from the cluster state each time it opens, so a data source that is deleted, or renamed, or whose
  endpoint is no longer allowed, breaks the segments that open afterwards (a node that restarts, a shard that moves), not the open ones;
- the disk cache keys a file by the stored location, which includes the name of the data source.

A node reads the files through the repository on every query, which is slow for an object store. The node-local disk cache of file
ranges is off until you give it a size. It needs exactly one data path and as much free space in it as its size, and a node that cannot
create it logs why and reads the repository directly.

```
-Dtests.es.xpack.parquet.cache.size=8g -Dtests.es.xpack.parquet.cache.region_size=16mb      # to ./gradlew run
```

Then use the index like any other:

```
POST /_query?format=txt
{ "query": "FROM hits | STATS a = AVG(UserID)" }

POST /_query?format=txt
{ "query": "FROM hits METADATA _id | WHERE EventTime >= \"2013-07-14\" | KEEP _id, UserID, EventTime | LIMIT 3" }

GET /hits/_doc/5?_source_includes=UserID,EventTime
```

The `_id` of a document is its row number in the index, so `GET _doc`, `mget` and `ids` queries are lookups of a row.

## Checking the results against an ES|QL data source

`clickbench/compare_with_data_source.py` runs ES|QL queries against the mounted index (`hits`) and against the same file read through an
ES|QL data source (`hits_ds`), and compares the results. The data source uses the ES|QL Parquet reader and no doc values format, so it
tells whether the iterators, skippers, conversions and mount of this plugin change a result. It cannot find a bug in the ES|QL reader
itself, because both sides use its decoding.

1. Serve the file through an S3-compatible endpoint that the node may reach (the check used a small local server for one file, path
   style, anonymous), and start the node with `-Dtests.es.esql.external.allowed_endpoint_hosts=localhost:9111` and a heap of 12 GB,
   because some queries group by a string with many values.
2. Mount the file as above, and define the dataset over the same file:

```
PUT /_query/data_source/fake
{ "type": "s3", "settings": { "auth": "anonymous", "endpoint": "http://localhost:9111", "region": "us-east-1", "addressing_style": "path" } }
PUT /_query/dataset/hits_ds
{ "data_source": "fake", "resource": "s3://bench/hits.parquet" }
```

3. Fetch and parse the 43 queries of the internal Rally track (a private repository, so the file is not kept here), and run the check:

```
gh api repos/elastic/rally-internal-tracks/contents/clickbench/operations/02_all_queries.json \
  -H "Accept: application/vnd.github.raw" > 02_all_queries.json
python3 clickbench/parse_rally_queries.py 02_all_queries.json rally_queries.json
python3 clickbench/compare_with_data_source.py rally_queries.json            # all queries, or give name prefixes: q12_ q31_
```

The check converts the four time columns of the data source with the same conversions as the mount (it uses `EVAL` into new names and
`RENAME`, not `EVAL EventDate = ...`: with the same name, ES|QL answers `MIN` and `MAX` of the data source from the footer statistics of the
raw column and returns the raw value as a date). Results are compared in order, with a tolerance for floating point sums; after a `SORT`
with a `LIMIT` the rows may differ among ties, so the sort keys must be equal and each row must be a row of the oracle, and a group of a tie
group that is too long to fetch is checked with a query for that group. The check prints one line for each query with both times and ends
with a summary.

## What is where

The classes sit in five places. The package `org.elasticsearch.xpack.parquet` holds only the plugin class (`ParquetDocValuesPlugin`).
The package `dv` holds everything about the doc values format and the model of a Parquet file that it needs, and it uses no other package
of the plugin. The package `repository` holds the repository access and the index settings of a mounted index, and uses `dv` for the
conversions. The packages `engine` and `mount` use `dv` and `repository`.

- `mount/` the preview and mount actions (`POST /_parquet/_preview`, `POST /_parquet/_mount`): they read the footers through the
  repository, derive the mapping, plan how the row groups fill the indices, and create the indices (and a template, an alias and a data
  stream for `data_stream`).
- `engine/` (`ParquetEngineFactory`, `ParquetReadOnlyEngine`, `ParquetAttachCodec`, `ExternalParquetCodecReader`) the read-only engine. A shard that is created empty gets one stub segment for each
  piece (a file, or a range of its row groups) through `IndexWriter#addIndexes` with `ParquetAttachCodec`, and the engine looks a
  document up by its row number.
- `dv/` holds everything about the doc values format and its readers, and `ParquetFileInfo` (with its `LogicalKind`), `Conversion` and
  `ParquetBlocks`, the footer model and the conversions. `ParquetDocValuesFormat` is the format of a stub segment: one small file
  with the piece and the column bindings, and no values. Its inner classes are `MetaFile`, which reads and writes that file, and
  `ReferenceConsumer`, which copies the reference at attach time. `ParquetDocValuesProducer` serves the doc values.
- `ColumnWindow` (with `FileLayout`), `NumericWindow`, `BytesWindow`, `DecodedPageCache` (with `ColumnPage`) the iterator view over the ES|QL reader. An iterator reads one
  immutable page of decoded values at a time. A move forward that continues the page scans the row group (each page is decoded once),
  and an isolated or backward move reads by position (about one pass over the column chunk for each call). A string column scans from the
  second load in a row group, and a numeric one from the third, because decompressing a string chunk is what a positional read pays for.
  The pages that scans decode are shared through a per-segment `DecodedPageCache`, because a query reads a column through several live
  iterators on one thread (the scorers of its filter and the loader), and each decoding every page for itself multiplied the work.
  When the reader returns dictionary-encoded strings as ordinals, the page keeps them as ordinals into a dictionary of the values that
  it uses. `ParquetNumericDocValues` and `ParquetBinaryDocValues` also read a page of documents into a block for ES|QL (`tryRead`): a
  block of dictionary pages is ordinals into the distinct values of the block (a constant when there is one), so grouping hashes a
  distinct value once. See the findings log in the plan.
- `ScanRegistry` closes the scans that no iterator uses any more, because a Lucene iterator has no close hook: when a scan opens, when
  a scan is touched, on a timer that the plugin runs every 5 seconds, and when the segment closes. A scan that is idle for 2 seconds is
  closed; the window that owned it opens another when it is read again.
- `ExternalFilePiece.ColumnStats`, `ParquetDocValuesSkipper` the doc values skippers. When a piece is attached, the footer's minimum, maximum and null count
  of each `INT32` or `INT64` column become the bounds and the document counts of each row group (in the value space of the field, so the
  conversion and the reader's scale are already applied), and the segment stores them in its `.dvm`. The skipper serves them without a
  request, so range and term queries skip or match whole row groups, sorts prune, and the mapper's range check (a date field) sees real
  bounds. A column with unreliable or missing statistics, an unknown null count, an unsigned 32 or 64 bit type, a decimal or a time gets
  no skipper, or the widest range, and is read in full: slower and never wrong.
- `repository/` (`FileLocation` with `RepositoryLocation` and `DataSourceLocation`, `ParquetFiles`, `RepositoryStorageObject`,
  `ParquetBackedSettings`) a file in a repository or of an ES|QL data source as the ES|QL reader sees a storage object (`ParquetFiles`
  opens both. A data source is read through `DataSourceStorageResolver` of the ES|QL plugin, which returns a `BorrowedStorageObject` that
  holds the borrow of the storage provider until the segment closes), and the `index.parquet.*` settings that tie an index to its repository, pieces and fields. `ParquetBlobCache` and
  `CachedStorageObject` are the disk cache of file ranges: the decorator sends every read of the reader through the cache, which is a
  thin layer on the shared cache of the `blob-cache` plugin (its own cache file, `parquet_cache`, and its own fetch thread pool).

## Checking concurrency and memory

`clickbench/concurrency_check.py` runs a mix of ES|QL queries, Query DSL searches and gets from several clients at once and checks that
every result equals the result of one client, that the heap after a collection does not grow from round to round, and that no task is
left. `ParquetConcurrencyTests` does the same for one segment with a circuit breaker that accounts the blocks and buffers, and the test
framework checks that it is back to zero.

## Limitations

- Only top-level `INT32`, `INT64` and `BYTE_ARRAY` columns are served. Floats, booleans, decimals, nested and repeated columns are
  listed as unmapped by the preview.
- A Parquet `DATE` and a `TIMESTAMP` are converted by the ES|QL reader (to epoch milliseconds, and to epoch nanoseconds for micros and
  nanos), so they need no conversion. A column that holds a bare number needs one, as in the example above.
- No `tryTermEqualIterator`, `tryContainsIterator` or `tryLengthIterator`: a profile of the string filters showed that the reader's own
  decoding is more than 85% of the CPU, so a matcher of the pages could save about 10%. A filter on a column whose row groups all
  overlap (a random id) gets nothing from the skipper and reads the whole column.
- `_id` is doc values only and has no postings (the mount sets `index.mapping.id_field.indexed: false`, which needs the columnar mode).
  An `ids`, `term` or `terms` query on `_id` is answered by a lookup of the row, not by a scan, but a range or prefix query on `_id`
  scans the synthesized values of every document.
- There is no memory backpressure. The reader runs with a byte budget that never refuses and a block factory with no circuit breaker,
  because a Lucene iterator cannot park the way an ES|QL driver does. The iterators bound their open scans themselves.
- The disk cache (see above) is off by default, needs a node with one data path, and keys a file by its location and length, so it
  assumes that a file is never replaced by another file of the same length under the same name. A node that starts again fills it
  anew. There is no prefetch.
- The ES|QL plugin must be on the node, because this plugin extends it.
