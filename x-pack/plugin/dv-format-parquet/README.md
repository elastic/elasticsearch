# dv-format-parquet

`dv-format-parquet` is an x-pack plugin with a Lucene `DocValuesFormat` that serves numeric and binary doc values straight from Parquet files in an object store, so that an index can be "mounted" over existing files without copying or re-indexing them. The plugin is experimental: it is not on `main` yet, the format and the mount API will change, and we currently do not promise any compatibility between versions.

## What a mount is and why the segments are only stubs

A mounted index has regular Lucene segments, but each segment is a stub. It has no data files of its own except one small meta file (extension `dvm`, segment suffix `ESParquet900_0`) that records which Parquet file, which row-group range (a "piece") and which columns the segment serves, plus a snapshot of the footer statistics for each column. When the segment opens, the producer reads the doc values for a field from the column chunks of the Parquet file through the registered snapshot repository, so the repository decides where the bytes live (`fs` today, S3 and friends in principle). Document `n` of a piece is row `n` of its row-group range.

Note that the plugin never indexes anything. Segments are attached with `IndexWriter#addIndexes(CodecReader)` through `ExternalParquetCodecReader` and `ParquetAttachCodec`, and the shard runs a `ParquetReadOnlyEngine` (loaded through `EnginePlugin`), which is why a mounted index is read-only and why the same code path serves primaries and replicas.

## How a Parquet column becomes a field

Every field has `index: false` and keeps its values only in doc values. The mapping must be complete before any segment exists, because no document goes through the document parser, so `ParquetMappingDeriver` derives it from the footer schema:

| Parquet column | Field | Doc values |
|---|---|---|
| `INT32` / `INT64` (including `UINT_*`) | `integer` / `long`, or `date` with a conversion | numeric |
| `BYTE_ARRAY` (string) | `keyword` with `multi_value: false` | binary |
| anything else (currently float, double, boolean, decimal, nested) | not mapped, reported in `unmapped` with the reason | none |

A mounted index uses `index.mode: columnar` with a synthetic `_id` (the row number) and the setting `index.mapping.id_field.indexed: false`, so `_id` has doc values only and `_source` is synthesized from the doc values. Timestamps in Parquet are frequently not in milliseconds (ClickBench stores `EventTime` as epoch seconds and `EventDate` as days in a `UINT_16`), so a field can carry a conversion: `seconds_to_millis`, `days_to_millis`, `micros_to_millis` or `micros_to_nanos`.

The values of a segment are read per column chunk, with REQUIRED and OPTIONAL flat columns, `PLAIN` and dictionary encodings, data pages v1 and v2, and `UNCOMPRESSED`, `SNAPPY` and `GZIP` compression. The footer min/max and null counts per row group also back a doc values skipper (`ParquetDocValuesSkipper`, one interval per row group), which is what lets range filters and sorts skip row groups. We only declare a skipper when the statistics make the value counts exact, and a server-side safeguard in `DateFieldMapper#isFieldWithinQuery` falls back to `INTERSECTS` when no segment has a skipper, because a missing skipper must never be read as "all rows are within the range".

## How to mount a file

The plugin adds two APIs (`cluster:admin/parquet/preview` and `cluster:admin/parquet/mount`). The files must be reachable through a registered snapshot repository, and `paths` are relative to its base path. `_preview` takes the same body as `_mount` and returns the plan (indices, row-group pieces, mapping and unmapped columns) without creating anything.

```
PUT _snapshot/lake
{ "type": "fs", "settings": { "location": "/data/lake" } }

POST _parquet/_mount
{
  "repository": "lake",
  "paths": ["clickbench/hits.parquet"],
  "data_stream": "hits",
  "timestamp_field": "EventTime",
  "fields": {
    "EventTime": { "conversion": "seconds_to_millis" },
    "EventDate": { "type": "date", "conversion": "days_to_millis" }
  },
  "exclude": [],
  "max_rows_per_index": 50000000,
  "index_settings": { "index.number_of_replicas": 0 }
}
```

The body keys are:

- `repository`, `paths`: where the files are.
- `data_stream` or `index` (exactly one): a data stream creates a template, one backing index per `max_rows_per_index` rows (split on row-group borders, one shard each), an alias, and then migrates the alias to a data stream, with cleanup on failure. `index` creates one plain index and fails when the plan needs more than one.
- `timestamp_field`: the column that becomes `@timestamp`.
- `fields`: per-column overrides of `name`, `type` and `conversion`. `exclude` leaves columns out.
- `index_settings`: extra index settings. The settings that the mount controls itself are rejected.

Behind the scenes a mounted index carries three index settings, which we do not expect users to set by hand: `index.parquet.repository`, `index.parquet.pieces` (`path#firstRowGroup+count`, comma separated) and `index.parquet.fields` (`field:column[:conversion]`). When several files go into one index, `ParquetFileInfo#requireCompatibleWith` checks that the schemas of the mapped columns agree. On a warm local file the mount of the 100-million-row ClickBench `hits.parquet` (226 row groups) takes about 1.1 s, because mounting only reads footers.

## Architecture: from a mount request to a doc values read

The plugin has three moments in which it does something, and they run in different places: the mount request plans indices and reads footers, the start of a shard attaches stub segments, and a query reads column chunks. Nothing in the first two moments reads data, and only the third one touches the object store for values. The picture below shows the pieces and who calls whom; the sections after it follow the three moments.

```
                      mount request (the node that handles it)
  POST _parquet/_mount ──► ParquetMountSpec ──► ParquetMountAnalysis ──► MountPlanner
                                                   │ footers only          │ pieces per index
                                                   ▼                       ▼
                                          ParquetFooterReader     ParquetMappingDeriver
                                                   │                       │
                                                   ▼                       ▼
                                  RepositoryParquetObject      template, indices (index.parquet.* settings),
                                       (BlobContainer)         alias, MigrateToDataStreamAction

                      shard start (every node that holds a copy)
  EnginePlugin ──► ParquetEngineFactory ──► attachIfEmpty: ParquetFooterReader ──► ExternalFilePiece + ColumnBinding
                          │                                                        │
                          │                      ExternalParquetCodecReader ──► IndexWriter#addIndexes (ParquetAttachCodec)
                          │                                                        │ ParquetDocValuesConsumer#merge
                          ▼                                                        ▼
                  ParquetReadOnlyEngine  ◄───────── commit ────────────  stub segment: .dvm (ParquetMetaFile)

                      query (search thread or ES|QL driver thread)
  Lucene opens the segment ──► ParquetDocValuesFormat (SPI) ──► ParquetDocValuesProducer
        ParquetNumericDocValues / ParquetBinaryDocValues ──► ColumnChunkReader ──► PageDecompressor ──► LongPageDecoder / BytesPageDecoder
                                                                  │ readRange(chunk)
                                                                  ▼
        ParquetObject ──► CachedParquetObject ──► ParquetBlobCache (SharedBlobCacheService, file parquet_cache)
              │                                        │ miss: one ranged read of the page-aligned range
              ▼                                        ▼
        RepositoryParquetObject ───────────► BlobContainer#readBlob(position, length)  (an fs or s3 repository)
```

### What the mount request decides, and why it never reads data

`TransportMountParquetAction` and `TransportPreviewParquetAction` call `ParquetMountAnalysis.analyze`, which opens each file through `RepositoryParquetObject` and reads only its footer with `ParquetFooterReader` (typically one request for a footer under 512 KB, and two for a larger one such as the 2.4 MB footer of ClickBench). From the footers, `ParquetMappingDeriver` derives the mapping (a `keyword` with `multi_value: false` for a string, a numeric or `date` field for an integer, and everything else reported as unmapped with a reason), and `MountPlanner` cuts the files into "pieces", each a whole file or a range of row groups, so that one backing index never exceeds `max_rows_per_index` and always ends on a row-group border. The mount then creates its own template, the backing indices (one shard each, with the three `index.parquet.*` settings that name the repository, the pieces and the field-to-column bindings), an alias, and migrates the alias to a data stream, and it removes what it created when a step fails. A preview runs the same analysis and creates nothing, which is why it is cheap.

### What happens when a shard starts, and what a stub segment holds

The plugin selects `ParquetEngineFactory` for every index that has `index.parquet.repository` set. When the store holds a commit without segments, the factory re-reads the footers (the only place that does, so that opening a segment later costs no request), builds one `ExternalFilePiece` per piece and one `ColumnBinding` per mapped field, and hands an `ExternalParquetCodecReader` to `IndexWriter#addIndexes` with `ParquetAttachCodec`. The reader has one document per row and no stored fields, postings, points or norms; the writer asks `ParquetDocValuesConsumer` to merge its producer, and the consumer copies the reference and reads no value. The result is a regular segment whose only data file is the `.dvm` written by `ParquetMetaFile`, with three parts:

- the bindings of fields to columns, with the Lucene doc values type that each field is served as and the conversion (for example `seconds_to_millis`) that turns file values into field values;
- the file pieces: the location (never credentials), the length of the file at attach time, the number of rows before the piece in the index, and the rows of each row group;
- a snapshot of the footer per served column: type, whether the column is `REQUIRED`, the offset, size and codec of each chunk, and the minimum, maximum and null count per row group (the format is at version 1).

Replicas get the stub through peer recovery and a restart reopens it, so attach runs once per primary. Because the stubs are attached in row order and never merge, the `_id` of a document is its row number, which is also its docID in the index reader, and `ParquetReadOnlyEngine` resolves a `get` that way instead of looking in postings that do not exist.

### How a doc values read finds its bytes

Lucene resolves the format by name through SPI when it opens a stub segment, and the format gives no hook for context, so the engine runs the open inside `ParquetReadContext.with(...)`, a thread-local that hands the segment its `ParquetObjects` (the files of the registered repositories). `ParquetMetaFile` then builds a `ParquetDocValuesProducer`, which opens the files at that moment and not at search time, because `BlobStoreRepository#blobStore()` must not be called on a search thread. A segment that is opened elsewhere, for example by a tool that does not know about these files, can be described but cannot read a value.

For each field the producer returns a `ParquetNumericDocValues` (`INT32`, `INT64`) or `ParquetBinaryDocValues` (`BYTE_ARRAY`) iterator. A document is a row, so `ColumnChunkReader` finds the row group that holds it, fetches the whole column chunk of that row group with one `readRange`, and walks the page headers from the start of the chunk because many files have no offset or column index. It skips pages that end before the target without decompressing them, decompresses the target page (`PageDecompressor`: `UNCOMPRESSED`, `SNAPPY`, `GZIP`), and a decoder (`LongPageDecoder` or `BytesPageDecoder`, with `RleBitPackedDecoder` for dictionary indices and definition levels) turns it into values. The iterators are not thread safe, so every iterator owns its reader and its buffers, and a value (a `BytesRef`) is only valid until the iterator moves on.

Alongside the values, `ParquetDocValuesSkipper` gives every integer field a one-level skipper with one interval per row group, built from the footer statistics. Range queries, sorts and `DateFieldType#isFieldWithinQuery` use it to skip row groups without reading the file. We only declare a skipper when the statistics make the value counts exact, and a group without a usable statistic gets the widest range, which is slower and never wrong.

### Where the bytes come from: repository, cache, and the threads that wait

`ParquetObject` is the one abstraction between the reader and the storage: a location, a length and a ranged read. `RepositoryParquetObject` implements it over the `BlobContainer` of a registered repository, below the base path of that repository (`RepositoryLocation` rejects any path that escapes it), so credentials, retries and client settings stay in the repository and the plugin carries no cloud SDK. The length of a file comes from one listing of its directory, because the blob API has no length call; this is why a repository must allow listing.

When `xpack.parquet.cache.size` is set, the plugin wraps every object in `CachedParquetObject`, which sends reads through `ParquetBlobCache`. That is a thin layer on `SharedBlobCacheService` of the `blob-cache` plugin with its own file (`parquet_cache`) and its own settings, so it can live next to the cache of searchable snapshots. A read asks for exactly the page-aligned range that the reader needs, and a miss fetches that range with one ranged read of the repository. Without the setting, or when the cache cannot be created (the node logs a warning), reads go straight to the repository.

A read blocks the calling thread: a search thread or an ES|QL driver thread waits inside the Lucene scorer, and cache fetches run on the generic thread pool. There is no prefetch, so a cold read costs one network round trip per chunk, and with ES|QL doc partitioning several slices of one row group each read the same chunk, which only the cache removes. That is also why a warm run over S3 is about as fast as a warm local run, and why an uncached remote scan is slow.

### Why the pieces are the way they are

| Choice | What we did | Why, and what it costs |
|---|---|---|
| Creating segments without a writer | `IndexWriter#addIndexes(CodecReader)` with a codec reader over the file | It works with the stock `IndexWriter` and costs about 14 KB and a few milliseconds per million rows; the cost is a one-time attach per primary. |
| Choosing the engine | `ReadOnlyEngine` subclass through `EnginePlugin`, format found by SPI name | No hook in the codec of Elasticsearch is needed; a mounted index is read-only by construction. |
| Reading remote bytes | The `BlobContainer` of a registered repository | Credentials and retries stay in the repository; it needs listing permission for the file length. |
| Reading Parquet | Own footer reader and page decoders, bundled `parquet-hadoop-bundle` without Hadoop, pure-Java Snappy | A node must not need ES\|QL (the `esql-datasource-parquet` plugin extends it) or Hadoop to open a segment; the cost is that we decode only what we implemented. |
| Unit of a read | The whole column chunk of one row group | Many files have no page index, and a request per page would be far too chatty; the cost is that a point lookup in a wide column reads a whole chunk. |
| The cache | Our own instance of the shared cache, with its own file and settings | It does not depend on node roles and cannot clash with searchable snapshots; the cost is a second cache file on the node. |

### What we assume, and what happens when it breaks

We assume that a Parquet file never changes after it is mounted. The length at attach time is stored in the segment and the cache key is the location plus the length, so a file that changes length is detected on some paths, but we do not check content and a same-length rewrite would serve stale or wrong values. Opening a shard makes no request to the object store, so a missing file or repository fails the read and not the open. Snapshots of a mounted index do not contain the data, and every node that holds a copy needs the plugin and access to the repository.

## How the code is laid out

The architecture section above names the classes by their job; this is how they sit in packages.

- `org.elasticsearch.xpack.parquet`: `ParquetDocValuesPlugin` only. It depends on both packages below.
- `...parquet.mount`: the mount and preview actions (`Transport*`, `Rest*`), `ParquetMountSpec` (strict body parsing), `MountPlanner`, `ParquetMappingDeriver` and `ParquetMountAnalysis`. It depends on `docvalues`.
- `...parquet.docvalues`: the format (`ParquetDocValuesFormat`, consumer, producer, meta file), the footer reader and file info, the page decoders (`ColumnChunkReader`, `LongPageDecoder`, `BytesPageDecoder`, `RleBitPackedDecoder`, `PageDecompressor`), the repository access (`RepositoryBlobReader`, `ParquetReadContext`), the skipper, and the engine. It depends on neither of the others.

We bundle our own Parquet footer reader (`parquet-hadoop-bundle`, with the Hadoop jars compile-only and never loaded) and a pure-Java Snappy (`aircompressor`) instead of extending `esql-datasource-parquet`, because that plugin extends x-pack-esql and a node must not need ES|QL to open a doc values segment. Producers open their files when the segment opens, not at search time, because `BlobStoreRepository#blobStore()` must not be called on a search thread.

Beyond this plugin the branch also touches `server` (a public `VersionsAndSeqNoResolver.DocIdAndVersion` constructor, the final index setting `index.mapping.id_field.indexed`, and the `DateFieldMapper` safeguard above) and, temporarily, `LuceneTopNSourceOperator` in ES|QL, which carries a fix for a bug that silently dropped the top docs of a long-running driver. That fix goes to `main` as its own change (see the comment at the code), and when this branch rebases onto it we keep the version of `main`.

## How to build and test

```
./gradlew :x-pack:plugin:dv-format-parquet:check
```

runs the unit tests (including the footer, decoder, skipper, mount-planning and compatibility tests, which write real Parquet files through the test helper `ParquetTestFiles`) and the integration tests in `src/internalClusterTest` (`ParquetEngineIT`, `ParquetColumnarIT`, `ParquetEsqlIT`, `ParquetMountIT`, and others). Some of these run ES|QL over a mount.

Two groups of tests are off by default:

- `ParquetClickBenchManualIT` mounts the real ClickBench file and compares all 43 ES|QL queries of the internal Rally track (and our own 37 translations) with what DuckDB computes. It needs `-Dtests.parquet.hits=<path to hits.parquet>` and a large heap, and the helper scripts, with their run commands, are in [`src/internalClusterTest/clickbench`](src/internalClusterTest/clickbench/README.md).
- `ParquetSortStressIT` (`-Dtests.parquet.stress=true`) is a synthetic stress test that compares a mount with a regular index under parallel sorted queries.

## What is not done yet

- Column types: float, double, boolean and decimal columns are unmapped, nested columns are not supported, and `DELTA_*` encodings, `ZSTD` and `LZ4` are not decoded.
- Reads: the whole column chunk is read per iterator (no page index, because ClickBench does not have one), and there is no bulk read path. A node-local disk cache of byte ranges exists (`ParquetBlobCache`, on top of `SharedBlobCacheService` of the `blob-cache` plugin). It is off unless `xpack.parquet.cache.size` is set (and `xpack.parquet.cache.region_size`, default 16 MB, can be smaller), it has its own file `parquet_cache` in the data path, so it can live next to the cache of searchable snapshots, and the node needs that much free disk space and one data path. When the cache cannot be created the node logs a warning and reads go straight to the repository.
- Skippers: one level, per row group. Per-page levels would need the column index.
- Repositories: only `fs` is tested. S3 (listing permission for the file length) needs a spike.
- Mounts: no mounting into an existing data stream, no rollover decision, `ProjectId.DEFAULT` is a TODO, and we have not designed the privileges for the mount actions yet.

## Open questions

- Should `index.mapping.id_field.indexed` stay a general setting, or should a Parquet-backed index imply it?
- How should a mount behave when a file changes in the object store after it was mounted? Today we assume immutable files and do not check.
- Do we want ordinals for high-cardinality strings (a possible later part of the plan), or is `keyword` with raw binary doc values enough for the queries we care about?
