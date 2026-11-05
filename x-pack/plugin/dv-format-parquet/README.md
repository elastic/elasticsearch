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

## How the code is laid out

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
