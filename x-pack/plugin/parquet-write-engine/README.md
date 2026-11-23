# Parquet write engine

An append-only engine that stores the documents of an index **only** as Parquet files in an object store. The files are listed in an
Apache Iceberg table, so other readers (the ES|QL data sources, Spark, DuckDB, pyiceberg) can open the same data.

This is a **prototype**. It tests whether the design works and what it costs. It is not for release builds: the engine needs batch
indexing, and that feature flag (`batch_indexing`) is off in release builds. Names and settings can change with no compatibility promise.

The plugin also reads. The read code is copied from the reader prototype (#165092, `parquet_doc_value_format_2`): stub segments that
reference Parquet files, and a Lucene doc values format that reads them through the ES|QL Parquet reader. It serves three kinds of index:
the search replicas of an index of this format (see "Search replicas"), and indices that the mount API creates over Parquet files or over
an Iceberg table that someone else wrote (see "Mounting files and tables").

## What an index of this format does

- Every shard bulk is written to a local Parquet file before the bulk returns. With the default `index.parquet.durability: staged` a
  copy of the file is also in the repository, below `staging/`, before the bulk is acknowledged (see "Durability: when a bulk is
  acknowledged"). With `local` the acknowledgement point is the local file only.
- A flush merges the complete local files, uploads the result to a snapshot repository (`s3`, or `fs` for tests), and appends it to the
  Iceberg table of the index.
- After an upload, a background compaction merges small uploaded files into larger ones with one atomic Iceberg commit.
- The primary cannot be searched, and no read ever reaches it. Its **search replicas** follow the Iceberg table and serve every uploaded
  row: search, aggregations, ES|QL and `GET`. A row is searchable after the flush that uploads it and the refresh that follows.

## A short example

```
PUT /_snapshot/lake
{ "type": "fs", "settings": { "location": "lake" } }          // below path.repo

PUT /logs-parquet
{
  "settings": {
    "index.storage_format": "parquet",
    "index.mode": "columnar",
    "index.parquet.repository": "lake"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "@timestamp": { "type": "date" },
      "host.name":  { "type": "keyword" },
      "bytes":      { "type": "long" }
    }
  }
}

POST /logs-parquet/_bulk          // auto-generated ids only, index or create items only
POST /logs-parquet/_flush         // uploads the local files and lists them in the table
POST /logs-parquet/_refresh       // the search replicas read the new version of the table

PUT /logs-parquet/_settings
{ "index.number_of_replicas": 1 } // a search replica; without one, a search finds no shard
```

After the flush, `<repository>/logs-parquet/<index uuid>/` holds the table: `metadata/v<N>.metadata.json`,
`metadata/version-hint.text`, the Avro manifests, and `data/shard-<n>/<history uuid>/<from seq no>-<to seq no>-<attempt>.parquet`.

## Rules of the format

The settings provider checks these when the index is created and refuses the index with a message that names the problem.

- `index.mode` must be `columnar`. No field is indexed: every field is doc values only. `index.mapping.source.mode` must be `synthetic`.
- Replicas are **search replicas** (see "Search replicas"). The provider sets `number_of_replicas` to 0 when the request does not name it,
  and refuses `auto_expand_replicas` at creation; a later settings update can set either. `index.mapping.id_field.indexed` is `false`:
  a search replica synthesizes the `_id`, which has no postings.
- `index.parquet.repository` is required, and the repository must keep conditional writes (see "The table commit" below).
- The supported field types are `long`, `integer`, `short`, `byte`, `date`, `keyword`, `double`, `float` and `boolean`, and objects of them
  (a dotted name is one flat column). A field is single-valued, a flat column, unless its mapping says `"multi_value": true`, and then it is a
  list (see "Multi-valued fields"). Other types, multi-fields, `nested`, `index: true` and
  `doc_values: false` are refused. `date_nanos` is refused because Iceberg tables of the format version 2 have no nanosecond timestamp.
- Only `index` and `create` items with auto-generated ids are written. An explicit `_id`, an update and a delete are failures of that item,
  and the other items of the bulk are written.
- Use `dynamic: strict`: a document with a field that the mapping lacks fails the whole shard bulk (see "Limits of the batch path").
- A retried bulk can write its documents again, so delivery is at least once. There is no `_id` lookup to deduplicate.

## Limits of the batch path

The engine accepts only batches: it implements `Engine#indexBatch`, and every other kind of write is refused. A bulk reaches `indexBatch` only
when the server can map all of its documents in columns, which is called the batch path. First the server parses the bulk into source columns
by the JSON shape of the values (`LONG`, `DOUBLE`, `BOOL`, `STRING`, `BINARY`, `ARRAY` of one primitive kind, and `UNION` for anything mixed,
including an explicit `null`). Then each field mapper converts its column in `doMapColumnBatch`, and a mapper throws
`UnsupportedOperationException` ("ESCF column kind [X] is not yet supported ...") for a kind that it does not implement. The server then maps
the bulk one document at a time instead, and this engine refuses that path, so **every document of the shard bulk fails** with the message
"the Parquet write engine accepts only batches of documents with auto-generated ids", which does not name the cause.

These limits are the ones of the batch mappers in `server`, which are still in development. They are not limits of Parquet or of our writer,
and a fix belongs in the mappers. Each row below takes the whole shard bulk off the batch path:

| In the source of a document | Fails the shard bulk |
|---|---|
| a field that the mapping lacks | yes, so use `dynamic: strict` and map every field |
| an explicit `null` or an empty array `[]` for a field of type `long`, `integer`, `double`, `boolean` or `date` | yes, even when other documents of the bulk have values for it (probed for these five types; `short`, `byte` and `float` use the same mapper as `long`, `integer` and `double`). Leave the field out of the document instead. |
| an explicit `null` or `[]` for a `keyword` | no |
| an array of `boolean` or of `date` | yes, because the date and boolean mappers handle only scalar column kinds. The format refuses such a field at index creation. |
| a numeric array with a `null` element, such as `[1, null, 2]` | yes |
| a numeric list field that is a scalar in one document and an array in another document of the same bulk | yes, because the column is mixed (a `keyword` list can mix them) |

A missing field is always fine. Real data often has JSON `null` for absent values, so the client or an ingest pipeline has to drop null
fields of the types above before the bulk (not tested here).

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `index.storage_format` | `native` | `parquet` selects this engine. Final. |
| `index.parquet.repository` | none | The snapshot repository that holds the files. Final, required. |
| `index.parquet.base_path` | `<index name>/<index uuid>` | The path of the table below the base path of the repository. Final. |
| `index.parquet.durability` | `staged` | `staged`: a bulk is acknowledged after a copy of its file is in the repository. `local`: after the local file only. Final. See "Durability: when a bulk is acknowledged". |
| `index.parquet.catalog.enabled` | `true` | Every commit of the table creates or updates its entry in the catalog of the repository (see "The catalog"). Final. |
| `index.parquet.catalog.namespace` | `default` | The namespace of the table in the catalog. A dotted value is a nested namespace (`prod.eu` is the directory `prod/eu`). Final. |
| `index.parquet.compression` | `zstd` | Compression of uploaded files (`uncompressed`, `snappy`, `zstd`). |
| `index.parquet.local_compression` | `zstd` | Compression of the local files that every bulk writes (same values). On ClickBench data, `zstd` made them 2.3 times smaller than uncompressed ones, about the size of the uploaded file, at no throughput cost that a measurement could see. Dynamic. |
| `index.parquet.flush_threshold_size` | `1gb` | Local bytes at which the periodic flush uploads. |
| `index.parquet.flush_threshold_age` | `30m` | Age of the oldest local file at which the periodic flush uploads. |
| `index.parquet.max_local_size` | `4gb` | Local bytes at which bulks get a 429 until a flush frees space. |
| `index.parquet.max_file_size` | `1gb` | The most **local bytes** that one uploaded file is merged from. With `zstd` local files (the default) an uploaded file is about as large as its local files; with `uncompressed` local files it is about 0.4 times as large. A compaction merges at most this many bytes of uploaded files. |
| `index.parquet.row_group_size` | `128mb` | Buffered data at which a row group is written. |
| `index.parquet.compaction.enabled` | `true` | Merge small uploaded files in the background. |
| `index.parquet.compaction.target_file_size` | `1gb` | A file below half of it is small and is merged with its neighbours. |
| `index.parquet.compaction.min_files` | `2` | The fewest adjacent small files that a compaction merges. |
| `index.parquet.compaction.delete_delay` | `1h` | How long replaced files stay for readers of an older snapshot. |
| `index.parquet.compaction.interval` | `5m` | How often a shard looks for files to compact when no flush asks for it (after a restart, or after a dropped compaction). Not dynamic. |
| `index.refresh_interval` | `10s` | The setting of the server, with another default (set at creation when the request has none). Each scheduled refresh of a search replica reads the table, which costs a listing and a few reads of the repository, so the server's default of `1s` would mostly cost object store requests: the primary tells its replicas of each upload anyway (see "Search replicas"). `-1` turns the scheduled reads off. |

A flush also runs when the shard is idle. The thresholds are in bytes and not in rows, because the size of a row is very different from
one data set to the next.

## How it works

**Write path.** The engine implements `Engine#indexBatch`. It reads each mapped column of the batch through the typed cursors of Lucene's
column API (no Lucene field is built) and writes one local file for each batch (compressed with `index.parquet.local_compression`), named
`pqlocal_<from>_<to>.parquet` by the range of sequence numbers. The shard still keeps one empty Lucene commit and an empty translog, because the server needs both before it calls the
engine. The commit also holds the state of the engine in custom keys (`parquet.pending_upload`, `parquet.uploaded_through_seq_no`,
`parquet.uploaded_rows`); the sequence numbers of the server in it are never rewritten.

**Flush.** A flush runs on a thread pool of its own (`parquet_flush`, and `parquet_compaction` for the compaction; each has at most half the
processors of the node). It must not run on `generic`: closing an index runs on `generic` and blocks on the flush, so with the flush on the same
pool a node that closes many shards at once could run out of threads (and a node with assertions refuses it). The flush asks a `generic`
thread once for the blob store of the repository, which only some pools may use, and keeps it. A flush does these steps, and a crash after
any of them loses no row:

1. merge the complete local files into files with row groups of the configured size and the field ids of the table. The local files are
   split into runs of equal size, and each run is at most `max_file_size` local bytes, so the split does not leave a tiny file behind;
2. write the pending marker in the commit data;
3. upload the file under its final key (no reader looks at it yet);
4. append it to the Iceberg table, which is the moment readers see the rows;
5. write the high-water mark and the row count in the commit data, and remove the marker;
6. delete the local files.

The merge copies the values from the column readers of the local files to the column writers of the merged file, with their repetition
and definition levels, so no row is ever assembled (the merge before it built a parquet-mr `Group` for each row). A column has the same
type and so the same levels in every file, and a column that an earlier file lacks becomes nulls. On 600,000 rows of 105 columns like
ClickBench's (68 integers, 10 longs, 27 keywords) with ZSTD, one thread on a laptop, the merge took 6.2 s where the row-wise merge took
11.2 s, and the files had the same size. The rest of the time is parquet-mr itself: the dictionaries, the min/max statistics of the
binary columns, and the compression (about 14% of the samples). A merge of the columns on several threads is not built.

The local file of a bulk is written the same way, column by column: each mapped column is read from its cursor of Lucene's column
batch and written to the column writer of parquet-mr, with no `Group` for each row. A bulk is one row group with one page for each
column (the server hands the engine at most 5,000 documents at a time, and the merge of the flush makes the row groups that readers
want). In a test node with one shard, four clients, local durability and 105 fields like ClickBench's, bulk indexing went from about
88,000 to about 180,000 documents per second with this change (a laptop, the time of parsing and of the client included).

On start, the engine finishes a flush that a crash cut off. The table, and not the commit, is the source of truth for what is uploaded.

**Every file has a column for every mapped field.** A batch has a column only for the fields that one of its documents has a value for,
but the schema of a file comes from the mapping: a field with no value in the batch becomes a column of nulls. The reader prototype needs
every file of a table to have every column of the others, and the schema of a file should not depend on the fields of one bulk. A field
that a mapping update adds later is in the files after the update, and a merge writes the union of its inputs.

**The table commit.** All shards of an index append to one table. A commit writes `v<N+1>.metadata.json` with a put-if-absent
(`If-None-Match: *` on S3), and Iceberg retries a commit that lost the race. **The repository must enforce conditional writes.** On a
store that ignores the header, two shards that commit at the same moment both write the same version, the later one replaces the
earlier one, and the flush still reports success. The `fs` repository and AWS S3 enforce it.

**Compaction.** After a flush that uploaded files, one compaction of the shard runs at a time. It chooses runs of *adjacent* small files
(the order of the rows of a shard never changes), merges them reading the inputs with ranged reads, uploads the result, and commits one
Iceberg rewrite that names the snapshot it planned from. It then expires snapshots older than `delete_delay`, which deletes the replaced
files, and deletes files that no snapshot lists (the output of a compaction that crashed after its upload). A file that was replaced
stays readable for the delay.

## Multi-valued fields

A field whose mapping says `"multi_value": true` (or every field, when `index.mapping.doc_values.multi_value` is `true`; the format sets it to
`false` unless the request does) is a Parquet `LIST`, the standard three-level layout that every reader understands:

```
optional group tags (LIST) { repeated group list { optional binary element (STRING); } }
```

The element has the type that the flat column of the field would have, and the Iceberg column is `list<element>`. The list has the values
of the document **in the order of the source and with the duplicates**, which is more than the doc values of a native index keep. A scalar
for a list field is a list of one element. A keyword array can have `null` elements and they stay null elements. A document that has no
value, a `null` and `[]` are the same for the batch path, and all of them are a null list. A field is a list in every file of the index or in
none, because the mapping never changes.

The batch path decides what can be a list (see "Limits of the batch path"):

- The types that can be lists are `keyword`, `long`, `integer`, `short`, `byte`, `double` and `float`. An array of `boolean` or of `date`
  cannot be mapped in a batch, so the format refuses such a field at index creation.
- A numeric array cannot have `null` elements, and a numeric list field must have arrays in every document of a bulk if it has an array in one
  (a `keyword` list can mix scalars and arrays).
- A single-valued field with an array in a document is an error for that document, as in any columnar index.

## The catalog

Every commit of the table (its creation, a schema update, the append of a flush, the rewrite of a compaction, the expiry of
snapshots) also creates or updates a small JSON file, the entry of the table, at `<base path of the repository>/_catalog/<namespace>/<index
name>.json`. Index names cannot start with `_`, so the directory never clashes with a table path (`<index name>/<index uuid>`). The entry
maps a stable name to the current location of the table, so a reader finds the table by name and not by a path that has the uuid of the
index in it, and an index that is deleted and created again with the same name moves the entry to the new table (the old files stay).

```json
{ "format": "elasticsearch-file-catalog", "format_version": 1, "namespace": ["default"], "name": "logs-parquet",
  "table_uuid": "...", "location": "s3://bucket/base/logs-parquet/<index uuid>",
  "metadata_location": "s3://bucket/base/logs-parquet/<index uuid>/metadata/v7.metadata.json",
  "previous_metadata_location": ".../metadata/v6.metadata.json", "metadata_version": 7,
  "created_at_ms": 1763200000000, "updated_at_ms": 1763203600000,
  "elasticsearch": { "index": "logs-parquet", "index_uuid": "...", "cluster_uuid": "..." } }
```

The entry is a pointer and not the commit: the commit of the table is still the put-if-absent of `v<N+1>.metadata.json`, and the entry is written
after it, like `version-hint.text`. Two shards that commit at the same moment can leave an entry that is one version behind, and the next
commit repairs it; an entry is never moved back to an older version. So a reader that wants the newest table reads the entry, and then
follows `v<N+1>.metadata.json` while that file exists (`RepositoryCatalog#load` does this). A failure to write the entry is logged and does
not fail the commit or the flush. The format is ours and not an Iceberg standard: pyiceberg opens a table with
`StaticTable.from_metadata(<metadata_location>)` after it reads the entry.

## Search replicas

The copies of an index of this format have separate roles (`ParquetShardRoutingRoleStrategy`, a `ClusterPlugin` hook): the primary is
`INDEX_ONLY` and every replica is `SEARCH_ONLY`. **No read ever reaches the primary.** Every read action chooses the copies of a shard
through `ShardRouting#isSearchable()`, which is false for `INDEX_ONLY`, whatever its `preference`: search, count, msearch, PIT,
field_caps, validate_query, ES|QL (and its lookups), get, mget, explain and term vectors. A preference that names only the primary's node
finds no copy (`NoShardAvailableActionException`); without a search replica, a search finds none. As a second layer the write engine still
refuses `acquireSearcher` and `get`.

A search replica never receives an operation (replication skips unpromotable copies) and has no local state that matters, so it is
allocated, moved and rebalanced like any shard. Its engine, `ParquetTableFollowEngine`, follows the table:

- Its store holds one stub segment for each data file of its shard (`data/shard-<n>/`, of every history of the shard, ordered by history
  and sequence number). The commit of the store records the files that the stubs reference. A read of the table that finds only new files
  appends their stubs, and the readers of the other segments stay; one that finds a file gone (a compaction) or a change of the mapping
  builds all the stubs again.
- **The primary tells its replicas of each upload.** When a flush uploads files, or a compaction replaces them, the primary sends its
  search replicas the refresh of the unpromotable copies (`TransportUnpromotableShardRefreshAction`, the request that `_refresh` sends)
  with the new version of the table (the `N` of `v<N>.metadata.json`) as the generation. Each replica reads the table until it shows that
  version or a later one. So an upload becomes searchable without a refresh of the index, as a new commit does in stateless. A plugin
  cannot send that request itself, so a local action of the plugin (`indices:admin/refresh/parquet_table`) sends it, as the internal user
  of the `post_write_refresh` origin, which may run `indices:admin/refresh*`. The announcement is best effort: a failure is logged at
  debug and costs only time.
- **A refresh reads the table.** The primary answers `_refresh` with the version of the table that holds its uploads, and the server
  forwards it to the replicas in the same way, so `_flush` then `_refresh` returns once the uploaded rows are searchable, and
  `refresh=wait_for` uploads and waits as well (see below). The **scheduled refresh** (`index.refresh_interval`, `10s` by default for this
  format) reads the table on every replica too; for a writer index it is the fallback for an announcement that was lost. Search-idle does
  not save these reads: a replica is never active from indexing, and an explicit refresh interval turns search-idle off.
- A replica reads only files of the table, so the rows in the local files of the primary are never visible.
- A field binds to the column of the same name. A file that was written before a mapping update added a field has no value for it. A list
  field (`multi_value`) is not served yet: it has no flat column, so it has no value on the replicas.
- The `_id` of a document is its row number on the replica, as in the reader prototype: `GET /index/_doc/<row>`, `mget` and `ids`
  queries look the row up. The `_id` that the bulk returned is not kept (the files have no `_id` column), and a compaction or a file of
  another history can change the row number of a document.
- A replaced file stays for `index.parquet.compaction.delete_delay`; a replica must read the new version within that delay, or its
  searches fail on the deleted file until the next refresh.

The server needed two changes for this, both small: `ShardRoutingRoleStrategy` gets methods that know the index (with defaults that keep
every other strategy as it is), and `PeerRecoveryTargetService` recovers an unpromotable copy of a stateful cluster locally (an empty
commit, then the engine) instead of sending the stateless relocation action. A node has one role strategy, so this plugin is
`STATEFUL_ONLY`.

## Mounting files and tables

The mount API of the reader prototype creates read-only indices whose segments are stubs over Parquet files:

```
POST /_parquet/_preview           // what a mount would do: the files, the mapping, the unmapped columns
POST /_parquet/_mount
{ "repository": "lake", "paths": ["events/part-0001.parquet"], "index": "events" }
```

A mount can also follow an **Iceberg table**, for example one that Spark or pyiceberg wrote, or the table of a deleted index of this
format:

```
POST /_parquet/_mount
{ "repository": "lake", "table": "warehouse/events", "index": "events",
  "fields": { "n": { "name": "number" } },
  "index_settings": { "index.refresh_interval": "1m" } }
```

- `table` is the path of the table below the base path of the repository. The mapping comes from the footer of the newest data file of the
  current snapshot, with the overrides of the request (`fields`, `exclude`, `timestamp_field`), so a table needs a data file to be mounted.
- The index records the table (`index.parquet.table.path`) and its fields, not its files, and each copy follows the table with
  `ParquetTableFollowEngine`: a refresh reads the current snapshot. No writer tells the index of its commits, so the scheduled refresh
  is how it finds them: every `index.refresh_interval`, which a mount sets to `10s` unless `index_settings` names it. A file belongs to the shard that the hash of its location chooses, and
  the files of a shard are ordered by the sequence number of the commit that added them, so an append adds stubs.
- The table is found without a catalog, by listing `metadata/`. Both namings of the metadata files work: `v<N>.metadata.json` (the
  tables of this format, and Hadoop tables) and `<NNNNN>-<uuid>.metadata.json` (the tables that pyiceberg, Spark or any other catalog
  writes, whose version is then the number plus one). The highest number is the current version, so a metadata file that a failed
  catalog commit left behind with a higher number would be read; and a commit to a table whose files a catalog named is refused.
- A snapshot with delete files is refused (the shard keeps the files that it read before). Columns are bound by name, not by Iceberg field
  id, so a renamed column is not followed. Data source tables (`s3://` without a repository) are not supported yet.

The types that a mount maps are `long`, `integer`, `short`, `byte`, `date`, `date_nanos`, `keyword`, `double`, `float` and `boolean`. A
`DOUBLE` or `FLOAT` column is encoded as the sortable long or int of its field type, and the column decides the field type; a `BOOLEAN`
column is served as 0 and 1. Nested and repeated columns are unmapped.

## Reading the data outside Elasticsearch

- The table is plain Iceberg (format version 2), so pyiceberg and pyarrow read it. Find it by name through the entry of the catalog (see
  "The catalog") or by its path. A unit test (`IcebergFeasibilityTests`, run with
  `-Dtests.iceberg.export.dir=<dir>`) exports a small table that you can open with them.
- The ES|QL Iceberg data source cannot read a table by its prefix yet: it sends a HEAD request for the table path first, which is always a
  404 for a prefix, and the Iceberg QA suite of the repository is disabled. `EXTERNAL "s3://bucket/<table>/data/*/*/*.parquet"` reads the
  data files with the Parquet reader, but not through the manifest.
- The Parquet files use `TIMESTAMP(MILLIS)` for `date`. Iceberg calls such a column `timestamptz`, which the spec defines in microseconds,
  and the bounds in the manifest are in microseconds. A strict reader may want `MICROS`; this is not tested.

## Layout of the code

| Package | Contents |
|---|---|
| `org.elasticsearch.xpack.parquet.write` | the plugin, the settings, the settings provider, the mapping validator and the allocation decider |
| `.engine` | `ParquetWriteEngine`, the shard tables, the commit data, the handling of linkage errors |
| `.file` | the local files, the batch writer, the merger, the codecs, input files for a directory and for a blob |
| `.flush` | the flusher and its crash protocol |
| `.staging` | the adoption of the staged files of lost stores |
| `.compaction` | the compactor and its policy |
| `.table` | the Iceberg table of a shard, its `FileIO` and `TableOperations` over a blob store, the metrics of a file, the table location, and `TableReader`, which reads a table without writing it |
| `org.elasticsearch.xpack.parquet.read.dv` | (copied from the reader) the doc values format of the stubs, its producer, windows, skippers and page cache |
| `org.elasticsearch.xpack.parquet.read.engine` | the read-only engine of mounted files (copied), `ParquetEngineFactory`, and `ParquetTableFollowEngine`, which follows a table |
| `org.elasticsearch.xpack.parquet.read.mount` | (copied) the preview and mount actions, with the mount of a table added |
| `org.elasticsearch.xpack.parquet.read.repository` | (copied) the locations of files in repositories and data sources, and the node-local disk cache |

The plugin extends `x-pack-esql`, `esql-datasource-parquet` (the Parquet reader and `parquet-hadoop-bundle`) and `blob-cache`, and bundles
`iceberg-core` itself. It cannot extend `esql-datasource-iceberg` as well: that plugin carries its own copy of `parquet-hadoop-bundle`, and a
node refuses a plugin that sees two copies. Iceberg's static worker pool needs the `manage_threads` entitlement in this plugin.

## Tests

```
./gradlew :x-pack:plugin:parquet-write-engine:test                  # unit tests (crash tests of the flush and the compaction)
./gradlew :x-pack:plugin:parquet-write-engine:internalClusterTest   # cluster tests with an fs repository
./gradlew :x-pack:plugin:parquet-write-engine:qa:javaRestTest       # a real node, a real S3 repository, an S3 fixture
./gradlew :x-pack:plugin:parquet-write-engine:precommit
```

Notes for test authors:

- A cluster test needs `addMockInternalEngine()` to return false (an index can have only one engine) and `beforeIndexDeletion()` to do
  nothing (the base class searches every shard). Use `numberOfReplicas()` of 0 or a cluster with one data node.
- The default durability is `staged`, so the repository must exist when the first bulk arrives, and the table is created by that bulk (and
  by the adoption when the engine starts). A test that bulks before it registers a repository, or that counts on "no table until the first
  flush", sets `index.parquet.durability: local`. The other cluster tests pick one of the two at random. A test that wipes the data of a
  node (`ParquetNodeLossIT`) needs a dedicated master, which keeps the cluster state.
- A node with assertions on asserts that it knows the extension of every file in the store. Set `es.allow_unknown_lucene_file_extensions`
  to true (the QA project does). A node without assertions does not need it.
- The S3 fixture of the ES|QL tests ignores conditional writes. The QA project uses its own fixture with the AWS consistency model.
- Iceberg keeps a static pool of threads (`iceberg-worker-pool-*`). Filter it in the thread leak check.
- A cluster test that hangs waits 20 minutes whatever `-Dtests.timeoutSuite` says. Take a thread dump instead of waiting.

## Running a node by hand

`./gradlew run` is fixed to port 9200 and does not take `http.port`. The plugin and the Iceberg plugin are already modules of the built
distribution, so installing them again fails at boot with a duplicate key. What works:

1. Build the distribution (`./gradlew :distribution:archives:darwin-aarch64-tar:assemble`, or run the QA project once), and copy
   `distribution/archives/*/build/install/elasticsearch-*` to a private directory.
2. After a change of the plugin, build the jar (`./gradlew :x-pack:plugin:parquet-write-engine:jar`) and copy it from
   `build/distributions` to `modules/parquet-write-engine/` in the copy.
3. Start the copy with `-E path.repo=<dir>`, `-E xpack.security.enabled=false`, `-E http.port=<free port>`, and a heap that fits the
   shards you test.
4. A disk that is nearly full keeps the shard unassigned (the high watermark), and the bulks then fail with `unavailable_shards_exception`.
   Use a node setting or `cluster.routing.allocation.disk.threshold_enabled: false` for a benchmark.

## Durability: when a bulk is acknowledged

The engine has no translog, and its replicas are search replicas that only read the table, so the file of a bulk is the only copy of its
rows until a flush has uploaded them. The setting
`index.parquet.durability` (final) says where that copy is when the bulk is acknowledged.

- `local`: the local file is durable (written and renamed). The loss of the node or of its disk loses every acknowledged row that no flush
  has uploaded yet, which is up to `flush_threshold_size` or `flush_threshold_age` of data.
- `staged` (the default): after the local file, the same bytes are uploaded to
  `<table>/staging/shard-<n>/<history uuid>/<from>-<to>-<attempt>.parquet`, and only then is the bulk acknowledged. This is the role of the
  translog in stateless Elasticsearch. The key has an attempt and the upload is a put-if-absent, as for data files. `staging/` is outside
  `data/`, so readers and Iceberg never see it.

What a bulk does with `staged`: it opens the table before it writes anything, so a repository that is missing or a table that another
index owns fails the bulk (HTTP 503) with no local file and no gap in the sequence numbers. It then takes its sequence numbers, writes
the local file, and uploads the staged copy. None of these steps holds a shard-wide lock, so the local files and the uploads of
concurrent bulks of a shard overlap (a profile of a run with one shard and 8 clients showed the bulks waiting for each other on a lock
that was held during the write of the local file). If the write or the upload fails, the local file is deleted, the bulk fails (a 503 for
the upload), and its sequence numbers stay used (a gap, which the engine tolerates because the ranges are in the names and the rows come
from the footers). A bulk that is rejected with a 429 takes no sequence numbers.

Because bulks finish in any order, a flush uploads only the local files whose range is at or below the processed checkpoint of the
shard, which means that every bulk with a lower range has finished (a failed bulk marks its range as processed too). A flush records the
highest range that it uploaded and deletes every local file below it as a leftover, so a file of a higher range must never be uploaded
while a lower range is still being written. The limit `index.parquet.max_local_size` is checked before a bulk writes, so bulks that are
writing at that moment can take the local size a little past it. The client may retry. A flush that picked the local file up before
the failure has put the rows in the table anyway, which is the at-least-once behaviour that a lost response also gives.

A flush deletes the staged copies of the rows that it has put in the table: after the local files, and at its start for any copy at or
below the high-water mark (a leftover of a crash, or of an upload that finished after the flush). A failed delete is only logged.

**After the loss of a node and its disk** the shard has no copy: the primary stays unassigned. A human runs `POST _cluster/reroute` with
`allocate_empty_primary` and `accept_data_loss: true` (the store is lost, the rows are not), and the shard starts with a new history. The
engine then **adopts** the staged files of every other history of the shard (`StagedFileAdopter`): for each old history it deletes the staged
files at or below that history's high-water mark in the table, merges the other staged files into files of the table (reading the staged
blobs with ranged reads, as the compactor reads data files), uploads them below `data/shard-<n>/<old history>/` and appends them, and then
deletes the staged files. The rows keep their old history and sequence numbers. The table is the record of what was adopted, so the steps
are idempotent: a crash after the append leaves staged files below the new mark, which the next run deletes, and a crash before it
merges the run again. It runs once after the engine starts, on the flush pool, and the timer of the compaction retries it until it has
succeeded (the repository can be down at the start).

The cost: every bulk waits for one object-store PUT, which is typically tens of milliseconds on S3, and the write availability depends on
the object store. One small object is created for each shard bulk and deleted by a flush. Not measured on S3, only on an `fs` repository
(see the findings of the plan), which has no latency.

## Shard histories and the names of data files

A key of a data file is written once. It has the history UUID of the shard and an attempt, which is new for every upload try, and the
upload is a put-if-absent (`failIfAlreadyExists`):

- A shard that starts with a new store (an empty primary after the loss of a node, or a restore) has a new history UUID and starts again
  at sequence number 0, while the table still lists the files of the earlier history. The two histories have different keys, so a new
  upload never replaces a listed file.
- A `ShardTable` looks at, compacts and deletes only the files of its own history: the high-water mark, the recovery of a pending flush,
  the compaction and the cleanup of orphans all ignore files of other histories. Recovery asks whether the exact key of the pending file
  is listed, so a file of another history with the same range is never taken for its own upload.
- A pending flush that was not listed is uploaded again under a new attempt, and the blob of the first try becomes an orphan that the
  cleanup of the compactor deletes. This also means that a blob that a crash left partial is never read as a data file.
- The files of an earlier history stay in the table and readers see them. Nothing compacts or deletes them yet, and `parquet.uploaded_rows`
  of a new history counts only its own rows.
- A table belongs to one index. It records `elasticsearch.index_uuid` when it is created, and a shard of another index (two indices with
  the same `index.parquet.base_path`) refuses to open it: its flush fails with a message that names both indices, and its rows stay in the
  local files, with backpressure, until someone fixes the path. Index creation cannot check this, because the table may not exist yet.

## Placement: the shard is pinned

Peer recovery copies only the Lucene commit of a shard, which has no segments, so the local Parquet files (`pqlocal_*`) do not move with a
shard. They hold rows that are acknowledged and not uploaded yet, so a relocated primary would start with an empty store and lose them
without an error. `ParquetAllocationDecider` (a `ClusterPlugin` hook) therefore pins the primary of an index with
`index.storage_format: parquet` to its node. Its search replicas read the table and have no local state, so the decider lets them go
anywhere; a replica with another role (which the role strategy never creates) is refused. It leaves every other index alone.

- `canAllocate` says no to a started primary that asks for another node. That covers the balancer, the desired balance and the `move`
  reroute command. An unassigned primary stays allocatable: a new index needs a node, and after a restart the gateway allocator picks the
  node that holds the copy. It says yes to a search replica on any node.
- `canRebalance` says no for the primary, so the balancer does not try, and yes for a search replica. `shouldAutoExpandToNode` says yes:
  the replicas that auto-expansion adds are search replicas.
- `canForceAllocateDuringReplace` says no. Its default is yes, and it overrides every other no when a node is being replaced.

What a user sees: a primary whose node has to go (a shutdown of type remove or replace, an allocation filter, a disk watermark) cannot
leave. The allocation explain output says that it cannot remain and has no target, and the decider names the reason; a node shutdown stays
stalled. A node that dies still loses its local rows with `local` durability (with `staged` they are adopted, see "Durability"). A real handoff (flush and block writes before the move, or ship the local files as recovery files) is future work.

## Known limits and open questions

- Not secured or reviewed for security. A user who can create an index with this format chooses `index.parquet.repository` and
  `index.parquet.base_path`, and the engine writes below that path in the repository with the credentials of the repository. It does not check
  who may use a repository, and it does not check the segments of `base_path`, so a path can point into the table of another index or into
  other data of the repository. A table that another index owns is refused (see "Shard histories and the names of data files"), but a path
  that points into other data of the repository is not.
- Whether the repository enforces conditional writes is not checked. On a store that ignores `If-None-Match` the commits of two shards can
  replace each other without an error (see "The table commit"). The `fs` repository and AWS S3 enforce it.
- The write path is tested on one data node; the search replicas on two and three. Writers that commit at the same moment are tested in one JVM (6 concurrent writers in `IcebergFeasibilityTests`, 2 to
  4 shards that compact together in `ParquetCompactorTests`, 2 shards on one node in the cluster tests). The protocol does not depend on where
  the shards run, but a cluster with several data nodes is not tested.
- Not tested: data streams, ILM and rollover, index sorting, shrink, split and clone, reindex into such an index, cross-cluster replication.
- All shards of a node share two small thread pools (`parquet_flush` and `parquet_compaction`, at most half the processors each), so many
  shards that flush at once queue for them.
- A clean shutdown does not upload. The local files stay on the disk, and the shard finds them when it starts again. With `local`
  durability the rows are not safe against the loss of the disk before their flush; with `staged` they are in the staging files.
- No relocation of the primary. It cannot move (the allocation decider refuses it, see "Placement: the shard is pinned"), a node shutdown of
  type remove or replace stalls on such a primary, and the loss of a node loses the rows that are not uploaded yet with `local` durability
  (with `staged` they are adopted, see "Durability"; a search replica cannot be promoted). Search replicas move freely. Snapshots of such
  an index copy only the empty commit, and a restore gives a new history (see "Shard histories and the names of
  data files").
- A lossless handoff of the primary is not built. Without it, the decider pins the shard (see "Placement: the shard is pinned"). To let
  a primary move, the old node must upload every local row after it blocks writes and before it hands off to the new node. The server gives
  no place for that today: `IndexShard#relocated` blocks the write permits and hands off, and it calls no engine method and no
  `IndexEventListener`. A flush in `acquireSafeIndexCommit` (before phase 1 of the recovery) is not enough, because bulks keep writing
  after it, and their `pqlocal_*` files would not move. The cleaner fix is a small hook in `server`: an `Engine` method that `IndexShard#relocated`
  calls under the blocked permits. Our engine would flush there, and a failed flush (for example a repository that is down) would abort the
  relocation, so the shard stays. Fencing writes in the engine instead is not an option: no callback tells the engine that a recovery was
  aborted, so the fence could stay on and reject writes for ever. Peer recovery has also never run with this engine. The target would start with
  its checkpoint tracker at the uploaded high-water mark, and the handoff asserts things about the checkpoint of the target, so we expect
  some work there. Then a decider opt-in (an index setting) would let the primary move.
- An opt-in flush before a shutdown of type remove, replace or sigterm is not built, so a node cannot leave without a loss of its local
  rows while the shard is pinned. It needs the handoff above, because the node can leave without a loss only if the shard can move after the
  flush. The alternative that needs no handoff is manual: flush, then let the shard become unassigned and allocate an empty primary with
  `accept_data_loss`. The rows are safe in the table, and the new primary gets a new history (see "Shard histories and the names of data
  files"), but the index is red until then.
- Staged durability, not covered: a PUT that succeeded and whose response was lost makes a bulk fail although its staged copy exists, and
  adoption later puts those rows in the table (at-least-once, like a retry). A **zombie** primary (a node that is only cut off from the
  cluster) can keep flushing its history while a new primary adopts the staged files of the same history: nothing fences it, and the table
  can list overlapping files. `_stats` does not count adopted rows (they belong to an old history). Orphan blobs that an adoption crashed
  on stay, because the writer of that history may be alive. The shard stays red after a node loss until someone runs
  `allocate_empty_primary`; automatic allocation (an `ExistingShardsAllocator`, as stateless has) is not built. A node shutdown of type
  remove or replace still stalls on the pinned primary. Staged copies of the same history are not used to rebuild missing local files, so
  they do not give a lossless relocation yet. The latency of the PUT is not measured on S3, only on an `fs` repository.
- The catalog entry is a file of our own format, with no compare-and-set (it can lag by a version, see "The catalog"). Spark, Trino and DuckDB
  need a shim to use it. An implementation of Iceberg's `Catalog` interface over the entries, the Hadoop-catalog layout, and a REST catalog
  client are not built.
- A compaction that crashes inside the Parquet or Iceberg libraries with a `LinkageError` is logged, and has no test (the flush has one).
- A float or double column has no minimum and maximum in the footer (parquet-mr 1.18.1 with our writer settings); not investigated.
- Whether `index.storage_format` is the final name, and when its definition moves from the plugin to `server`.
- Whether to register the `parquet` extension in `LuceneFilesExtensions` and not rely on the test property.
- Whether to delete the files of an index when the index is deleted (a data lake keeps them today).
- Search replicas: list fields are not served; `_segments` and the segment stats of a replica are empty; a skip index of a numeric field
  must be the same in every file of a shard (a file whose footer lacks the statistics of a column that the others have would fail to
  attach), not tested; the memory limits of the reader prototype apply (no circuit breaker in the iterators).
- Mounts of tables: the table mount test runs on one data node. On two nodes a preview right after the repository was registered did not
  find the repository on the second node (`RepositoryMissingException`); not investigated.
