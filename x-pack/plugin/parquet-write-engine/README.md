# Parquet write engine

An append-only engine that stores the documents of an index **only** as Parquet files in an object store. The files are listed in an
Apache Iceberg table, so other readers (the ES|QL data sources, Spark, DuckDB, pyiceberg) can open the same data.

This is a **prototype**. It tests whether the design works and what it costs. It is not for release builds: the engine needs batch
indexing, and that feature flag (`batch_indexing`) is off in release builds. Names and settings can change with no compatibility promise.
The read side is a separate prototype (`parquet_doc_value_format_2`). This plugin only writes.

## Contents

1. **Using the engine:** [what an index of this format does](#what-an-index-of-this-format-does), [an example](#a-short-example),
   [the rules](#rules-of-the-format), [the settings](#settings), [durability](#durability-what-a-bulk-survives),
   [the limits of the batch path](#limits-of-the-batch-path), [benchmarking](#benchmarking-with-the-clickbench-track) and
   [the limits at a glance](#known-limits-at-a-glance).
2. **[Reference: the format and the table](#reference-the-format-and-the-table):** multi-valued fields, the catalog, reading the data, the
   names of data files, and placement.
3. **[Internals: how the engine works](#internals-how-the-engine-works):** the write path, the flush, the merge, the commit data, durability
   in detail, and what the benchmarks taught us.
4. **[Working on the code](#working-on-the-code)**, **[the full list of known limits](#known-limits-and-open-questions)** and
   **[the recent changes](#recent-changes)**.

## What an index of this format does

- Every shard bulk is written to a local Parquet file before the bulk returns. With the default `index.parquet.durability: staged` a
  copy of the file is also in the repository, below `staging/`, before the bulk is acknowledged (see "Durability: what a bulk survives").
  With `local` the acknowledgement point is the local file only.
- A flush merges the complete local files, uploads the result to a snapshot repository (`s3`, or `fs` for tests), and appends it to the
  Iceberg table of the index.
- After an upload, a background compaction merges small uploaded files into larger ones with one atomic Iceberg commit.
- The index cannot be searched. There is no refresh, no `GET` by id, no search. A read of the data goes through the files.

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
```

After the flush, `<repository>/logs-parquet/<index uuid>/` holds the table: `metadata/v<N>.metadata.json`,
`metadata/version-hint.text`, the Avro manifests, and `data/shard-<n>/<history uuid>/<from seq no>-<to seq no>-<attempt>.parquet`.

## Rules of the format

The settings provider checks these when the index is created and refuses the index with a message that names the problem.

- `index.mode` must be `columnar`. No field is indexed: every field is doc values only. `index.mapping.source.mode` must be `synthetic`.
- No replicas. The provider sets `number_of_replicas` to 0, and `auto_expand_replicas` and any other number are refused. A later settings
  update is not checked by the provider, and the allocation decider keeps such a replica unassigned (see "Placement: the shard is pinned").
- `index.parquet.repository` is required, and the repository must keep conditional writes (see "How several shards commit to one table" below).
- The supported field types are `long`, `integer`, `short`, `byte`, `date`, `keyword`, `double`, `float` and `boolean`, and objects of them
  (a dotted name is one flat column). A field is single-valued, a flat column, unless its mapping says `"multi_value": true`, and then it is a
  list (see "Multi-valued fields"). Other types, multi-fields, `nested`, `index: true` and
  `doc_values: false` are refused. `date_nanos` is refused because Iceberg tables of the format version 2 have no nanosecond timestamp.
- Only `index` and `create` items with auto-generated ids are written. An explicit `_id`, an update and a delete are failures of that item,
  and the other items of the bulk are written.
- Use `dynamic: strict`: a document with a field that the mapping lacks fails the whole shard bulk (see "Limits of the batch path").
- A retried bulk can write its documents again, so delivery is at least once. There is no `_id` lookup to deduplicate.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `index.storage_format` | `native` | `parquet` selects this engine. Final. |
| `index.parquet.repository` | none | The snapshot repository that holds the files. Final, required. |
| `index.parquet.base_path` | `<index name>/<index uuid>` | The path of the table below the base path of the repository. Final. |
| `index.parquet.durability` | `staged` | `staged`: a bulk is acknowledged after a copy of its file is in the repository. `local`: after the local file only. Final. See "Durability: what a bulk survives". |
| `index.parquet.catalog.enabled` | `true` | Every commit of the table creates or updates its entry in the catalog of the repository (see "The catalog"). Final. |
| `index.parquet.catalog.namespace` | `default` | The namespace of the table in the catalog. A dotted value is a nested namespace (`prod.eu` is the directory `prod/eu`). Final. |
| `index.parquet.compression` | `zstd` | Compression of uploaded files (`uncompressed`, `snappy`, `zstd`). |
| `index.parquet.local_compression` | `zstd` | Compression of the local files that every bulk writes, and of their staged copies (same values). With `zstd` the level is 1, and a local file has no dictionary, because the merge of the flush encodes the rows again (see "Which encoding each kind of file gets"). Dynamic. |
| `index.parquet.flush_threshold_size` | `1gb` | Local bytes at which the periodic flush uploads. |
| `index.parquet.flush_threshold_age` | `30m` | Age of the oldest local file at which the periodic flush uploads. |
| `index.parquet.max_local_size` | `4gb` | Local bytes at which batches of at most 5,000 documents get a 429, for each document, until a flush frees space. A bulk can be accepted in part. |
| `index.parquet.max_file_size` | `1gb` | The most **local bytes** that one uploaded file is merged from. With `zstd` local files (the default) the local files are about 1.1 times as large as the uploaded file (345 and 310 bytes for each row in a test with 105 fields and distinct documents); with `uncompressed` local files an uploaded file is about 0.4 times as large. A compaction merges at most this many bytes of uploaded files. |
| `index.parquet.flush_merge_parallelism` | `4` | The threads that encode the columns of one run of a flush (1 to 16): the thread of the flush and tasks of the `parquet_merge` pool. It is lowered to the thread of the flush plus the size of that pool, so more than that does not help. 1 merges on the thread of the flush alone. The threads compete with the bulks for CPU. Dynamic. See "How the merge encodes the columns on several threads". |
| `index.parquet.row_group_size` | `128mb` | Buffered data at which a row group is written. |
| `index.parquet.compaction.enabled` | `true` | Merge small uploaded files in the background. |
| `index.parquet.compaction.target_file_size` | `1gb` | A file below half of it is small and is merged with its neighbours. |
| `index.parquet.compaction.min_files` | `2` | The fewest adjacent small files that a compaction merges. |
| `index.parquet.compaction.delete_delay` | `1h` | How long replaced files stay for readers of an older snapshot. |
| `index.parquet.compaction.interval` | `5m` | How often a shard looks for files to compact when no flush asks for it (after a restart, or after a dropped compaction). Not dynamic. |

A flush also runs when the shard is idle. The thresholds are in bytes and not in rows, because the size of a row is very different from
one data set to the next. A flush starts as soon as the local bytes reach `flush_threshold_size` and no flush runs, so a lower threshold gives
smaller and more files (the compaction merges them) and a smaller flush at the end of a load.

## Durability: what a bulk survives

The engine has no translog and no replica, so the local file of a bulk is the only copy of its rows until a flush has uploaded them.
`index.parquet.durability` (final) chooses what a bulk waits for before it is acknowledged:

- `staged` (the default): a copy of the file is also in the repository, below `staging/`. A bulk survives the loss of the node and of its
  disk. After such a loss a human allocates an empty primary, and the engine adopts the staged files. Every bulk waits for one object-store
  PUT, which took about 30 ms in a benchmark on S3.
- `local`: the local file only. The loss of the node or of its disk loses every acknowledged row that no flush has uploaded yet, which is up
  to `flush_threshold_size` or `flush_threshold_age` of data.

A batch that the engine refuses (a 429 for the local size, a 503 for the staged copy) fails for each of its documents and is never an
exception, so a client has to send again only the documents that failed. The details are in "Durability in detail".

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

## Benchmarking with the clickbench track

The challenge `parquet-write-index` of the clickbench track (`rally-internal-tracks`, branch `clickbench-mount-parquet-challenge`) indexes
the ClickBench corpus (99,997,497 documents, 105 fields) into a parquet index with an S3 repository, flushes it (`flush-and-upload`), and
checks the document count with the index stats. The total time until every row is in the table is the duration of the `index` task plus the
duration of `flush-and-upload`. The index template renders these track parameters into index settings: `parquet_repository_type` and
`parquet_repository_settings` (the repository), `parquet_durability`, `parquet_max_local_size`, `parquet_flush_threshold_size` and
`parquet_flush_merge_parallelism`; `bulk_indexing_clients` and `bulk_size` are the usual ones. The params file for esbench is
`clickbench_parquet_write.json` in `elasticsearch-benchmarks`.

The results so far are single runs (S3 in `eu-west-1`). The first rows are an `i4i.2xlarge` (8 vCPU, x86), the last two a `c6gd.8xlarge`
(32 vCPU, aarch64, 16 GB heap):

| What changed | Clients, bulk size, flush threshold | Indexing | Final flush | Total |
|---|---|---|---|---|
| before the performance work | 8, 10,000, 512 MB | 1,720 s | 494 s | 2,214 s |
| column-wise writer and merge, no shard-wide lock, local bytes counted for each run | 8, 10,000, 512 MB | 704 s | 695 s | 1,399 s |
| merge on 4 threads | 8, 10,000, 512 MB | 732 s | 130 s | 862 s |
| groups balanced by cost | 8, 10,000, 512 MB | 741 s | 86 s | 827 s |
| two groups for each thread, uploads overlap between runs | 8, 10,000, 512 MB | 746 s | 20 s | 766 s |
| c6gd, 8 merge threads | 24, 2,000, 1 GB | 393 s | 248 s | 641 s |
| commit data in memory, footers in parallel | 24, 2,000, 1 GB | 313 s | 141 s | 454 s |

The total time is the sum of the two durations, so the flush has to keep up with the ingest, or the final flush has to catch up on a backlog.
"What the benchmark runs taught us" lists what was not obvious from the numbers, and how the retry runner of the track shows rejected bulks.

## Known limits at a glance

- It is a prototype: the engine needs batch indexing, which is off in release builds, and the index cannot be searched.
- A shard is pinned to its node and has no replica. The loss of a node loses the rows that no flush has uploaded, unless the durability is
  `staged`, and then a human has to allocate an empty primary.
- The repository must enforce conditional writes (AWS S3 and `fs` do), and the format is not secured or reviewed for security.
- It is tested on one data node. Data streams, ILM, rollover, index sorting, shrink, split, clone and cross-cluster replication are not.
- The reader prototype does not read `float`, `double`, `boolean` or the list columns yet.

The full list, with the open questions, is the last section: "Known limits and open questions".

## Reference: the format and the table

How the data looks on disk and in the table, for people who read it or write another reader.

### Multi-valued fields

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

### The catalog

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

### Reading the data

- The table is plain Iceberg (format version 2), so pyiceberg and pyarrow read it. Find it by name through the entry of the catalog (see
  "The catalog") or by its path. A unit test (`IcebergFeasibilityTests`, run with
  `-Dtests.iceberg.export.dir=<dir>`) exports a small table that you can open with them.
- The reader prototype (`parquet_doc_value_format_2`) mounts the data files with `POST /_parquet/_mount`. It does not read `float`,
  `double` and `boolean` yet.
  It does not read the list columns either: the preview lists them as unmapped ("it is inside a struct, list or map, which is not supported yet"),
  and the flat columns of the same table are mapped.
- The ES|QL Iceberg data source cannot read a table by its prefix yet: it sends a HEAD request for the table path first, which is always a
  404 for a prefix, and the Iceberg QA suite of the repository is disabled. `EXTERNAL "s3://bucket/<table>/data/*/*/*.parquet"` reads the
  data files with the Parquet reader, but not through the manifest.
- The Parquet files use `TIMESTAMP(MILLIS)` for `date`. Iceberg calls such a column `timestamptz`, which the spec defines in microseconds,
  and the bounds in the manifest are in microseconds. A strict reader may want `MICROS`; this is not tested.

### Shard histories and the names of data files

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

### Placement: the shard is pinned

Peer recovery copies only the Lucene commit of a shard, which has no segments, so the local Parquet files (`pqlocal_*`) do not move with a
shard. They hold rows that are acknowledged and not uploaded yet, so a relocated primary would start with an empty store and lose them
without an error, and a replica would be an empty copy of a shard that never receives an operation. `ParquetAllocationDecider` (a
`ClusterPlugin` hook) therefore pins the primary of an index with `index.storage_format: parquet` to its node and refuses its replicas. It
leaves every other index alone.

- `canAllocate` says no to every replica, and to a started primary that asks for another node. That covers the balancer, the desired
  balance, the `move` and `allocate_replica` reroute commands, and the replica that `number_of_replicas` creates. An unassigned primary stays
  allocatable: a new index needs a node, and after a restart the gateway allocator picks the node that holds the copy.
- `canRebalance` and `shouldAutoExpandToNode` say no, so the balancer does not try and auto-expansion adds nothing.
- `canForceAllocateDuringReplace` says no. Its default is yes, and it overrides every other no when a node is being replaced.

What a user sees: a primary whose node has to go (a shutdown of type remove or replace, an allocation filter, a disk watermark) cannot
leave. The allocation explain output says that it cannot remain and has no target, and the decider names the reason; a node shutdown stays
stalled. Raising `number_of_replicas` gives a yellow index whose replica stays unassigned, with the same explanation. A node that dies still
loses its local rows. A real handoff (flush and block writes before the move, or ship the local files as recovery files) is future work.

## Internals: how the engine works

These sections explain how the engine is built, for people who change it. Nothing here is needed to use it.

### How a bulk becomes a local file

The engine implements `Engine#indexBatch`. It reads each mapped column of the batch through the typed cursors of Lucene's column API (no
Lucene field is built) and writes one local Parquet file for each batch, named `pqlocal_<from>_<to>.parquet` after the range of sequence
numbers. The shard still keeps one empty Lucene commit and an empty translog, because the server needs both before it calls the engine. The
commit also holds the state of the engine in custom keys (`parquet.pending_upload`, `parquet.uploaded_through_seq_no`,
`parquet.uploaded_rows`); the sequence numbers of the server in it are never rewritten.

The file is written column by column: each mapped column is read from its cursor and written to the column writer of parquet-mr, with no
`Group` for each row. A bulk is one row group with one page for each column, because the server hands the engine at most 5,000 documents at
a time and the merge of the flush makes the row groups that readers want. In a test node with one shard, four clients, local durability and
105 fields like ClickBench's, this took bulk indexing from about 88,000 to about 180,000 documents per second (a laptop, with the parsing and
the client included).

Every file has a column for every mapped field. A batch has a column only for the fields that one of its documents has a value for, but the
schema of a file comes from the mapping, so a field with no value in the batch becomes a column of nulls. The reader prototype needs every
file of a table to have every column of the others, and the schema of a file should not depend on the fields of one bulk. A field that a
mapping update adds later is in the files after the update, and a merge writes the union of its inputs.

### How a flush turns local files into one table commit

A flush runs on a thread pool of its own, and so do the other long tasks of the engine. All four pools have at most half the processors of
the node, and all shards of a node share them, so many shards that flush at once queue for them:

| Pool | Runs |
|---|---|
| `parquet_flush` | a flush, and the adoption of staged files |
| `parquet_merge` | the tasks that encode a group of the columns of a merge, and the reading of footers |
| `parquet_upload` | the upload, the append and the cleanup of a run |
| `parquet_compaction` | a compaction |

A flush must not run on `generic`: closing an index runs on `generic` and blocks on the flush, so with the flush on the same pool a node
that closes many shards at once could run out of threads (and a node with assertions refuses it). The same rule is why the merge and the
upload have pools of their own: the thread that waits for a task must not be one that the task needs. The flush asks a `generic` thread
once for the blob store of the repository, which only some pools may use, and keeps it.

A flush splits the complete local files into runs of equal size, each of at most `max_file_size` local bytes, so that the split does not
leave a tiny file behind. For each run it does these steps, and a crash after any of them loses no row:

1. merge the local files of the run into one file with row groups of the configured size and the field ids of the table;
2. write the pending marker in the commit data;
3. upload the file under its final key (no reader looks at it yet);
4. append it to the Iceberg table, which is the moment readers see the rows;
5. write the high-water mark and the row count in the commit data, and remove the marker;
6. delete the local files.

On start, the engine finishes a flush that a crash cut off. The table, and not the commit, is the source of truth for what is uploaded. A
marker that names a file that the table lists only needs steps 5 and 6. A marker whose merged file still exists is uploaded again under a
new attempt, and a marker whose merged file is gone is dropped, because the local files are deleted only after the table lists their rows.

### How the merge copies columns and never assembles a row

The merge copies the values from the column readers of the local files to the column writers of the merged file, with their repetition and
definition levels, so no row is ever assembled (the merge before it built a parquet-mr `Group` for each row). A column has the same type
and so the same levels in every file, and a column that an earlier file lacks becomes nulls. On 600,000 rows of 105 columns like
ClickBench's (68 integers, 10 longs, 27 keywords) with ZSTD, one thread on a laptop, the merge took 6.2 s where the row-wise merge took
11.2 s, and the files had the same size. The rest of the time is parquet-mr itself: the dictionaries, the min/max statistics of the binary
columns, and the compression (about 14% of the samples).

### How the merge encodes the columns on several threads and still writes the same file

`index.parquet.flush_merge_parallelism` (4 by default, 1 merges on the thread of the flush alone) sets the threads of a merge. The merge
works like this:

- The columns are split into contiguous groups of about the same estimated cost, and there are twice as many groups as threads. The cost of
  a column is 30 ns for each value and 3 ns for each byte before compression, from the footers of a sample of the files. The compressed size
  is the wrong measure: with it, two groups took 0.9 s (14 columns) and 3.2 s (91 columns) on two threads.
- Each group has its own page store, column store and column writers. For each local file the threads take the groups one at a time, the
  most expensive first. The thread of the flush is one worker, the `parquet_merge` pool runs the others, and a worker that has finished a
  group takes the next, so an estimate that is wrong for one group does not leave the other threads waiting.
- A worker reads only the columns of its group from the file. The groups meet after each local file, and that is the only place where a
  row group is cut, so a row group is at most one local file larger than `row_group_size`. The page stores are then written to the row
  group one after the other in the order of the columns.
- The uploaded file has the same row groups, encodings, compression and page indexes as a serial merge makes. It is not byte for byte the
  same, because parquet-mr decides when to cut a page from the columns of a store, and a store has fewer columns now: on 600,000 real rows
  the file is 44,469,596 bytes with one thread and 44,472,769 with four (+0.007%).
- A failed task makes the other tasks stop, and the merge throws only after all of them have ended, because nothing may write to the output
  or read an input that the caller closes after the merge returns. A task that the pool rejects is not needed: the other workers take its
  groups.
- The merge stays serial when the parallelism is 1, when the schema has one field, and when an input has a row group of more than 100,000
  rows (a compacted file), because the cut would be too coarse then. The compaction and the adoption of staged files always merge on one
  thread.

The effective parallelism is the setting or the thread of the flush plus the size of the `parquet_merge` pool, whichever is smaller. The
merge alone, on 600,000 real ClickBench rows (105 columns) on a 12-core laptop:

| Threads | 120 files of 5,000 rows | 300 files of 2,000 rows |
|---|---|---|
| 1 | 4.2 to 4.7 s | 4.7 to 4.8 s |
| 2 | 2.4 s | 1.9 s |
| 4 | 1.0 to 1.3 s | 1.4 to 1.7 s |
| 6 | not measured | 1.0 to 1.2 s |
| 8 | 0.85 to 0.97 s | 1.0 to 1.1 s |
| 12 | not measured | 1.1 to 1.4 s |

The gain flattens at 6 to 8 threads on that laptop, and smaller files cost more because each file costs a footer read for every group.
One to three groups for each thread merged equally fast, and four were slower.

### How an upload overlaps the next merge

Steps 3 to 6 of a run go on in the `parquet_upload` pool while the next run of the same flush is merged. Only one run is announced at a
time: the marker of a run is written after the run before it is in the table, so a crash always has one marker to finish, and the recovery
is the one above. The next run waits for that upload before it writes its own marker, and the local bytes of the run that is uploading do not
count as data that waits for a flush (otherwise a flush would start for data that is already being uploaded and write a tiny file). An
upload that fails in the background is logged and left to the next flush, which finishes the run from its marker. The close of the engine
waits for an upload that runs.

A flush that a caller waits for returns only when the rows of every run are in the table. That is the `_flush` API with `wait_if_ongoing`
true, which is the default, and also the periodic flush of the shard, because `IndexShard` requests it with that default. A flush with
`wait_if_ongoing: false`, which the flush of an idle shard uses, returns while its last upload runs: the next flush skips the local files of
that run (they are neither leftovers nor new), merges its own files, and waits for the upload before it writes its own marker.

So the overlap applies between the runs of one flush (a flush of more than `max_file_size` local bytes) and to the idle flush. It does not
apply to the last upload of a periodic flush. In benchmarks of 100 million rows on S3 with 8 vCPU, that upload took 13 to 17% of the time of
the flush thread, and the merge threads were idle meanwhile. Overlapping it needs a way to tell a periodic flush from a `_flush` request, which
`Engine#flush` does not give.

### How the commit data stays cheap when thousands of local files pile up

Lucene reads a commit by listing the directory twice and starting again until both lists are the same, and the directory holds one file for
each bulk that is not uploaded yet. While bulks create and rename more of them, the two lists hardly ever match, so one read took about 20
seconds with tens of thousands of files. The flush read the commit data for every run, and the stats API read the last commit on every call.
In a benchmark with 2,000-row bulks on 32 vCPU that was 155 of the 365 busy seconds of the flush thread, the flush fell behind, the local
files reached `max_local_size`, and bulks waited for retries of a 429 (a p99.9 of 16 s and a maximum of 46 s). A stats call blocked a
management thread for as long.

So `StoreCommitData` reads the user data from the store once and keeps it. A commit replaces it, and a commit that fails makes the next read
ask the store. The last commit that the stats API reads (`getLastCommittedSegmentInfos`) is read again only after a commit of the engine,
which a version counter tells. This is correct because the engine is the only one that changes the user data of the commit while it is open:
the server writes it when it creates or recovers a store, and then no engine is open.

The schema of a run is the union of the schemas of all its local files, so the flush opens every file. It reads the footers on the threads of
the merge (at least 64 for each thread), because 6,800 files took about 20 seconds on one.

### Which encoding each kind of file gets

The two kinds of file are encoded differently, on purpose:

| | Local file of a bulk, and its staged copy | File of a flush, of a compaction, of an adoption |
| --- | --- | --- |
| Lives | until the next flush, and one read by the merge | in the object store, for a long time |
| Dictionary | no | yes, where it pays |
| ZSTD level | 1 | 3 |
| Row groups | one for each bulk, one page for each column | `index.parquet.row_group_size` |

The local file is written once and read once, by a merge that encodes the rows again, so it gets the cheapest encoding. The file that stays
in the object store keeps the encoding that makes it small, and its storage efficiency does not change. In a test node (one shard, four
clients, local durability, 600,000 distinct documents with 105 fields like ClickBench's), with a dictionary and level 3 the write threads
used 11 to 12 microseconds of CPU for each document, and without a dictionary and with level 1 they used 6. The local files were 6% smaller
(345 against 367 bytes for each row), because a column with many different values wrote its dictionary and then the values again after the
writer gave the dictionary up. The uploaded file had the same size in every run (185.8 MB), and the flush took the same time. On the real
ClickBench rows the change was neutral: 79 bytes for each row either way. The level is not in the file, and a reader handles pages with and
without a dictionary, so files that an earlier version wrote read the same. `ParquetFlushIT` checks that the local files have no dictionary
and that the uploaded file has one.

### How several shards commit to one table

All shards of an index append to one table. A commit writes `v<N+1>.metadata.json` with a put-if-absent (`If-None-Match: *` on S3), and
Iceberg retries a commit that lost the race. **The repository must enforce conditional writes.** On a store that ignores the header, two
shards that commit at the same moment both write the same version, the later one replaces the earlier one, and the flush still reports
success. The `fs` repository and AWS S3 enforce it.

### How the compaction merges small uploaded files

After a flush that uploaded files, one compaction of the shard runs at a time. It chooses runs of *adjacent* small files (the order of the
rows of a shard never changes), merges them reading the inputs with ranged reads, uploads the result, and commits one Iceberg rewrite that
names the snapshot it planned from. It then expires snapshots older than `delete_delay`, which deletes the replaced files, and deletes files
that no snapshot lists (the output of a compaction that crashed after its upload). A file that was replaced stays readable for the delay.

### Durability in detail

The engine has no translog and no replica, so the file of a bulk is the only copy of its rows until a flush has uploaded them. The setting
`index.parquet.durability` (final) says where that copy is when the bulk is acknowledged.

- `local`: the local file is durable (written and renamed). The loss of the node or of its disk loses every acknowledged row that no flush
  has uploaded yet, which is up to `flush_threshold_size` or `flush_threshold_age` of data.
- `staged` (the default): after the local file, the same bytes are uploaded to
  `<table>/staging/shard-<n>/<history uuid>/<from>-<to>-<attempt>.parquet`, and only then is the bulk acknowledged. This is the role of the
  translog in stateless Elasticsearch. The key has an attempt and the upload is a put-if-absent, as for data files. `staging/` is outside
  `data/`, so readers and Iceberg never see it.

#### What a bulk does with staged durability

A bulk opens the table before it writes anything, so a repository that is missing or a table that another index owns fails the bulk (HTTP
503) with no local file and no gap in the sequence numbers. It then takes its sequence numbers, writes the local file, and uploads the
staged copy. None of these steps holds a shard-wide lock, so the local files and the uploads of concurrent bulks of a shard overlap (a
profile of a run with one shard and 8 clients showed the bulks waiting for each other on a lock that was held during the write of the local
file). If the write or the upload fails, the local file is deleted, the documents fail (a 503 for the upload), and the sequence numbers
stay used. That is a gap, which the engine tolerates because the ranges are in the names and the rows come from the footers. A batch that
is rejected with a 429 takes no sequence numbers.

#### Why a refusal is a result for each document and not an exception

The server hands the engine the documents of a bulk in batches of at most 5,000, one after the other. The engine refuses a batch (a 429
when the local files have reached `max_local_size`, a 503 when the repository cannot take the staged copy) by returning a failed result for
each document of it. An exception would fail the whole shard request, and the client would be told that the documents of the earlier
batches failed too, although they are written. A client that sends them again indexes them twice: a benchmark with a retry of the whole
bulk counted 40,000 to 65,000 documents too many that way. So a bulk of 10,000 documents can be accepted in part, and a client has to
send again only the documents that failed (`ParquetFlushIT` does it, with many clients, constant flushes and a limit that rejects).

#### Which local files a flush may take

Because bulks finish in any order, a flush uploads only the local files whose range is at or below the processed checkpoint of the shard,
which means that every bulk with a lower range has finished (a failed bulk marks its range as processed too). A flush records the highest
range that it uploaded and deletes every local file below it as a leftover, so a file of a higher range must never be uploaded while a lower
range is still being written. With staged durability a range is marked processed only after its staged copy is uploaded (or the bulk is
discarded), so a flush cannot put the rows of a bulk in the table while the upload of its staged copy is still being tried. Without this a
bulk could fail after its rows were in the table, and a client that sent it again would index the rows twice. The cost is that a slow
upload holds back the flush of the files of the later bulks until it ends, which is at most the time of the three tries.

#### How the staged upload is retried

A request to the repository can fail for a moment (a throttled request, a reset connection, a credential that is being renewed), and the
upload is on the path that acknowledges the bulk. So the engine tries it three times, with waits of 250 ms and 1 s, under a new key each
time, and it deletes the blob of a try that failed. If every try fails, the documents of the batch fail with a 503 whose message has the
class and the message of the exception and of its causes (`...not acknowledged: [AccessDeniedException: ...]`), and the engine logs the
exception at `WARN`.

#### How the local bytes are counted

The engine counts the local rows and bytes for the limit `index.parquet.max_local_size` and for the statistics
(`docs.total_size_in_bytes`). The limit is checked before a bulk writes, so bulks that are writing at that moment can take the local size a
little past it. A flush cuts a large backlog into runs of at most `max_file_size` local bytes, and a run takes minutes to merge, so the
counter must not wait for the whole flush: in a benchmark that reached the limit, every client waited about 4 minutes (the longest bulk
took 239.8 s) because the counter still held files of runs that were already uploaded and deleted.

The flusher therefore tells the engine after each committed run, with the commit data moved and the local files of the run deleted, and
also when a flush deletes the leftover files of a run that a crash had committed. The engine counts the directory again under the write
side of its lock, so a bulk that is writing is never counted twice or not at all. A failure to count is logged and does not fail the
flush: the engine counts again when the flush ends. The bytes of a run that is uploading in the background count until its upload ends,
but they do not count as data that waits for a flush (see "How an upload overlaps the next merge").

#### What happens to the staged copies

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

#### What staged durability costs

Every bulk waits for one object-store PUT, and the write availability depends on the object store. One small object is created for each
shard bulk and deleted by a flush. In a benchmark on S3 (24 clients, bulks of 2,000 documents) the PUT took about 30 ms for each bulk, which
was 63% of the wall time that the write threads spent in `indexBatch`; the rest was mostly the write of the local file. It is not measured
against `local` durability, so the saving from skipping it is not known.

### What the benchmark runs taught us

The track has its own runner for the bulk operation, because Rally does not retry the items of a bulk that the engine rejects with a 429
(and a retry of the whole bulk indexes the accepted items twice). It sends only the rejected items again, after a wait of 0.5 s that
doubles up to 10 s, so a service time of about 16 s or 46 s in the results is five or eight retries. That points to bulks that the engine refused
because the local files reached `max_local_size`; in the runs so far this was inferred from the pattern (the delays went away when the flush
kept up), and not read from a log. esbench builds Elasticsearch from the local worktree, with its uncommitted changes, and the build hash shows only the last
commit.

Things that were not obvious from the numbers:

- A run is only as good as its hardware: check the node in the JFR file of the run (`jfr print --events jdk.CPUInformation`) and the
  instance name in the params file. One run with 24 clients on a node that turned out to have 8 vCPU had 8 write threads and no gain.
- The write path scales with the clients until the write pool or the CPU is full, but the flush has to keep up. When it does not, the local
  files reach `max_local_size`, bulks wait for retries, and the final flush has to catch up.
- Smaller bulks make more local files, and every operation that lists the directory costs more (see "How the commit data stays cheap when
  thousands of local files pile up"). In the run with 2,000-row bulks, 63% of the wall time that the write threads spent in
  `indexBatch` was the staged PUT.

## Working on the code

### Layout of the code

| Package | Contents |
|---|---|
| `org.elasticsearch.xpack.parquet.write` | the plugin, the settings, the settings provider, the mapping validator and the allocation decider |
| `.engine` | `ParquetWriteEngine`, the shard tables, the commit data, the handling of linkage errors |
| `.file` | the local files, the batch writer, the merger, the codecs, input files for a directory and for a blob |
| `.flush` | the flusher and its crash protocol |
| `.staging` | the adoption of the staged files of lost stores |
| `.compaction` | the compactor and its policy |
| `.table` | the Iceberg table of a shard, its `FileIO` and `TableOperations` over a blob store, the metrics of a file, the table location |

The plugin extends only `esql-datasource-iceberg`. A node refuses a plugin that extends both the Parquet reader plugin and the Iceberg
plugin, because both contain `parquet-hadoop-bundle`. So the plugin has its own read options and codecs.
`IcebergDataSourcePlugin` is an `ExtensiblePlugin` for this reason.

### Tests

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

### Running a node by hand

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

## Known limits and open questions

- Not secured or reviewed for security. A user who can create an index with this format chooses `index.parquet.repository` and
  `index.parquet.base_path`, and the engine writes below that path in the repository with the credentials of the repository. It does not check
  who may use a repository, and it does not check the segments of `base_path`, so a path can point into the table of another index or into
  other data of the repository. A table that another index owns is refused (see "Shard histories and the names of data files"), but a path
  that points into other data of the repository is not.
- Whether the repository enforces conditional writes is not checked. On a store that ignores `If-None-Match` the commits of two shards can
  replace each other without an error (see "How several shards commit to one table"). The `fs` repository and AWS S3 enforce it.
- Tested on one data node. Writers that commit at the same moment are tested in one JVM (6 concurrent writers in `IcebergFeasibilityTests`, 2 to
  4 shards that compact together in `ParquetCompactorTests`, 2 shards on one node in the cluster tests). The protocol does not depend on where
  the shards run, but a cluster with several data nodes is not tested.
- Not tested: data streams, ILM and rollover, index sorting, shrink, split and clone, reindex into such an index, cross-cluster replication.
- A clean shutdown does not upload. The local files stay on the disk, and the shard finds them when it starts again. With `local`
  durability the rows are not safe against the loss of the disk before their flush; with `staged` they are in the staging files.
- No relocation. A shard cannot move (the allocation decider refuses it, see "Placement: the shard is pinned"), a node shutdown of type
  remove or replace stalls on such a primary, and the loss of a node loses the rows that are not uploaded yet with `local` durability
  (with `staged` they are adopted, see "Durability"). There is no replica. Snapshots of such an index copy only the empty commit, and a
  restore gives a new history (see "Shard histories and the names of data files").
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
  they do not give a lossless relocation yet.
- The catalog entry is a file of our own format, with no compare-and-set (it can lag by a version, see "The catalog"). Spark, Trino and DuckDB
  need a shim to use it. An implementation of Iceberg's `Catalog` interface over the entries, the Hadoop-catalog layout, and a REST catalog
  client are not built.
- A compaction that crashes inside the Parquet or Iceberg libraries with a `LinkageError` is logged, and has no test (the flush has one).
- The merge reads the footer of a local file once for each group, which is 16 parses for each 2,000-row file with 8 threads. In a benchmark that
  was 37% of the CPU of the merge, and the merge threads were busy for only about half of their time, probably because every file also
  costs a barrier. Sharing one parsed footer between the groups of a file, and putting the barrier after several files, is not built.
- The last upload of a periodic flush is not overlapped with the next merge (see "How an upload overlaps the next merge"), and the default of
  `flush_merge_parallelism` (4) does not depend on the number of processors: on a node with 32 vCPU a larger value is needed to keep up
  with the ingest.
- A float or double column has no minimum and maximum in the footer (parquet-mr 1.18.1 with our writer settings); not investigated.
- Whether `index.storage_format` is the final name, and when its definition moves from the plugin to `server`.
- Whether to register the `parquet` extension in `LuceneFilesExtensions` and not rely on the test property.
- Whether to delete the files of an index when the index is deleted (a data lake keeps them today).

## Recent changes

The commits of the engine since last review, oldest first, each with what it does and why. The measurements are from the clickbench track (see
"Benchmarking with the clickbench track").

1. **`d337ce2` Keep data files of shard histories apart.** A data file is now named `data/shard-<n>/<history uuid>/<from>-<to>-<attempt>.parquet`,
   and the upload fails if the key exists, so a shard that starts again at sequence number 0 with a new store (or two indices with the same
   `base_path`) never replaces a listed file. Recovery of a pending flush checks the exact key, and a shard table lists, compacts and cleans only
   the files of its own history. A table also records the uuid of its index, so a shard of another index refuses to open it.

2. **`873f6b0` Pin parquet primaries and refuse replicas.** An allocation decider keeps the primary of a parquet index on its node and
   refuses its replicas. The local files do not move with a shard, so a relocation or a replica would lose rows without an error.

3. **`847e587` Stage bulk files in the object store.** The new setting `index.parquet.durability` defaults to `staged`: a copy of each bulk
   file goes to the repository before the bulk is acknowledged. After the loss of a node and its disk, the new engine adopts the staged files
   into the table, and with `local` the acknowledgement point stays the local file.

4. **`500811b` Write local files of a shard in parallel.** The shard-wide write lock is gone, and bulks write their local files at the same
   time. A flush takes only files up to the processed checkpoint, so it never uploads a file while a lower range is still being written.

5. **`0108006` Merge Parquet files by column, not by row.** The merge copies values from the column readers to the column writers with their
   levels, and it never builds a row. On 600,000 rows it took 6.2 s instead of 11.2 s, with files of the same size.

6. **`fca9971` Write the local file of a bulk by column.** The batch writer reads each mapped column from its cursor and writes it to the
   column writer, with no row objects. Bulk indexing went from about 88,000 to about 180,000 documents per second in a test node.

7. **`2d8889d` Fail the documents of a refused batch.** A refusal (a 429 for the local size, a 503 for the staged copy) is now a failed
   result for each document and not an exception. An exception failed the whole shard request, and a client that retried indexed documents
   twice (40,000 to 65,000 too many in one benchmark).

8. **`9a7e9c1` Write local files without a dictionary at level 1.** Local files and their staged copies use no dictionary and ZSTD level 1,
   because the merge encodes the rows again. The uploaded files keep the dictionary and level 3, so their size does not change.

9. **`b8f3bf7` List only histories that have a staged file.** The adoption of staged files lists only the directories that hold at least
   one staged blob. This fixed a flaky unit test that failed when the file system added an extra directory.

10. **`e8f20c5` Retry the staged upload and show why it failed.** The staged upload is tried three times, with waits of 250 ms and 1 s, under a
    new key each time. A final 503 names the class and message of the exception, and the engine logs a warning. A flush can no longer take a
    file before its staged copy is uploaded.

11. **`896366f` Count the local files again after each run.** The flusher tells the engine after each committed run, and the engine counts
    the local rows and bytes again. Before, the counter held files that were already uploaded, and in one benchmark every client waited
    about 4 minutes.

12. **`3810090` Encode the columns of a flush merge in parallel.** The columns are split into groups, each thread encodes its group into its
    own page store, and the stores are written to the row group in column order, so the file has the same row groups, encodings and page
    indexes. The merge of 600,000 rows took 1.1 s with 4 threads instead of 4.2 to 4.7 s. A new `parquet_merge` pool and the setting
    `index.parquet.flush_merge_parallelism` (default 4) control it.

13. **`94a3903` Balance the merge and overlap uploads.** A merge makes two groups for each thread, and workers take the next group from a
    queue. The upload of a run runs on a new `parquet_upload` pool while the next run of the same flush merges. A periodic flush still waits
    for its last upload, so only the runs of one flush and the idle flush overlap.

14. **`11c1ee5` Keep the commit data in memory, read footers in parallel.** The server needs one empty Lucene commit for each shard, and the
    engine keeps its flush state (the pending marker and the uploaded high-water mark) in the user data of that commit, which is why it reads
    and writes it. The engine now reads that data from the store once, keeps it in memory, and re-reads the last commit for the stats API only
    after a commit of its own, because Lucene's read lists the whole directory and took about 20 s with tens of thousands of local files
    (indexing went from 393 s to 313 s). The footers for the union schema are also read in parallel.
