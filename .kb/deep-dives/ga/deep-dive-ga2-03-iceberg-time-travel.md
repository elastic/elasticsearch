# Deep Dive: Iceberg Time-Travel in ES|QL

## Executive Summary

There is **zero** time-travel support in the current Iceberg integration. The code always reads the latest metadata version. Adding time-travel requires changes at five layers: grammar, plan nodes, parameter plumbing, SPI adapter, and Iceberg scan configuration. The existing `WITH` map parameters provide a viable delivery mechanism without grammar changes, though first-class syntax (`AS OF`) would be more ergonomic.

---

## 1. Current Snapshot Reading

### How the latest snapshot is selected

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java`

The `resolveTable()` method (lines 41-60) always reads the **latest** metadata version:

```java
// Line 47: Find the latest metadata file
String metadataLocation = findLatestMetadataFile(tablePath, fileIO);

// Line 50-51: Load table using StaticTableOperations
StaticTableOperations ops = new StaticTableOperations(metadataLocation, fileIO);
Table table = new BaseTable(ops, tablePath);
Schema schema = table.schema();
```

The `findLatestMetadataFile()` method (lines 75-118) follows this algorithm:
1. First tries `version-hint.text` (lines 82-100) -- reads the integer version from the hint file and constructs `v{N}.metadata.json`
2. Falls back to scanning versions 100 down to 1 (lines 104-115), returning the first file that exists

**Critical observation**: `StaticTableOperations` is constructed with a single metadata file location. The Iceberg `Table` object built from `StaticTableOperations` *does* contain snapshot history (it's in the metadata JSON), but the code never accesses it. It uses `table.schema()` only. The `table.currentSnapshot()`, `table.snapshot(long)`, and `table.history()` methods **are available** on the `Table` interface but are never called.

### Three places that build Table objects

All three always use `table.newScan()` with no snapshot configuration:

1. **IcebergCatalogAdapter.resolveTable()** (line 51) -- schema discovery only
2. **IcebergTableCatalog.planScan()** (line 69-73):
   ```java
   // File: /Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java
   // Lines 69-73
   StaticTableOperations ops = new StaticTableOperations(metadata.metadataLocation(), fileIO);
   Table table = new BaseTable(ops, tablePath);
   TableScan scan = table.newScan();  // Always current snapshot
   ```
3. **IcebergSourceOperatorFactory.createIcebergTableReader()** (lines 130-138):
   ```java
   // File: /Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java
   // Lines 130-138
   IcebergTableMetadata metadata = IcebergCatalogAdapter.resolveTable(tablePath, s3Config);
   // ...
   Table table = new org.apache.iceberg.BaseTable(ops, tablePath);
   TableScan scan = table.newScan().planWith(EsExecutors.DIRECT_EXECUTOR_SERVICE);
   ```

---

## 2. Iceberg Snapshot API

### Available methods (Iceberg 1.10.1)

The project uses `org.apache.iceberg:iceberg-core:1.10.1` (from `/Users/oleglvovitch/github/root/elasticsearch/build-tools-internal/version.properties`, line 27).

**On the `Table` interface** (from Iceberg API docs):
- `Table.currentSnapshot()` -- returns the current `Snapshot` object
- `Table.snapshot(long snapshotId)` -- returns a specific snapshot by ID
- `Table.history()` -- returns `List<HistoryEntry>` with snapshot history (timestamps + IDs)
- `Table.snapshots()` -- returns `Iterable<Snapshot>` of all valid snapshots

**On the `TableScan` interface** (from Iceberg API docs):
- `TableScan.useSnapshot(long snapshotId)` -- creates a new TableScan pinned to a specific snapshot ID
- `TableScan.asOfTime(long timestampMillis)` -- creates a new TableScan for the most recent snapshot as of a given timestamp

Both `useSnapshot()` and `asOfTime()` return a new `TableScan` instance (immutable builder pattern). They are chained exactly like `.filter()` and `.select()`.

### How time-travel would work at the Iceberg API level

For scan planning (IcebergTableCatalog):
```java
TableScan scan = table.newScan();
if (snapshotId != null) {
    scan = scan.useSnapshot(snapshotId);
} else if (asOfTimestamp != null) {
    scan = scan.asOfTime(asOfTimestamp);
}
```

For data reading (IcebergSourceOperatorFactory):
```java
TableScan scan = table.newScan().planWith(EsExecutors.DIRECT_EXECUTOR_SERVICE);
if (snapshotId != null) {
    scan = scan.useSnapshot(snapshotId);
} else if (asOfTimestamp != null) {
    scan = scan.asOfTime(asOfTimestamp);
}
```

**Important**: The schema may differ between snapshots (Iceberg supports schema evolution). Time-travel to an old snapshot might expose different columns than the current schema. The Iceberg `TableScan` handles this by projecting via the snapshot's schema.

---

## 3. Metadata Structure

### How Iceberg stores snapshots

An Iceberg `metadata.json` file (the file `IcebergCatalogAdapter.findLatestMetadataFile()` locates) contains:

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "current-snapshot-id": 3497810964824022504,
  "snapshots": [
    {
      "snapshot-id": 3497810964824022504,
      "timestamp-ms": 1709825612345,
      "manifest-list": "s3://bucket/table/metadata/snap-34978...avro",
      "summary": { "operation": "append", "added-data-files": "3", ... }
    },
    {
      "snapshot-id": 1234567890123456789,
      "timestamp-ms": 1709739212345,
      "manifest-list": "s3://bucket/table/metadata/snap-12345...avro",
      ...
    }
  ],
  "snapshot-log": [
    { "snapshot-id": 1234567890123456789, "timestamp-ms": 1709739212345 },
    { "snapshot-id": 3497810964824022504, "timestamp-ms": 1709825612345 }
  ],
  "schemas": [ ... ],
  "current-schema-id": 0
}
```

### Does the current code parse snapshot history?

**No.** The code in `IcebergCatalogAdapter.resolveTable()` creates a `StaticTableOperations` from the metadata file. When `BaseTable` is constructed, it parses the full metadata JSON -- including `snapshots`, `current-snapshot-id`, and `snapshot-log`. However, the code only accesses `table.schema()` (line 52 of IcebergCatalogAdapter.java).

The `IcebergTableMetadata` class (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableMetadata.java`) stores:
- `tablePath` (String)
- `schema` (Iceberg Schema -- from current snapshot)
- `s3Config` (S3Configuration)
- `sourceType` (String, always "iceberg")
- `metadataLocation` (String -- the metadata file path)

There is **no** field for snapshot ID, timestamp, or snapshot list. The `Table` object is not preserved; only the extracted schema and metadata location are kept.

---

## 4. UnresolvedExternalRelation Params

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java`

The `params` field (line 33) is typed as `Map<String, Expression>`:

```java
// Line 33
private final Map<String, Expression> params;
```

This map is populated from the `WITH { ... }` clause in the EXTERNAL command. The grammar rule at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`, line 111:

```antlr
externalCommand
    : DEV_EXTERNAL stringOrParameter commandNamedParameters
    ;

commandNamedParameters
    : (WITH mapExpression)?     // Lines 265-267
    ;
```

The map expression (`{ "key": value, ... }`) supports string keys and constant values (strings, integers, booleans, decimals, null). This is parsed in `LogicalPlanBuilder.visitExternalCommand()` at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`, lines 781-788:

```java
public LogicalPlan visitExternalCommand(EsqlBaseParser.ExternalCommandContext ctx) {
    Source source = source(ctx);
    Expression tablePath = expression(ctx.stringOrParameter());
    MapExpression options = visitCommandNamedParameters(ctx.commandNamedParameters());
    Map<String, Expression> params = options != null ? options.keyFoldedMap() : Map.of();
    return new UnresolvedExternalRelation(source, tablePath, params);
}
```

### Could params carry snapshot specifiers?

**Yes, today, with no grammar changes.** A user could write:

```esql
EXTERNAL "s3://bucket/table" WITH { "snapshot_id": 3497810964824022504 }
EXTERNAL "s3://bucket/table" WITH { "as_of_timestamp": "2024-03-01T12:00:00Z" }
```

The params flow through:
1. **Parser** -> `UnresolvedExternalRelation.params` (Map<String, Expression>)
2. **EsqlSession.extractIcebergParams()** -> `Map<String, Map<String, Expression>> pathParams`
   File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`, line 878
3. **ExternalSourceResolver.resolve()** -> `paramsToConfigMap()` converts to `Map<String, Object>`
   File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`, lines 286-305
4. **ExternalSourceFactory.resolveMetadata(location, config)** receives it
   File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java`, line 26
5. **IcebergTableCatalog.metadata()** receives it as `Map<String, Object> config`
   File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`, line 48

The plumbing exists end-to-end. The only missing piece is the Iceberg code reading `snapshot_id` or `as_of_timestamp` from the config map.

---

## 5. Grammar Extension Points

### Option A: Use existing WITH clause (no grammar changes)

The simplest approach -- snapshot specifiers become config parameters:

```esql
EXTERNAL "s3://bucket/table" WITH { "snapshot_id": 3497810964824022504 }
EXTERNAL "s3://bucket/table" WITH { "as_of": "2024-03-01T12:00:00Z" }
```

**Pros**: Zero grammar changes, works today.
**Cons**: Not discoverable, no type checking, mixed with credentials.

### Option B: Add AS OF / AT SNAPSHOT to the EXTERNAL command grammar

Modify the parser rule at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`, line 111:

**Current**:
```antlr
externalCommand
    : DEV_EXTERNAL stringOrParameter commandNamedParameters
    ;
```

**Proposed**:
```antlr
externalCommand
    : DEV_EXTERNAL stringOrParameter timeTravelClause? commandNamedParameters
    ;

timeTravelClause
    : AS OF constant          // timestamp-based: AS OF "2024-03-01T12:00:00Z"
    | AT SNAPSHOT constant    // snapshot-id-based: AT SNAPSHOT 3497810964824022504
    ;
```

**New lexer tokens needed** in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/lexer/From.g4`:
- `AS` -- already exists in Expression.g4 (used by RENAME)
- `OF` -- new token (or reuse UNQUOTED_SOURCE)
- `AT` -- new token
- `SNAPSHOT` -- new token

These would need to be added to FROM_MODE since EXTERNAL pushes into `FROM_MODE`:
```antlr
// In lexer/From.g4
FROM_AS : 'as' -> type(AS);
FROM_OF : 'of';  // New token
FROM_AT : 'at';  // New token
FROM_SNAPSHOT : 'snapshot';  // New token
```

**Cons**: New keywords could conflict with index names; must be DEV-gated. Requires Kibana ANTLR dictionary update.

### Option C: When migrating to FROM datasource::expression syntax

The future target syntax `FROM iceberg::s3://bucket/table` could naturally accommodate time-travel:

```esql
FROM iceberg::s3://bucket/table AT SNAPSHOT 123456789
FROM iceberg::s3://bucket/table AS OF "2024-03-01T12:00:00Z"
```

This would go in whatever grammar rule replaces `externalCommand` with the `FROM` extension.

---

## 6. ExternalSourceExec -- Does It Carry Time/Version Info?

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

**No dedicated snapshot field exists.** The node carries (lines 58-66):
- `sourcePath` (String)
- `sourceType` (String)
- `attributes` (List<Attribute>)
- `config` (Map<String, Object>) -- **snapshot info could go here**
- `sourceMetadata` (Map<String, Object>) -- **or here**
- `pushedFilter` (Object, opaque, not serialized)
- `estimatedRowSize` (Integer)
- `fileSet` (FileSet, not serialized)
- `splits` (List<ExternalSplit>)

**Could it carry a snapshot parameter?** Yes, via two paths:

1. **config map**: The `config` field is already serialized (line 146, 174) as a generic map via `writeGenericValue`/`readGenericValue`. Adding `"snapshot_id"` or `"as_of_timestamp"` to the config map requires zero changes to ExternalSourceExec.

2. **sourceMetadata map**: Same serialization mechanism. Could store snapshot-related metadata here.

The `ExternalRelation.toPhysicalExec()` method at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`, lines 115-127, passes `metadata.config()` and `metadata.sourceMetadata()` through to ExternalSourceExec.

---

## 7. What Exactly Needs to Change

### Minimal implementation (Option A: WITH clause, no grammar changes)

#### Layer 1: IcebergTableCatalog.extractS3Config() (or new method)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`

Add snapshot parameter extraction from the config map:

```java
// In extractS3Config() or a new extractSnapshotConfig() method
Long snapshotId = config.containsKey("snapshot_id")
    ? Long.parseLong(config.get("snapshot_id").toString())
    : null;
String asOfTimestamp = (String) config.get("as_of_timestamp");
```

#### Layer 2: IcebergCatalogAdapter.resolveTable()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java`

Add overload or parameter for snapshot selection:

```java
public static IcebergTableMetadata resolveTable(
    String tablePath, S3Configuration s3Config, Long snapshotId, Long asOfTimestamp
) throws Exception {
    // ... existing code to find metadata and create Table ...

    Schema schema;
    if (snapshotId != null) {
        Snapshot snap = table.snapshot(snapshotId);
        schema = table.schemas().get(snap.schemaId());
    } else if (asOfTimestamp != null) {
        // Find snapshot at timestamp
        for (HistoryEntry entry : table.history()) {
            if (entry.timestampMillis() <= asOfTimestamp) {
                snapshotId = entry.snapshotId();
            }
        }
        Snapshot snap = table.snapshot(snapshotId);
        schema = table.schemas().get(snap.schemaId());
    } else {
        schema = table.schema(); // current
    }

    return new IcebergTableMetadata(tablePath, schema, s3Config, "iceberg", metadataLocation, snapshotId);
}
```

**Note**: Schema at a historical snapshot may differ from current schema. This is the main complexity.

#### Layer 3: IcebergTableMetadata

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableMetadata.java`

Add optional `snapshotId` field:

```java
private final Long snapshotId;  // null = current snapshot
```

#### Layer 4: IcebergTableCatalog.planScan()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`, line 73

```java
TableScan scan = table.newScan();
if (snapshotId != null) {
    scan = scan.useSnapshot(snapshotId);
}
```

#### Layer 5: IcebergSourceOperatorFactory.createIcebergTableReader()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`, line 138

Same pattern:
```java
TableScan scan = table.newScan().planWith(EsExecutors.DIRECT_EXECUTOR_SERVICE);
if (snapshotId != null) {
    scan = scan.useSnapshot(snapshotId);
}
```

### Full implementation (Option B: AS OF / AT SNAPSHOT syntax)

All of the above, plus:

#### Grammar changes

1. **Lexer** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/lexer/From.g4`): Add `FROM_AT`, `FROM_OF`, `FROM_SNAPSHOT` tokens in FROM_MODE
2. **Parser** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`, line 111): Add `timeTravelClause?` to `externalCommand` rule
3. **LogicalPlanBuilder** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`, lines 781-788): Parse the time-travel clause into special params

#### Plan node changes

**UnresolvedExternalRelation** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java`): Two options:
- (a) Add dedicated `snapshotId`/`asOfTimestamp` fields -- cleaner but couples plan node to Iceberg
- (b) Store in `params` map under reserved keys like `"__snapshot_id"` -- works with existing structure

**No changes needed** to ExternalRelation or ExternalSourceExec -- the snapshot info flows through the config/sourceMetadata maps.

### Data flow summary (end-to-end)

```
User query:  EXTERNAL "s3://bucket/table" WITH { "snapshot_id": 123 }
    |
    v
Parser (LogicalPlanBuilder.visitExternalCommand)
    -> UnresolvedExternalRelation(tablePath, params={"snapshot_id": Literal(123)})
    |
    v
PreAnalyzer.preAnalyze()
    -> collects "s3://bucket/table" into icebergPaths
    |
    v
EsqlSession.extractIcebergParams()
    -> pathParams = {"s3://bucket/table": {"snapshot_id": Literal(123)}}
    |
    v
ExternalSourceResolver.resolve()
    -> paramsToConfigMap() converts to {"snapshot_id": "123"}
    -> resolveSingleSource() finds IcebergTableCatalog
    |
    v
IcebergTableCatalog.metadata("s3://bucket/table", {"snapshot_id": "123"})
    -> extractSnapshotConfig() reads snapshotId=123
    -> IcebergCatalogAdapter.resolveTable(path, s3Config, snapshotId=123)
    -> table.snapshot(123) -> schema at that snapshot
    -> IcebergTableMetadata with snapshotId=123
    |
    v
Analyzer (ResolveExternalRelations)
    -> ExternalRelation with SourceMetadata containing snapshot config
    |
    v
Physical planning
    -> ExternalSourceExec with config={"snapshot_id": "123"}
    |
    v
Operator creation
    -> IcebergSourceOperatorFactory reads snapshotId from config
    -> scan.useSnapshot(123)
```

---

## Key Risks and Considerations

1. **Schema evolution**: A historical snapshot may use a different schema version. The code must use the schema associated with the target snapshot, not `table.schema()` (which is the current schema). Iceberg's `TableScan.useSnapshot()` handles this for data reading, but the *metadata resolution* (schema discovery) path must also be aware.

2. **StaticTableOperations limitation**: `StaticTableOperations` loads from a single metadata file. All snapshots referenced in that metadata file are accessible. However, if snapshots have been expired and garbage-collected, they won't be in the metadata file. This is an Iceberg-level limitation, not an ES|QL one.

3. **Snapshot discovery**: Users need to know the snapshot ID or timestamp. A `SHOW SNAPSHOTS` or schema discovery API would be useful but is out of scope for the initial implementation.

4. **Config map serialization**: The `ExternalSourceExec.config` map uses `writeGenericValue`/`readGenericValue`. Long values may be serialized as strings when passed through `paramsToConfigMap()` (line 299 of ExternalSourceResolver.java does `value.toString()`). The Iceberg plugin must handle string-to-long conversion.

5. **Validation**: Need to validate that the requested snapshot actually exists before creating the scan. `table.snapshot(snapshotId)` returns null if the snapshot doesn't exist -- must throw a clear error.
