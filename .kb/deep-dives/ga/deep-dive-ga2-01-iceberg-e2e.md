# Deep Dive: Iceberg End-to-End

**Date**: 2026-03-03
**Branch**: `esql/connector-spi-v3` (main at `970b4789c35`)
**Status**: Metadata-only on main. Data reading infrastructure exists but is not wired.

---

## 1. IcebergCatalogAdapter

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java`

### What it does

A static utility class for resolving Iceberg table metadata from S3. It does NOT implement any SPI interface.

### `resolveTable(String tablePath, S3Configuration s3Config)` (lines 41-60)

1. Creates an `S3FileIO` via `S3FileIOFactory.create(s3Config)` (line 43)
2. Calls `findLatestMetadataFile(tablePath, fileIO)` to locate the current metadata JSON (line 47)
3. Loads the table via `StaticTableOperations(metadataLocation, fileIO)` + `BaseTable(ops, tablePath)` (lines 50-51)
4. Extracts the Iceberg `Schema` from the table (line 52)
5. Returns `IcebergTableMetadata` containing path, schema, s3Config, sourceType="iceberg", and metadataLocation (line 55)
6. Closes the `S3FileIO` in a finally block (line 58)

**Key design choice**: Uses `StaticTableOperations` instead of `HadoopCatalog`, avoiding Hadoop dependencies and security manager issues. This is a metadata-pointer approach -- it needs the exact metadata file path.

### `findLatestMetadataFile(String tablePath, FileIO fileIO)` (lines 75-118)

Two-phase discovery:
1. **Phase 1** (lines 83-100): Reads `version-hint.text` to get the current version number, then checks `v{N}.metadata.json`.
2. **Phase 2** (lines 104-115): Fallback brute-force scan from v100 down to v1, checking `inputFile.exists()` for each.

**Limitation**: Brute-force scan up to v100 only. Production Iceberg tables with more than 100 metadata versions would fail. The code itself notes: "For production, consider using a catalog that tracks the current metadata location."

### `extractVersionNumber(String path)` (lines 127-142)

Static helper that parses version number from `vN.metadata.json` filenames. Used only in tests.

### Tests

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/test/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapterTests.java`

14 unit tests, all for `extractVersionNumber()`. No tests for `resolveTable()` or `findLatestMetadataFile()` (those require S3 connectivity). Lines 20-122.

---

## 2. IcebergSourceOperatorFactory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`

### The critical blocker: `get()` throws UnsupportedOperationException

Lines 98-107:
```java
@Override
public SourceOperator get(DriverContext driverContext) {
    // TODO: Implement async source operator creation
    throw new UnsupportedOperationException(
        "Direct Iceberg source operator creation is not yet supported. "
            + "Use the generic async operator factory via OperatorFactoryRegistry."
    );
}
```

This is the single most important gap. The factory implements `SourceOperator.SourceOperatorFactory` but cannot actually create operators.

### Constructor (lines 75-95)

Accepts: `Executor`, `tablePath` (String), `S3Configuration`, `sourceType`, `Expression` (Iceberg filter), `Schema` (Iceberg), `List<Attribute>`, `pageSize`, `maxBufferSize`.

### `createDataSupplier()` (lines 113-121)

Private method returning a `Supplier<CloseableIterable<VectorSchemaRoot>>`. Lazy wrapper around `createIcebergTableReader()`.

### `createIcebergTableReader()` (lines 127-174) -- THE READING LOGIC

This method contains the complete data reading pipeline, but it is never called from production code:

1. **Table recreation** (lines 130-135): Re-resolves the table from metadata via `IcebergCatalogAdapter.resolveTable()`, creates a new `S3FileIO` and `BaseTable`.
2. **Scan setup** (line 138): `table.newScan().planWith(EsExecutors.DIRECT_EXECUTOR_SERVICE)` -- uses direct executor to avoid Iceberg's default ThreadPool.
3. **Filter application** (lines 140-142): If `filter != null`, applies it to the scan.
4. **Column projection** (lines 145-151): Projects only needed columns from `attributes`.
5. **File planning** (line 154): `scan.planFiles()` returns `CloseableIterable<FileScanTask>`.
6. **CombinedScanTask conversion** (lines 157-160): Wraps each `FileScanTask` as a single `CombinedScanTask`.
7. **ArrowReader creation** (line 164): `new ArrowReader(scan, pageSize, false)` -- Iceberg's vectorized reader.
8. **Batch iterator** (line 170): `arrowReader.open(tasks)` returns `CloseableIterator<ColumnarBatch>`.
9. **VectorSchemaRoot wrapping** (line 173): Returns `ColumnarBatchToVectorSchemaRootIterable` adapter.

### `ColumnarBatchToVectorSchemaRootIterable` (lines 185-259)

Private inner class that adapts Iceberg's `ColumnarBatch` iterator to Arrow's `VectorSchemaRoot` iterator:
- `convertColumnarBatchToVectorSchemaRoot()` (lines 242-258): Extracts `FieldVector` from each `ColumnVector` via `columnVector.getFieldVector()`, creates a new `VectorSchemaRoot(fieldVectors)`.

**Important**: This produces `VectorSchemaRoot` objects that would need to be converted to ESQL `Page`/`Block` via `ArrowToBlockConverter`. That conversion step is NOT implemented in this class.

### ArrowReader provenance

The `ArrowReader` import is `org.apache.iceberg.arrow.vectorized.ArrowReader` (line 18) -- from the `iceberg-arrow` library. This is Iceberg's native vectorized reader that reads Parquet data files into Arrow columnar format. It is NOT the Arrow IPC `ArrowReader`.

The dependency is declared in `build.gradle` line 81: `org.apache.iceberg:iceberg-arrow:${versions.iceberg}`.

---

## 3. IcebergPushdownFilters

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`

### `convert(Expression esqlExpr)` (lines 55-138)

Static method converting ESQL expressions to `org.apache.iceberg.expressions.Expression`. Returns `null` for unsupported expressions.

**Supported expression types**:

| ESQL Expression | Iceberg Expression | Lines |
|---|---|---|
| `Equals` | `equal(field, value)` | 62 |
| `NotEquals` | `notEqual(field, value)` | 63 |
| `LessThan` | `lessThan(field, value)` | 64 |
| `LessThanOrEqual` | `lessThanOrEqual(field, value)` | 65 |
| `GreaterThan` | `greaterThan(field, value)` | 66 |
| `GreaterThanOrEqual` | `greaterThanOrEqual(field, value)` | 67 |
| `In` | `in(field, values)` | 72-83 |
| `IsNull` | `isNull(field)` | 86-88 |
| `IsNotNull` | `notNull(field)` | 91-93 |
| `Range` (lower op field op upper) | `and(greaterThan[OrEqual], lessThan[OrEqual])` | 96-111 |
| `And` | `and(left, right)` | 115-125 |
| `Or` | `or(left, right)` | 115-125 |
| `Not` | `not(inner)` | 129-135 |

**Requirements for pushdown**: Left side must be `NamedExpression`, right side must be `foldable()` (literal). For `In`, all values must be foldable.

### `convertValue(Object value)` (lines 140-142)

**Critical bug**: Always converts to String via `BytesRefs.toString(value)`. This means integer 25 becomes string "25" in the Iceberg filter, which will produce incorrect results or errors for non-string Iceberg columns. Numeric and boolean types need type-aware conversion.

### NOT wired into FilterPushdownSupport

`IcebergPushdownFilters.convert()` exists as a standalone utility. It is NOT connected to the `FilterPushdownSupport` SPI interface. The `TableCatalog` interface's `filterPushdownSupport()` default returns `null` (line 57-59 of `TableCatalog.java`), and `IcebergTableCatalog` does not override it.

### Tests

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/test/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFiltersTests.java`

24 unit tests (lines 42-394). Covers all expression types plus edge cases: unsupported expressions, non-foldable values, AND/OR with unsupported children, nested expressions, BytesRef conversion.

---

## 4. ArrowReader Usage in Iceberg

The `ArrowReader` is `org.apache.iceberg.arrow.vectorized.ArrowReader` (imported at line 18 of `IcebergSourceOperatorFactory.java`). This is from Iceberg's `iceberg-arrow` module.

**What it does**: Reads Parquet data files referenced by Iceberg table metadata into Arrow columnar batches (`ColumnarBatch`). Each `ColumnarBatch` contains `ColumnVector` objects that wrap Arrow `FieldVector` instances.

**Data flow**:
```
Iceberg Table Scan
  -> FileScanTask (individual Parquet files)
  -> CombinedScanTask (grouped)
  -> ArrowReader.open(tasks) -> Iterator<ColumnarBatch>
  -> ColumnarBatch.column(i).getFieldVector() -> Arrow FieldVector
  -> VectorSchemaRoot (Arrow's in-memory table)
  [MISSING] -> ArrowToBlockConverter -> ESQL Page/Block
```

**Dependency chain** (`build.gradle` lines 81-98):
- `iceberg-arrow:${versions.iceberg}` -- with Arrow transitive deps excluded (provided by x-pack-esql's arrow module at runtime)
- `iceberg-parquet:${versions.iceberg}` -- with parquet-hadoop excluded (uses `parquet-hadoop-bundle` instead)
- Arrow vectors/memory provided via `compileOnly` deps (lines 115-116)

---

## 5. ArrowToBlockConverter

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java`

### Architecture

Abstract base class with static inner classes for each Arrow type. The `convert(FieldVector, BlockFactory)` method converts an Arrow vector to an ESQL Block.

### Conversion approach: Row-by-row with null checking

Every converter follows the same pattern (example from `FromFloat64`, lines 113-130):
```java
Float8Vector f8v = (Float8Vector) vector;
int valueCount = f8v.getValueCount();
try (DoubleBlock.Builder builder = factory.newDoubleBlockBuilder(valueCount)) {
    for (int i = 0; i < valueCount; i++) {
        if (f8v.isNull(i)) {
            builder.appendNull();
        } else {
            builder.appendDouble(f8v.get(i));
        }
    }
    return builder.build();
}
```

**Not vectorized**: Each value is checked for null and appended individually. A future optimization could use bulk copy from Arrow buffers to Block arrays.

### Type mapping (lines 70-83)

| Arrow MinorType | ESQL Block Type | Converter Class |
|---|---|---|
| FLOAT4 | DoubleBlock (float->double promotion) | `FromFloat32` |
| FLOAT8 | DoubleBlock | `FromFloat64` |
| BIGINT | LongBlock | `FromInt64` |
| INT | IntBlock | `FromInt32` |
| BIT | BooleanBlock | `FromBoolean` |
| VARCHAR | BytesRefBlock | `FromVarChar` |
| VARBINARY | BytesRefBlock | `FromVarBinary` |
| TIMESTAMPMICRO | LongBlock (micros/1000) | `FromTimestampMicro` |
| TIMESTAMPMICROTZ | LongBlock (micros/1000) | `FromTimestampMicroTZ` |

**Not supported**: DECIMAL, DATE32, DATE64, LIST, STRUCT, MAP, SMALLINT, TINYINT.

**Timestamp conversion**: Arrow stores timestamps as microseconds; ESQL stores datetime as milliseconds. The converter divides by 1000 (lines 265-266, 290-291).

### `forType(Types.MinorType)` factory method (lines 70-83)

Returns the appropriate converter subclass, or `null` for unsupported types.

### Tests

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/test/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverterTests.java`

11 test methods covering: Float64, Int64, Int32, Boolean, VarChar, VarBinary, all-nulls, empty vector, large vector (10k rows), type factory, and round-trip symmetric conversion. Lines 34-314.

---

## 6. IcebergTableCatalog

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`

Implements `TableCatalog` interface. This is the actual SPI registration point.

### `catalogType()` (line 37): Returns `"iceberg"`

### `canHandle(String path)` (lines 41-44)

Only accepts paths starting with `s3://`, `s3a://`, or `s3n://`. Does NOT check for Iceberg metadata directory presence.

### `metadata(String tablePath, Map<String, Object> config)` (lines 48-56)

Delegates to `IcebergCatalogAdapter.resolveTable()`, wraps result in `IcebergSourceMetadata` (private inner class, lines 156-177). This works end-to-end for schema discovery.

### `planScan(String tablePath, Map<String, Object> config, List<Object> predicates)` (lines 59-93)

**Implemented but disconnected**:
1. Resolves table metadata via `IcebergCatalogAdapter.resolveTable()` (line 65)
2. Creates S3FileIO and Table (lines 68-70)
3. Creates a `TableScan` (line 73)
4. Does NOT apply predicates (line 76-77: "For now, we don't apply predicates at the scan planning level")
5. Iterates `scan.planFiles()` and wraps each `FileScanTask` as an `IcebergDataFile` (lines 81-84)

The `IcebergDataFile` inner class (lines 119-151) implements `TableCatalog.DataFile` with `path()`, `format()`, `sizeInBytes()`, `recordCount()`, `partitionValues()` (returns empty map).

**The gap**: `planScan()` produces `DataFile` objects, but nothing in the framework consumes `DataFile`. The framework's `SplitProvider` interface produces `ExternalSplit` objects, not `DataFile` objects. These two concepts are not connected.

### `close()` (lines 96-98): No-op.

---

## 7. TableCatalog Interface

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/TableCatalog.java`

Extends `ExternalSourceFactory` and `Closeable`.

### Methods added on top of ExternalSourceFactory

| Method | Signature | Description |
|---|---|---|
| `catalogType()` | `String catalogType()` | Type identifier (e.g., "iceberg") |
| `canHandle(path)` | `boolean canHandle(String path)` | Whether this catalog can handle the path |
| `metadata(path, config)` | `SourceMetadata metadata(String, Map)` | Resolve schema metadata |
| `planScan(path, config, predicates)` | `List<DataFile> planScan(String, Map, List)` | Plan which files to read |

### Default implementations from ExternalSourceFactory

| Method | Default | Line |
|---|---|---|
| `type()` | delegates to `catalogType()` | 43-45 |
| `resolveMetadata()` | delegates to `metadata()` | 48-54 |
| `filterPushdownSupport()` | returns `null` | 57-59 |
| `operatorFactory()` | returns `null` | 62-64 |
| `splitProvider()` | inherited: `SplitProvider.SINGLE` | (from ExternalSourceFactory line 37) |

### DataFile inner interface (lines 66-76)

```java
interface DataFile {
    String path();
    String format();
    long sizeInBytes();
    long recordCount();
    Map<String, Object> partitionValues();
}
```

---

## 8. IcebergDataSourcePlugin

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergDataSourcePlugin.java`

Minimal plugin class:

- Extends `Plugin` and implements `DataSourcePlugin` (line 39)
- `supportedCatalogs()` returns `Set.of("iceberg")` (line 43)
- `tableCatalogs(Settings)` returns `Map.of("iceberg", s -> new IcebergTableCatalog())` (line 48)

**Does NOT provide**: `operatorFactories()`, `filterPushdownSupport()`, `sourceFactories()`, `storageProviders()`, `formatReaders()`, `splitProvider()`.

**SPI registration**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/resources/META-INF/services/org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin`

---

## 9. Integration Tests

### IcebergSpecIT

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/qa/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/iceberg/IcebergSpecIT.java`

**Disabled**: `@AwaitsFix(bugUrl = "Iceberg integration tests disabled pending stabilization")` (line 29)

**Bug URL**: The string `"Iceberg integration tests disabled pending stabilization"` is the bug URL. There is NO GitHub issue number -- it is a placeholder string.

The test class loads `iceberg-*.csv-spec` files from the classpath. However, **no csv-spec files exist** in the source tree (confirmed by glob search). The test would find zero spec files and would fail even if enabled.

### InteractiveFixtureManual

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/qa/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/iceberg/InteractiveFixtureManual.java`

Also `@AwaitsFix(bugUrl = "Iceberg integration tests disabled pending stabilization")` (line 77).

Manual interactive test tool that starts S3HttpFixture + Elasticsearch cluster and waits for manual curl queries. Not an automated test.

### IcebergSpecTestCase (base class)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/qa/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/iceberg/IcebergSpecTestCase.java`

Extends `AbstractExternalSourceSpecTestCase` with `StorageBackend.S3` and format `"iceberg"`.

Has helper methods `verifyIcebergMetadataUsed()` and `wasIcebergMetadataUsed()` for checking S3 request logs.

### Test fixtures

Located at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/qa/src/javaRestTest/resources/iceberg-fixtures/`:

```
employees/
  data/data.parquet
  metadata/
    v1.metadata.json
    v2.metadata.json
    version-hint.text
    snap-5740414668264810322-1-5947ebd2-0430-4fde-9a42-1b6a58c11c6b.avro
    5947ebd2-0430-4fde-9a42-1b6a58c11c6b-m0.avro
standalone/
  employees.parquet
```

A valid Iceberg table with two metadata versions, one snapshot, one manifest list, one manifest, and one data file.

---

## 10. What Exactly Needs to Be Wired

Based on thorough code analysis, here are the specific disconnections:

### Gap 1: `operatorFactory()` returns null

**Where**: `TableCatalog.operatorFactory()` defaults to `null` (line 62-64 of `TableCatalog.java`). `IcebergTableCatalog` does NOT override it.

**Impact**: When `OperatorFactoryRegistry.factory()` receives sourceType="iceberg", it finds the `LazyTableCatalogWrapper`, calls `sf.operatorFactory()`, gets `null`, then falls through to `pluginFactories` (also empty for iceberg), and throws `IllegalArgumentException("No operator factory for sourceType: iceberg")`.

**Fix**: `IcebergTableCatalog` (or `IcebergDataSourcePlugin`) must provide a `SourceOperatorFactoryProvider` that creates a working `SourceOperator.SourceOperatorFactory`. The `IcebergSourceOperatorFactory` exists with the reading logic in `createIcebergTableReader()`, but its `get()` throws `UnsupportedOperationException`.

### Gap 2: No bridge from `VectorSchemaRoot` to ESQL `Page`

**Where**: `IcebergSourceOperatorFactory.createIcebergTableReader()` produces `CloseableIterable<VectorSchemaRoot>`. The `ArrowToBlockConverter` exists in a separate module (`esql/arrow/`). Nobody connects them.

**Fix**: Create a bridge that:
1. Iterates `VectorSchemaRoot` batches from `createIcebergTableReader()`
2. For each batch, calls `ArrowToBlockConverter.forType()` per column
3. Converts each `FieldVector` to a `Block`
4. Assembles `Block[]` into a `Page`
5. Feeds pages into the async buffer

This should likely be implemented as a new operator factory that follows the `AsyncExternalSourceOperatorFactory` pattern (background thread producing pages into `AsyncExternalSourceBuffer`).

### Gap 3: `splitProvider()` returns `SINGLE`

**Where**: `ExternalSourceFactory.splitProvider()` defaults to `SplitProvider.SINGLE` (line 37). `TableCatalog` inherits this. `IcebergTableCatalog` does NOT override it.

**Impact**: Iceberg tables always run as a single split on the coordinator node. No distributed execution across data nodes.

**Fix**: Implement an `IcebergSplitProvider` that calls `IcebergTableCatalog.planScan()` and converts each `DataFile` into an `ExternalSplit`. This enables the `SplitDiscoveryPhase` -> `AdaptiveStrategy` -> `DataNodeComputeHandler` distributed execution path.

### Gap 4: `planScan()` returns `DataFile` but framework needs `ExternalSplit`

**Where**: `TableCatalog.planScan()` returns `List<DataFile>` (line 40). The split discovery framework works with `ExternalSplit` (from `SplitProvider.discoverSplits()`). These types are unrelated.

**Fix**: Create an `IcebergExternalSplit` that implements `ExternalSplit` and wraps the information from `IcebergDataFile` (file path, format, size, record count, partition values). The `IcebergSplitProvider.discoverSplits()` calls `planScan()` and maps each `DataFile` to an `IcebergExternalSplit`.

### Gap 5: `filterPushdownSupport()` returns null

**Where**: `TableCatalog.filterPushdownSupport()` defaults to `null`. `IcebergPushdownFilters.convert()` exists but is not wrapped in a `FilterPushdownSupport` implementation.

**Fix**: Create `IcebergFilterPushdownSupport implements FilterPushdownSupport`:
- `pushFilters(List<Expression>)` iterates expressions, calls `IcebergPushdownFilters.convert()` on each
- Expressions that convert successfully go into `pushedFilter` (as `org.apache.iceberg.expressions.Expression`)
- Expressions that return `null` go into `remainder`
- Override `filterPushdownSupport()` in `IcebergTableCatalog` to return the new implementation

### Gap 6: `convertValue()` always converts to String

**Where**: `IcebergPushdownFilters.convertValue()` (line 140-142) always calls `BytesRefs.toString(value)`.

**Impact**: Numeric and boolean comparisons will produce Iceberg filter expressions with String values. For example, `WHERE age > 25` would produce `greaterThan("age", "25")` instead of `greaterThan("age", 25)`. Iceberg will either error or produce wrong results.

**Fix**: Type-aware conversion that preserves Integer, Long, Double, Boolean, etc. Only convert `BytesRef` to String.

### Gap 7: No csv-spec test files

**Where**: `IcebergSpecIT.readScriptSpec()` (line 54) loads `iceberg-*.csv-spec` from classpath, but no such files exist.

**Fix**: Create `iceberg-basic.csv-spec` in `qa/src/javaRestTest/resources/` with test cases like:
```
icebergBasic
EXTERNAL "s3://test-bucket/warehouse/employees" | LIMIT 10;
emp_no:integer | ...
```

### Gap 8: S3-only (no GCS/Azure/HTTP)

**Where**: `IcebergTableCatalog.canHandle()` (line 44) only accepts `s3://`, `s3a://`, `s3n://`. `IcebergCatalogAdapter` uses `S3FileIO` directly. `S3Configuration` is S3-specific.

**Impact**: Iceberg tables on GCS, Azure, or HTTP are not supported.

**Fix (not for MVP)**: Abstract `FileIO` creation behind the storage provider registry, or add GCS/Azure `FileIO` implementations.

---

## 11. Implementation Path (ordered by dependency)

### Phase 1: Minimal read path (enables end-to-end query execution)

1. **Fix `convertValue()` type coercion** in `IcebergPushdownFilters` -- 1 hour
2. **Create `IcebergFilterPushdownSupport`** implementing `FilterPushdownSupport`, wrapping `IcebergPushdownFilters.convert()` -- 2 hours
3. **Create `IcebergOperatorFactory`** (new class) implementing `SourceOperatorFactoryProvider` that:
   - Receives `SourceOperatorContext` with sourceType, path, attributes, config, pushedFilter
   - Uses `createIcebergTableReader()` logic from `IcebergSourceOperatorFactory`
   - Converts `VectorSchemaRoot` batches to ESQL Pages via `ArrowToBlockConverter`
   - Wraps in `AsyncExternalSourceBuffer`/`AsyncExternalSourceOperator` pattern
   - This is the largest piece -- ~3-4 days
4. **Wire `operatorFactory()` and `filterPushdownSupport()`** in `IcebergTableCatalog` -- 1 hour
5. **Write csv-spec test files** and enable `IcebergSpecIT` -- 1 day
6. **Remove `@AwaitsFix`** from integration tests -- trivial

### Phase 2: Distributed execution

7. **Create `IcebergExternalSplit`** implementing `ExternalSplit` -- 2 hours
8. **Create `IcebergSplitProvider`** implementing `SplitProvider` that calls `planScan()` -> `DataFile` -> `ExternalSplit` -- 4 hours
9. **Wire `splitProvider()`** override in `IcebergTableCatalog` -- 1 hour
10. **Pass split info to operator factory** so each data node reads its assigned files -- 4 hours

### Phase 3: Production hardening

11. **Metadata version fallback** -- handle tables with >100 metadata versions (use catalog API instead of brute-force)
12. **Connection pooling** -- reuse `S3FileIO` across scans
13. **Memory management** -- integrate Arrow allocator with ESQL circuit breaker
14. **Error handling** -- graceful handling of missing/corrupt metadata, permission errors

---

## 12. IcebergTableMetadata

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableMetadata.java`

Implements `ExternalSourceMetadata`. Converts Iceberg `Schema` to ESQL `Attribute` list.

### Type mapping (`mapPrimitiveType`, lines 91-117)

| Iceberg Type | ESQL DataType |
|---|---|
| BOOLEAN | BOOLEAN |
| INTEGER | INTEGER |
| LONG | LONG |
| FLOAT | DOUBLE |
| DOUBLE | DOUBLE |
| STRING | KEYWORD |
| TIMESTAMP | DATETIME |
| DATE | DATETIME |
| BINARY/FIXED | KEYWORD |
| DECIMAL | DOUBLE |

LIST types unwrap to element type (lines 77-82). MAP and STRUCT return UNSUPPORTED and are filtered out (lines 57-58).

### Tests

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/test/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableMetadataTests.java`

24 tests covering all type mappings, multi-column schemas, accessors, equals/hashCode, and toString. Lines 22-296.

---

## 13. Supporting Classes

### S3Configuration

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/S3Configuration.java`

Parses access_key, secret_key, endpoint, region from ESQL query parameters. Has `fromParams(Map<String, Expression>)` for query-time and `fromFields(String...)` for deserialization. 22 unit tests.

### S3FileIOFactory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/S3FileIOFactory.java`

Creates `S3FileIO` with:
- Empty AWS profile to prevent reading `~/.aws/credentials` (Elasticsearch entitlement violations) (lines 68-79)
- `UrlConnectionHttpClient` instead of Apache HTTP client (to avoid daemon thread entitlement violations) (line 109)
- Path-style access enabled for MinIO/LocalStack/fixture compatibility (line 105)
- Hardcoded test credentials fallback when no config provided (lines 87-91)

---

## 14. Summary: Current State vs Target State

| Capability | Current State | Target State |
|---|---|---|
| Schema discovery | Working (resolveTable -> metadata) | Working (no change needed) |
| Data reading | Code exists but `get()` throws | Working operator factory |
| Arrow -> ESQL conversion | `ArrowToBlockConverter` exists | Wired into operator factory |
| Filter pushdown | `IcebergPushdownFilters.convert()` exists | Wired via `FilterPushdownSupport` |
| Split discovery | `planScan()` returns `DataFile` list | Wired via `SplitProvider` |
| Distributed execution | `SINGLE` (coordinator only) | Per-file splits across data nodes |
| Integration tests | Disabled, no csv-spec files | Enabled with test cases |
| Storage backends | S3 only | S3 (GCS/Azure post-MVP) |

**Bottom line**: The Iceberg plugin has ~70% of the code written. Schema discovery works end-to-end. The ArrowReader-based data reading pipeline is fully implemented in `createIcebergTableReader()`. The pushdown filter converter handles all major expression types. What is missing is the "glue" -- connecting these pieces through the SPI interfaces (`operatorFactory()`, `filterPushdownSupport()`, `splitProvider()`), converting Arrow VectorSchemaRoot to ESQL Pages, and enabling the integration tests.
