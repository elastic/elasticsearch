# Deep Dive: Parquet Row-Group Filter Pushdown

## Summary

Parquet files organize data into **row groups** (typically 128 MB each), and each row group stores per-column **min/max statistics**. Row-group filter pushdown uses these statistics to skip entire row groups whose value ranges prove the filter predicate cannot match any rows. This is a pure metadata operation -- no data bytes are read for skipped row groups.

The existing ESQL codebase has a complete filter pushdown SPI (`FilterPushdownSupport`), an optimizer rule (`PushFiltersToSource`), and a registry (`FilterPushdownRegistry`) -- but none of these are wired up for Parquet. The Parquet reader (`ParquetFormatReader`) reads row groups unconditionally via `readNextRowGroup()` with no filtering. This document maps every piece of the existing infrastructure and identifies exactly what needs to change.

---

## 1. FilterPushdownSupport Interface

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`

### Methods

| Method | Signature | Lines |
|--------|-----------|-------|
| `pushFilters` | `PushdownResult pushFilters(List<Expression> filters)` | 46 |
| `canPush` | `default Pushability canPush(Expression expr)` | 57-59 |

### Enums and Records

- **`Pushability`** (lines 64-84): Three-valued enum:
  - `YES` -- expression fully pushed, removed from FilterExec
  - `NO` -- expression cannot be pushed, remains in FilterExec
  - `RECHECK` -- expression used for optimization (e.g., partition pruning) but must remain in FilterExec for correctness

- **`PushdownResult`** (lines 92-127): Record with:
  - `Object pushedFilter` -- opaque source-specific filter (null if nothing pushed)
  - `List<Expression> remainder` -- expressions that could not be pushed
  - Factory methods: `none(List<Expression> all)`, `all(Object pushedFilter)`
  - Predicate methods: `hasPushedFilter()`, `hasRemainder()`

### Registration Path

`FilterPushdownSupport` is obtained from `ExternalSourceFactory.filterPushdownSupport()` (line 28 of `ExternalSourceFactory.java`). Each `DataSourcePlugin` can also return `Map<String, FilterPushdownSupport>` from `filterPushdownSupport(Settings)` (line 121 of `DataSourcePlugin.java`). The `DataSourceModule` iterates over source factories and collects non-null `filterPushdownSupport()` results into the `FilterPushdownRegistry` (lines 215-227 of `DataSourceModule.java`).

### Key Design Note

The pushed filter is **opaque** (`Object`) and is **never serialized**. External sources execute on the coordinator only (`ExecutesOn.Coordinator`), so the filter is created during local physical optimization and consumed immediately by the operator factory in the same JVM. This means the pushed filter type can be anything -- a parquet-mr `FilterPredicate`, a list of ESQL expressions, or a custom predicate object.

---

## 2. PushFiltersToSource Optimizer Rule

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

### Structure

This rule extends `PhysicalOptimizerRules.ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext>` (line 45).

The `rule()` method (lines 48-58) dispatches based on the child of `FilterExec`:

1. **`EsQueryExec`** (line 50): Lucene pushdown path -- translates to QueryBuilder
2. **`EvalExec -> EsQueryExec`** (line 52): Lucene pushdown through eval aliases
3. **`ExternalSourceExec`** (line 54): External source pushdown path -- uses `FilterPushdownRegistry`

### External Source Pushdown (lines 241-286)

The method `planFilterExecForExternalSource()` works as follows:

1. **Lookup** (line 247): `registry.get(externalExec.sourceType())` -- looks up `FilterPushdownSupport` by the source type string (e.g., `"parquet"`, `"iceberg"`)
2. **Split filters** (line 254): `splitAnd(filterExec.condition())` -- breaks the filter condition into AND-separated expressions
3. **Push** (line 257): `pushdownSupport.pushFilters(filters)` -- delegates to the source-specific implementation
4. **Rewrite** (lines 259-285):
   - If pushed filter exists, creates new `ExternalSourceExec` via `externalExec.withPushedFilter(combinedFilter)` (line 273)
   - If remainder exists, wraps in new `FilterExec` with remainder expressions (line 277)
   - If all filters pushed, removes FilterExec entirely (line 280)

### How Context Carries the Registry

The `LocalPhysicalOptimizerContext` record (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalOptimizerContext.java`, lines 17-37) carries `FilterPushdownRegistry filterPushdownRegistry` as a field. The rule accesses it via `ctx.filterPushdownRegistry()` on line 55.

---

## 3. FilterPushdownRegistry

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FilterPushdownRegistry.java`

### Implementation

A simple wrapper around `Map<String, FilterPushdownSupport>`:

```java
public class FilterPushdownRegistry {
    private final Map<String, FilterPushdownSupport> pushdownSupport;  // line 26

    public FilterPushdownSupport get(String sourceType) {              // line 32
        return sourceType != null ? pushdownSupport.get(sourceType) : null;
    }

    public boolean hasSupport(String sourceType) {                     // line 36
        return sourceType != null && pushdownSupport.containsKey(sourceType);
    }

    public static FilterPushdownRegistry empty() {                     // line 40
        return new FilterPushdownRegistry(Map.of());
    }
}
```

### Population

In `DataSourceModule` constructor (lines 215-227), the registry is built by iterating over `sourceFactories` entries and calling `factory.filterPushdownSupport()`. Lazy wrappers (`LazyConnectorFactory`, `LazyTableCatalogWrapper`) are explicitly skipped to avoid triggering class loading.

The `FileSourceFactory` (which handles all file-based sources including Parquet) does **not** override `filterPushdownSupport()` -- it inherits the default `null` from `ExternalSourceFactory` (line 28 of `ExternalSourceFactory.java`). This is why Parquet currently has no pushdown.

---

## 4. IcebergPushdownFilters (Reference Implementation)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`

### Conversion Logic

The `convert(Expression esqlExpr)` method (lines 55-138) converts ESQL expressions to Iceberg `org.apache.iceberg.expressions.Expression` objects. It handles:

| ESQL Expression Type | Iceberg Output | Lines |
|---------------------|----------------|-------|
| `Equals` | `equal(fieldName, value)` | 62 |
| `NotEquals` | `notEqual(fieldName, value)` | 63 |
| `LessThan` | `lessThan(fieldName, value)` | 64 |
| `LessThanOrEqual` | `lessThanOrEqual(fieldName, value)` | 65 |
| `GreaterThan` | `greaterThan(fieldName, value)` | 66 |
| `GreaterThanOrEqual` | `greaterThanOrEqual(fieldName, value)` | 67 |
| `In` | `in(fieldName, values)` | 72-83 |
| `IsNull` | `isNull(fieldName)` | 87 |
| `IsNotNull` | `notNull(fieldName)` | 92 |
| `Range` | `and(lower_bound, upper_bound)` | 96-112 |
| `And` | `and(left, right)` | 120 |
| `Or` | `or(left, right)` | 121 |
| `Not` | `not(inner)` | 132 |

### Constraints

- Binary comparisons require left side to be `NamedExpression` and right side to be `foldable()` (line 57)
- `In` requires all list elements to be foldable (line 77)
- Returns `null` for unsupported expressions (propagated up through logical operators)
- Value conversion uses `BytesRefs.toString()` (line 141) -- converts all values to strings

### Tests

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/test/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFiltersTests.java`

**22 test methods** covering:
- All 6 comparison operators (Equals, NotEquals, LT, LTE, GT, GTE) -- lines 46-136
- IsNull, IsNotNull -- lines 138-160
- In -- lines 162-176
- Range (inclusive both, exclusive both) -- lines 178-208
- Logical operators (And, Or, Not) -- lines 210-264
- Nested expressions (And+Or) -- lines 266-288
- Null returns for unsupported expressions -- lines 290-330
- Non-foldable values -- lines 332-357
- BytesRef value conversion -- lines 359-369

---

## 5. ExternalSourceExec -- pushedFilter Field

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

### Declaration

```java
private final Object pushedFilter; // Opaque filter - NOT serialized (coordinator only)  // line 63
```

### How It Is Set

1. **Constructor** (line 109): `this.pushedFilter = pushedFilter;`
2. **`withPushedFilter(Object newFilter)`** (lines 239-252): Creates a new `ExternalSourceExec` with the given filter, preserving all other fields
3. **Serialization** (line 161-162): When read from stream, `pushedFilter` is always `null` -- it is NOT serialized
4. **Write** (lines 169-179): `pushedFilter` is not written to the stream

### How It Flows to Execution

In `LocalExecutionPlanner.planExternalSource()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java`, lines 1223-1278):

```java
SourceOperatorContext operatorContext = SourceOperatorContext.builder()
    ...
    .pushedFilter(externalSource.pushedFilter())   // line 1270
    ...
    .build();
```

The `SourceOperatorContext` record (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java`) carries `Object pushedFilter` (line 51) and passes it to whatever `SourceOperatorFactoryProvider` creates the actual operator.

### Current Gap for Parquet

The `FileSourceFactory.operatorFactory()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`, lines 118-151) creates an `AsyncExternalSourceOperatorFactory` but does **not** pass `context.pushedFilter()` to it. The `AsyncExternalSourceOperatorFactory` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`) has no `pushedFilter` field at all -- it receives `FormatReader` and `StorageProvider` but no filter.

---

## 6. ParquetFormatReader / ParquetPageIterator -- Current Row Group Reading

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

### Schema Discovery (lines 72-86)

Uses `ParquetMetadataConverter.SKIP_ROW_GROUPS` (line 77) to read **only** file-level metadata, not row group metadata. This is efficient for schema discovery.

### Data Reading (lines 89-122)

The `read()` method:
1. Opens `ParquetFileReader.open(parquetInputFile, options)` with **default** `ParquetReadOptions` (line 94) -- no filter
2. Gets the full file schema from `reader.getFileMetaData()` (line 100)
3. Creates a `ParquetPageIterator` (line 121)

### ParquetPageIterator (lines 173-383)

The inner class that actually iterates over row groups:

```java
private static class ParquetPageIterator implements CloseableIterator<Page> {
    private final ParquetFileReader reader;           // line 174
    private PageReadStore currentRowGroup;             // line 181
    private RecordReader<Group> recordReader;           // line 182
    private long rowsRemainingInGroup;                 // line 183
    private boolean exhausted = false;                 // line 184
```

**`hasNext()`** (lines 202-223):
```java
currentRowGroup = reader.readNextRowGroup();   // line 212 -- UNCONDITIONAL
```

This is the critical line. `readNextRowGroup()` reads the **next** row group unconditionally, without any filtering. There is:
- No call to `reader.getRowGroups()` to inspect metadata first
- No call to `reader.skipNextRowGroup()` to skip irrelevant groups
- No call to `reader.readNextFilteredRowGroup()` to use Parquet's built-in filtering
- No access to `BlockMetaData.getColumns()` or `ColumnChunkMetaData.getStatistics()`
- No metadata pre-fetch of any kind

**`next()`** (lines 226-253): Reads records from the current row group in batches of `batchSize`, using Parquet's `GroupRecordConverter` pattern.

### Conversion to ESQL Blocks (lines 255-377)

The `convertToPage()` method iterates over attributes, creating typed Block builders (Boolean, Int, Long, Double, BytesRef). Each row is read individually from `Group` objects, which is the **record-materialization** path rather than the direct columnar path.

---

## 7. Parquet Row Group Statistics -- parquet-mr API

The project uses **parquet-hadoop-bundle 1.16.0** (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/build.gradle`, line 13).

### Available API

The parquet-mr library provides a complete row-group statistics and filtering API:

#### Metadata Access
- `ParquetFileReader.getRowGroups()` returns `List<BlockMetaData>` -- all row group metadata
- `BlockMetaData.getColumns()` returns `List<ColumnChunkMetaData>` -- per-column metadata within a row group
- `BlockMetaData.getRowCount()` -- number of rows in the row group

#### Statistics (on `ColumnChunkMetaData.getStatistics()`)
- `Statistics.genericGetMin()` / `Statistics.genericGetMax()` -- typed min/max values
- `Statistics.getMinBytes()` / `Statistics.getMaxBytes()` -- raw byte min/max
- `Statistics.getNumNulls()` -- null count
- `Statistics.isEmpty()` -- whether stats are present
- `Statistics.hasNonNullValue()` -- whether any non-null values exist
- `Statistics.comparator()` -- proper comparator for the column's logical type
- `Statistics.compareMinToValue(T value)` / `Statistics.compareMaxToValue(T value)` -- direct comparison

#### Built-in Filtering
- `ParquetFileReader.filterRowGroups(FilterPredicate)` -- filters row groups using the predicate
- `ParquetFileReader.readNextFilteredRowGroup()` -- reads only row groups that pass the filter
- `ParquetFileReader.readFilteredRowGroup(int)` -- reads a specific filtered row group
- `ParquetFileReader.skipNextRowGroup()` -- skips a row group without reading data

#### FilterPredicate API
`org.apache.parquet.filter2.predicate.FilterApi` provides builders:
- `FilterApi.eq(column, value)`, `FilterApi.notEq(column, value)`
- `FilterApi.lt(column, value)`, `FilterApi.ltEq(column, value)`
- `FilterApi.gt(column, value)`, `FilterApi.gtEq(column, value)`
- `FilterApi.and(left, right)`, `FilterApi.or(left, right)`, `FilterApi.not(pred)`
- Column references: `FilterApi.intColumn(path)`, `FilterApi.longColumn(path)`, etc.

---

## 8. ParquetDataSourcePlugin

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetDataSourcePlugin.java`

This plugin implements `DataSourcePlugin` and provides:

- `supportedFormats()` (line 41): `Set.of("parquet")`
- `supportedExtensions()` (line 46): `Set.of(".parquet")`
- `formatReaders()` (lines 51-53): Returns a `FormatReaderFactory` that creates `ParquetFormatReader`

**What it does NOT provide:**
- No `filterPushdownSupport()` override -- inherits default empty map from `DataSourcePlugin` (line 121)
- No `operatorFactories()` override
- No `sourceFactories()` override
- No `connectors()` override

The Parquet plugin is a pure format reader -- it provides no filtering, no custom operators, no pushdown support of any kind.

---

## 9. What Exactly Needs to Change

### 9.1. New Class: `ParquetPushdownFilters`

**Location**: `x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetPushdownFilters.java`

This class converts ESQL expressions to parquet-mr `FilterPredicate` objects. It follows the same pattern as `IcebergPushdownFilters` but targets a different output type.

**Conversion table:**

| ESQL Expression | Parquet FilterApi | Notes |
|-----------------|-------------------|-------|
| `Equals(field, literal)` | `FilterApi.eq(column, value)` | Column type depends on DataType |
| `NotEquals(field, literal)` | `FilterApi.notEq(column, value)` | |
| `LessThan(field, literal)` | `FilterApi.lt(column, value)` | |
| `LessThanOrEqual(field, literal)` | `FilterApi.ltEq(column, value)` | |
| `GreaterThan(field, literal)` | `FilterApi.gt(column, value)` | |
| `GreaterThanOrEqual(field, literal)` | `FilterApi.gtEq(column, value)` | |
| `And(left, right)` | `FilterApi.and(left, right)` | Recursive conversion |
| `Or(left, right)` | `FilterApi.or(left, right)` | Recursive conversion |
| `Not(inner)` | `FilterApi.not(inner)` | Recursive conversion |
| `IsNull(field)` | `FilterApi.eq(column, null)` | Parquet treats eq(null) as isNull |
| `IsNotNull(field)` | `FilterApi.notEq(column, null)` | |
| `Range` | `FilterApi.and(lower, upper)` | Decompose to two comparisons |

**Column type mapping** (based on `ParquetFormatReader.convertParquetTypeToEsql()`, lines 149-170):

| ESQL DataType | Parquet FilterApi column | Value type |
|---------------|-------------------------|------------|
| `BOOLEAN` | `FilterApi.booleanColumn(path)` | `Boolean` |
| `INTEGER` | `FilterApi.intColumn(path)` | `Integer` |
| `LONG` / `DATETIME` | `FilterApi.longColumn(path)` | `Long` |
| `DOUBLE` | `FilterApi.doubleColumn(path)` | `Double` |
| `KEYWORD` / `TEXT` | `FilterApi.binaryColumn(path)` | `Binary` |

**Key difference from Iceberg**: Parquet's `FilterApi` is **strongly typed** -- you must use `intColumn()` vs `longColumn()` vs `binaryColumn()` based on the physical Parquet type. The Iceberg API just takes field name strings. This means `ParquetPushdownFilters` needs access to the Parquet schema (`MessageType`) to determine the correct column type for each field.

### 9.2. New Class: `ParquetFilterPushdownSupport`

**Location**: `x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFilterPushdownSupport.java`

Implements `FilterPushdownSupport`:

```java
public class ParquetFilterPushdownSupport implements FilterPushdownSupport {
    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        // For each filter expression:
        //   1. Try to convert via ParquetPushdownFilters.convert()
        //   2. If convertible, add to pushed list
        //   3. If not, add to remainder
        // Combine pushed predicates with FilterApi.and()
        // Return PushdownResult with FilterPredicate as opaque pushed filter
        // Remainder expressions stay in FilterExec
    }

    @Override
    public Pushability canPush(Expression expr) {
        // Row-group statistics are approximate -- they can eliminate row groups
        // but cannot guarantee exact evaluation. However, for comparison predicates
        // on columns with statistics, the semantics are exact: if the row group's
        // max < filter value, no rows can match.
        //
        // Use RECHECK for safety? Or YES since parquet-mr's StatisticsFilter
        // is provably correct for the supported predicate types?
        //
        // Answer: YES for supported types. Parquet's row-group filtering is
        // conservative -- it only skips groups that provably cannot contain
        // matching rows. Rows that pass the row-group filter still need
        // per-row evaluation, but that happens at the record reader level.
        // The FilterExec remainder handles per-row filtering.
        //
        // IMPORTANT: The pushed filter skips row groups. A separate FilterExec
        // must remain for per-row filtering. So the correct answer is RECHECK
        // unless parquet-mr's record-level filtering is also enabled.
    }
}
```

**RECHECK vs YES**: Row-group filter pushdown is a **coarse-grained** optimization. It skips entire row groups but does NOT filter individual rows. Therefore, the pushed expressions should use `RECHECK` pushability -- they are pushed for efficiency (row group skipping) but must remain in FilterExec for per-row correctness. Alternatively, if parquet-mr's row-level `FilterCompat` is also wired in, `YES` could be used, but that is a separate (more complex) change.

### 9.3. Modify `ParquetDataSourcePlugin`

Add `filterPushdownSupport()` override:

```java
@Override
public Map<String, FilterPushdownSupport> filterPushdownSupport(Settings settings) {
    return Map.of("parquet", new ParquetFilterPushdownSupport());
}
```

**But wait**: The current registration path in `DataSourceModule` (lines 215-227) only checks `ExternalSourceFactory.filterPushdownSupport()`, not `DataSourcePlugin.filterPushdownSupport()`. The `FileSourceFactory` handles all file-based sources and does not delegate filter pushdown to format-specific plugins.

This reveals **two options**:

**Option A -- Register via `DataSourcePlugin.filterPushdownSupport()`**: The `DataSourceModule` would need to also iterate over plugins' `filterPushdownSupport()` maps and merge them into the registry. Currently this is NOT done (lines 215-227 only iterate `sourceFactories`). The `DataSourcePlugin.filterPushdownSupport()` method exists (line 121) but its results are never collected. This is a gap.

**Option B -- Register via `FileSourceFactory`**: Modify `FileSourceFactory` to accept format-specific pushdown support and expose it from its `filterPushdownSupport()` method. The `FileSourceFactory` would need a `Map<String, FilterPushdownSupport>` keyed by format name and return the appropriate one based on the source type.

Option A is simpler and consistent with the existing SPI design.

### 9.4. Modify `ParquetFormatReader.read()` to Accept a Filter

The `FormatReader.read()` interface (line 58 of `FormatReader.java`) currently has no filter parameter:

```java
CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;
```

**Two approaches:**

**Approach 1 -- Extend `FormatReader` with a filtered read method**:
```java
default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, Object filter) throws IOException {
    return read(object, projectedColumns, batchSize); // default ignores filter
}
```

**Approach 2 -- Pass filter through `AsyncExternalSourceOperatorFactory`**: The `FileSourceFactory.operatorFactory()` already receives `SourceOperatorContext` which has `pushedFilter()`. It could pass this to the `AsyncExternalSourceOperatorFactory`, which would then pass it to `FormatReader.read()`.

Approach 2 is preferred because it uses the existing plumbing without changing the SPI.

### 9.5. Modify `ParquetPageIterator` to Use `readNextFilteredRowGroup()`

In `ParquetPageIterator.hasNext()` (lines 202-223), change:

**Current** (line 212):
```java
currentRowGroup = reader.readNextRowGroup();
```

**New**:
```java
// If filter was set during reader construction:
currentRowGroup = reader.readNextFilteredRowGroup();
// Falls back to readNextRowGroup() behavior if no filter was configured
```

The filter must be set on the `ParquetFileReader` before iteration begins. This is done by calling `reader.filterRowGroups(filterPredicate)` after opening the reader but before the first `readNextFilteredRowGroup()` call.

Alternatively, the `ParquetReadOptions.Builder` can accept a `RecordFilter` via `withRecordFilter(FilterCompat.get(filterPredicate))`, which enables both row-group-level and record-level filtering.

### 9.6. Wire pushedFilter Through to AsyncExternalSourceOperatorFactory

The `FileSourceFactory.operatorFactory()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`, lines 118-151) needs to pass `context.pushedFilter()` through to the format reader. The `AsyncExternalSourceOperatorFactory` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`) would need a new `pushedFilter` field and pass it to `formatReader.read()`.

### 9.7. Summary of Changes

| Component | File | Change Type |
|-----------|------|-------------|
| `ParquetPushdownFilters` | `esql-datasource-parquet/.../ParquetPushdownFilters.java` | NEW -- expression converter |
| `ParquetFilterPushdownSupport` | `esql-datasource-parquet/.../ParquetFilterPushdownSupport.java` | NEW -- SPI implementation |
| `ParquetDataSourcePlugin` | `esql-datasource-parquet/.../ParquetDataSourcePlugin.java` | MODIFY -- add `filterPushdownSupport()` |
| `ParquetFormatReader` | `esql-datasource-parquet/.../ParquetFormatReader.java` | MODIFY -- accept filter, use `filterRowGroups()` + `readNextFilteredRowGroup()` |
| `DataSourceModule` | `esql/.../DataSourceModule.java` | MODIFY -- collect plugin `filterPushdownSupport()` into registry |
| `FileSourceFactory` | `esql/.../FileSourceFactory.java` | MODIFY -- pass `pushedFilter` to operator factory |
| `AsyncExternalSourceOperatorFactory` | `esql/.../AsyncExternalSourceOperatorFactory.java` | MODIFY -- accept and forward `pushedFilter` to format reader |
| `FormatReader` | `esql/.../spi/FormatReader.java` | MODIFY -- add `read()` overload with filter (or use a setter) |
| `ParquetPushdownFiltersTests` | `esql-datasource-parquet/.../ParquetPushdownFiltersTests.java` | NEW -- unit tests following Iceberg pattern |

### 9.8. Risk Assessment

**Low risk**: The existing SPI infrastructure (`FilterPushdownSupport`, `PushFiltersToSource`, `FilterPushdownRegistry`, `ExternalSourceExec.pushedFilter`, `SourceOperatorContext.pushedFilter`) is complete and working for the Iceberg case. The primary work is:

1. Implementing the expression converter (`ParquetPushdownFilters`) -- straightforward, maps 1:1 to `FilterApi`
2. Wiring the `pushedFilter` through `FileSourceFactory` -> `AsyncExternalSourceOperatorFactory` -> `FormatReader` -- plumbing only
3. Calling `reader.filterRowGroups()` + `readNextFilteredRowGroup()` instead of `readNextRowGroup()` -- single method change
4. Collecting `DataSourcePlugin.filterPushdownSupport()` in `DataSourceModule` -- the method already exists on the SPI, just isn't wired

**Schema dependency**: `ParquetPushdownFilters` needs the Parquet `MessageType` to create typed column references. The schema is available in `ParquetFormatReader.read()` (line 101) but would need to be passed to the filter converter. This is solvable by converting the filter at read time rather than at optimization time, or by storing the schema in `sourceMetadata`.

**RECHECK semantics**: Using `RECHECK` means the FilterExec stays in the plan, and rows are filtered twice (once by row-group statistics, once by ESQL). This is correct but less efficient than full pushdown. Record-level filtering via `RecordFilter` could upgrade this to `YES` in a future iteration.
