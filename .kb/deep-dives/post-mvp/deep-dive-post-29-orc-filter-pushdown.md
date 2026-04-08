# Deep Dive #29: Filter Pushdown for ORC Format

## Executive Summary

ORC files organize data into **stripes** with per-column statistics (min/max/count/hasNull) at both the file level and stripe level. Apache ORC's `Reader.Options` already supports a `searchArgument(SearchArgument sarg)` method that enables stripe-level predicate pushdown. The ORC plugin in ES|QL currently has **no** filter pushdown -- it reads all stripes unconditionally. Adding ORC filter pushdown requires: (1) a ~200-line ESQL-to-SearchArgument translator (structurally identical to `IcebergPushdownFilters`), (2) a `FilterPushdownSupport` implementation registered in `OrcDataSourcePlugin`, and (3) wiring the pushed filter through `FileSourceFactory` to `OrcFormatReader.read()`.

**Estimated effort**: 2-3 days (translation code + plumbing + tests), leveraging the Iceberg translator as a direct template.

---

## 1. Current ORC Implementation

### 1.1 OrcFormatReader

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcFormatReader.java`

The reader implements the `FormatReader` SPI. Key observations:

- **Line 18-21**: Imports `OrcFile`, `Reader`, `RecordReader`, `TypeDescription` from `org.apache.orc`
- **Line 87**: `read(StorageObject object, List<String> projectedColumns, int batchSize)` -- the main entry point
- **Line 129-133**: Creates `Reader.Options`, sets batch size and column include array, then calls `reader.rows(readOptions)`:
  ```java
  Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
  if (include != null) {
      readOptions.include(include);
  }
  RecordReader rows = reader.rows(readOptions);
  ```
- **Column projection** (lines 96-127): Works via ORC's `include` boolean array on `Reader.Options`. Index 0 = root struct, indices 1..N = each field by TypeDescription ID.
- **VectorizedRowBatch** (line 203): ORC reads data as vectorized column batches, mapped directly to ESQL Blocks. This is already efficient columnar reading.
- **No filter**: The `Reader.Options` object is created without calling `.searchArgument()`. All stripes are read.

### 1.2 OrcDataSourcePlugin

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcDataSourcePlugin.java`

- Registers format "orc" with extension ".orc"
- **Does NOT override** `filterPushdownSupport()` -- defaults to returning empty map
- Only provides `formatReaders()` (line 41-43)

### 1.3 ORC Library Version

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/build.gradle`

- **ORC version**: 2.2.2 (line 13)
- **Hadoop version**: 3.4.2 (line 12)
- **Hive storage API**: 2.8.1 (line 41)

ORC 2.2.2 is a recent release that fully supports `SearchArgument`-based predicate pushdown. The SearchArgument API has been stable since ORC 1.x.

---

## 2. ORC SearchArgument API

### 2.1 API Overview

Apache ORC provides predicate pushdown via `org.apache.orc.filter.SearchArgument` (in orc-core). The key classes are:

- `SearchArgument` -- represents a predicate tree (AND/OR/NOT of leaf predicates)
- `SearchArgumentFactory` -- factory to create SearchArgument instances
- `SearchArgument.Builder` -- fluent builder API
- `PredicateLeaf` -- individual comparison (EQUALS, NULL_SAFE_EQUALS, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN, IS_NULL)

### 2.2 SearchArgument Builder Pattern

```java
SearchArgument sarg = SearchArgumentFactory.newBuilder()
    .startAnd()
        .lessThan("age", PredicateLeaf.Type.LONG, 30L)
        .startNot()
            .isNull("name", PredicateLeaf.Type.STRING)
        .end()
    .end()
    .build();
```

### 2.3 PredicateLeaf Operations

The `PredicateLeaf.Operator` enum supports:
- `EQUALS` -- column = value
- `NULL_SAFE_EQUALS` -- column <=> value (null-safe)
- `LESS_THAN` -- column < value
- `LESS_THAN_EQUALS` -- column <= value
- `IN` -- column IN (v1, v2, ...)
- `BETWEEN` -- column BETWEEN low AND high
- `IS_NULL` -- column IS NULL

### 2.4 PredicateLeaf Types

The `PredicateLeaf.Type` enum supports:
- `LONG` -- for BOOLEAN, BYTE, SHORT, INT, LONG
- `FLOAT` -- for FLOAT
- `DOUBLE` -- for DOUBLE (NEW in ORC 2.x -- prior versions used FLOAT for both)
- `STRING` -- for STRING, CHAR, VARCHAR, BINARY
- `DATE` -- for DATE
- `TIMESTAMP` -- for TIMESTAMP, TIMESTAMP_INSTANT
- `DECIMAL` -- for DECIMAL

### 2.5 Wiring to Reader.Options

The `Reader.Options` class has a method:
```java
Reader.Options searchArgument(SearchArgument sarg, String[] columnNames)
```

The `columnNames` array maps SearchArgument column references to actual ORC column names. This is how the pushed filter reaches the ORC reader, which then uses stripe-level and row-group-level statistics to skip data.

### 2.6 What Gets Skipped

ORC has a three-level filtering hierarchy:
1. **File-level statistics**: Column min/max for the entire file (used during split planning)
2. **Stripe-level statistics**: Column min/max per stripe (typically ~200MB per stripe). The SearchArgument evaluates against stripe statistics to skip entire stripes.
3. **Row-group-level statistics (ORC index)**: Column min/max per ~10,000 rows within a stripe. Further narrows which row groups within a non-skipped stripe are read.

All three levels are evaluated automatically by ORC's RecordReader when a SearchArgument is set.

---

## 3. Existing Filter Pushdown Infrastructure

### 3.1 FilterPushdownSupport SPI

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`

Interface contract:
- `pushFilters(List<Expression> filters)` -- returns `PushdownResult(Object pushedFilter, List<Expression> remainder)`
- `canPush(Expression expr)` -- returns `Pushability.YES`, `NO`, or `RECHECK`
- The `pushedFilter` is opaque to core -- only the source-specific operator factory interprets it

### 3.2 PushFiltersToSource Optimizer Rule

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

- Line 54-56: Handles `ExternalSourceExec` children of `FilterExec`
- Line 241-286: `planFilterExecForExternalSource()` -- looks up `FilterPushdownSupport` from `FilterPushdownRegistry`, calls `pushFilters()`, creates new `ExternalSourceExec.withPushedFilter()`
- The pushed filter flows: optimizer rule -> ExternalSourceExec.pushedFilter -> SourceOperatorContext.pushedFilter -> operator factory

### 3.3 Filter Flow Gap: FileSourceFactory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`

**CRITICAL GAP**: Line 118-151 shows `FileSourceFactory.operatorFactory()` creates `AsyncExternalSourceOperatorFactory` but **does NOT pass `context.pushedFilter()`** to it. The pushed filter is available in the `SourceOperatorContext` but the `AsyncExternalSourceOperatorFactory` constructor doesn't accept it (line 68-80 of that class -- no `pushedFilter` parameter).

This means even if we implement `FilterPushdownSupport` for ORC, the pushed filter would be stored in `ExternalSourceExec.pushedFilter()` and passed to `SourceOperatorContext.pushedFilter()`, but it would be silently dropped by `FileSourceFactory` before reaching `OrcFormatReader.read()`.

### 3.4 FormatReader Interface Gap

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`

The `FormatReader.read()` signature (line 58):
```java
CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize)
```

There is **no filter parameter**. Adding filter pushdown to ORC requires either:
1. Adding an optional `Object pushedFilter` parameter to `FormatReader.read()`, or
2. Having the `FormatReader` accept the filter during construction (set it on the reader instance before `read()` is called), or
3. Creating a `FilterAwareFormatReader` subinterface with a `read(..., Object filter)` method

---

## 4. IcebergPushdownFilters as Template

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`

This is the closest existing translation code. It converts ESQL expressions to Iceberg expressions. The pattern is:

```java
public static org.apache.iceberg.expressions.Expression convert(Expression esqlExpr) {
    if (esqlExpr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
        String fieldName = ne.name();
        Object value = convertValue(literalValueOf(bc.right()));
        return switch (bc) {
            case Equals ignored -> equal(fieldName, value);
            case NotEquals ignored -> notEqual(fieldName, value);
            case LessThan ignored -> lessThan(fieldName, value);
            // ...
        };
    }
    // IN, IS NULL, IS NOT NULL, Range, AND, OR, NOT...
}
```

### 4.1 Expressions Handled by IcebergPushdownFilters

| ESQL Expression | Iceberg Target | ORC SearchArgument Equivalent |
|---|---|---|
| `Equals` | `equal(field, value)` | `equals(field, type, value)` |
| `NotEquals` | `notEqual(field, value)` | `startNot().equals(...)` |
| `LessThan` | `lessThan(field, value)` | `lessThan(field, type, value)` |
| `LessThanOrEqual` | `lessThanOrEqual(field, value)` | `lessThanEquals(field, type, value)` |
| `GreaterThan` | `greaterThan(field, value)` | `startNot().lessThanEquals(...)` |
| `GreaterThanOrEqual` | `greaterThanOrEqual(field, value)` | `startNot().lessThan(...)` |
| `In` | `in(field, values)` | `in(field, type, values...)` |
| `IsNull` | `isNull(field)` | `isNull(field, type)` |
| `IsNotNull` | `notNull(field)` | `startNot().isNull(...)` |
| `Range` | `and(gte, lte)` | `between(field, type, low, high)` |
| `And` | `and(left, right)` | `startAnd()...end()` |
| `Or` | `or(left, right)` | `startOr()...end()` |
| `Not` | `not(inner)` | `startNot()...end()` |

### 4.2 Key Difference: Builder Pattern

Iceberg uses a functional expression tree (each call returns an Expression). ORC uses a **stateful builder** pattern with `startAnd()/startOr()/startNot()/end()`. This means the ORC translator needs to be structured slightly differently -- using the builder's stack-based approach rather than recursive expression composition.

---

## 5. Parquet Filter Pushdown Status

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

**No filter pushdown implemented.** The Parquet reader:
- Uses `ParquetFileReader.open()` with default `ParquetReadOptions` (line 94-97)
- Reads row groups sequentially with `reader.readNextRowGroup()` (line 212)
- Does not use Parquet's `FilterCompat` or `FilteredRecordReader`
- `ParquetDataSourcePlugin` does not register any `FilterPushdownSupport`

The roadmap has "Parquet row-group filter pushdown" listed as an MVP GA item. Neither ORC nor Parquet currently have filter pushdown wired.

---

## 6. What Needs to Be Built

### 6.1 Layer 1: ESQL Expression to ORC SearchArgument Translation

Create `OrcPushdownFilters.java` in the ORC plugin module. Structure mirrors `IcebergPushdownFilters`:

```java
package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
// ... ESQL expression imports

public class OrcPushdownFilters {

    /**
     * Convert a list of ESQL filter expressions (ANDed together) to an ORC SearchArgument.
     * Returns null if no expressions can be converted.
     */
    public static SearchArgument convert(List<Expression> filters, TypeDescription schema) {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        builder.startAnd();
        boolean anyConverted = false;
        for (Expression filter : filters) {
            if (convertExpression(builder, filter, schema)) {
                anyConverted = true;
            }
        }
        builder.end();
        return anyConverted ? builder.build() : null;
    }

    private static boolean convertExpression(SearchArgument.Builder builder, Expression expr, TypeDescription schema) {
        // Pattern-match on ESQL expression types (same cases as IcebergPushdownFilters)
        // Use builder.equals(), builder.lessThan(), builder.in(), etc.
        // Handle GreaterThan as startNot().lessThanEquals()
        // Handle NotEquals as startNot().equals()
        // ...
    }

    private static PredicateLeaf.Type mapEsqlTypeToOrcType(DataType dataType) {
        return switch (dataType) {
            case BOOLEAN, INTEGER, LONG -> PredicateLeaf.Type.LONG;
            case DOUBLE -> PredicateLeaf.Type.DOUBLE; // ORC 2.x supports DOUBLE
            case KEYWORD, TEXT -> PredicateLeaf.Type.STRING;
            case DATETIME -> PredicateLeaf.Type.TIMESTAMP;
            default -> null; // unsupported
        };
    }
}
```

**Key complexity**: The ORC `SearchArgument.Builder` uses a stack-based API (`startAnd()/end()`) rather than the functional composition pattern used by Iceberg. The builder expects predicates to be added between `start*()` and `end()` calls. This requires careful sequencing when handling AND/OR/NOT recursion.

**ESQL type to ORC type mapping** (based on `convertOrcTypeToEsql()` at line 165-177 of `OrcFormatReader.java`):

| ESQL DataType | ORC PredicateLeaf.Type | Notes |
|---|---|---|
| BOOLEAN | LONG | ORC stores booleans as 0/1 longs |
| INTEGER | LONG | ORC BYTE/SHORT/INT map to LONG in predicates |
| LONG | LONG | Direct mapping |
| DOUBLE | DOUBLE | ORC 2.x added DOUBLE type (was FLOAT pre-2.x) |
| KEYWORD/TEXT | STRING | BytesRef values need toString() conversion |
| DATETIME | TIMESTAMP | Need millis-to-Timestamp conversion |

**Value conversion considerations**:
- `BytesRef` values from ESQL keywords must be converted to `String` via `BytesRefs.toString()` (same as Iceberg translator, line 141)
- Boolean values in ESQL are `Boolean` but ORC predicates expect `Long` (0 or 1)
- DATETIME values in ESQL are epoch millis (`Long`) but ORC expects `Timestamp` objects
- Integer values in ESQL are `Integer` but ORC predicates on INT columns expect `Long`

### 6.2 Layer 2: FilterPushdownSupport Registration

Create `OrcFilterPushdownSupport implements FilterPushdownSupport` in the ORC plugin:

```java
public class OrcFilterPushdownSupport implements FilterPushdownSupport {
    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushed = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>();
        for (Expression filter : filters) {
            if (canPush(filter) != Pushability.NO) {
                pushed.add(filter);
            } else {
                remainder.add(filter);
            }
        }
        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }
        // Build SearchArgument from pushed expressions
        SearchArgument sarg = OrcPushdownFilters.convert(pushed, ...);
        // Return sarg as opaque pushed filter, with RECHECK remainder
        return new PushdownResult(sarg, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        // Check if expression is a supported comparison with foldable right side
        // Return RECHECK for most predicates (stripe stats are min/max based, not exact)
    }
}
```

**Important**: Most ORC pushdown should use `RECHECK` rather than `YES`. ORC stripe statistics provide min/max bounds -- they can rule out stripes that definitely don't match, but rows within a matching stripe may still not match the predicate. The FilterExec must remain in the plan for correctness. The exception is `IS NULL` when the stripe's `hasNull` stat is false -- that can be `YES`.

Register in `OrcDataSourcePlugin`:
```java
@Override
public Map<String, FilterPushdownSupport> filterPushdownSupport(Settings settings) {
    return Map.of("orc", new OrcFilterPushdownSupport());
}
```

### 6.3 Layer 3: Plumbing the Filter Through

**Three changes required in framework code:**

1. **`FormatReader` interface** -- Add a filter-aware read method:
   ```java
   // In FormatReader.java
   default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns,
                                         int batchSize, Object pushedFilter) throws IOException {
       return read(object, projectedColumns, batchSize); // default ignores filter
   }
   ```

2. **`AsyncExternalSourceOperatorFactory`** -- Add `pushedFilter` field, pass it to `formatReader.read()`:
   - Add `Object pushedFilter` to constructor
   - Pass it through `startSyncWrapperRead()`, `startMultiFileRead()`, `startSliceQueueRead()` paths

3. **`FileSourceFactory.operatorFactory()`** -- Pass `context.pushedFilter()` when creating `AsyncExternalSourceOperatorFactory`

4. **`OrcFormatReader.read()`** -- Override the filter-aware variant:
   ```java
   @Override
   public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns,
                                        int batchSize, Object pushedFilter) throws IOException {
       // ... existing setup code ...
       Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
       if (include != null) {
           readOptions.include(include);
       }
       if (pushedFilter instanceof SearchArgument sarg) {
           String[] columnNames = schema.getFieldNames().toArray(new String[0]);
           readOptions.searchArgument(sarg, columnNames);
       }
       RecordReader rows = reader.rows(readOptions);
       // ... rest unchanged ...
   }
   ```

### 6.4 DataSourceModule Wiring

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java`

Currently (line 215-227), `DataSourceModule` builds the `FilterPushdownRegistry` by iterating over `sourceFactories` and calling `factory.filterPushdownSupport()`. The file-based path goes through `FileSourceFactory` which inherits the default `filterPushdownSupport() -> null` from `ExternalSourceFactory`.

**Problem**: ORC registers filter pushdown via `DataSourcePlugin.filterPushdownSupport()` (keyed by source type), but `DataSourceModule` doesn't collect those. It only checks `ExternalSourceFactory.filterPushdownSupport()`.

Looking at the DataSourceModule constructor more carefully, there **is** collection of `DataSourcePlugin.filterPushdownSupport()` -- but only into the `filterPushdownProviders` map indirectly. Actually, the `FileSourceFactory` is registered under format names (line 203-205) like "orc", "parquet", etc., and it returns `null` for `filterPushdownSupport()`. The `DataSourcePlugin.filterPushdownSupport()` method exists but is never collected in `DataSourceModule`.

**Fix needed**: In `DataSourceModule`'s constructor, after the per-plugin loop, collect `plugin.filterPushdownSupport(settings)` entries and add them to `filterPushdownProviders`. This bridges the gap between the plugin's declaration and the registry.

---

## 7. Reusability Assessment

### 7.1 Can Iceberg Translator Be Reused?

**Partially, but not directly.** The two translators share:
- Same input types (ESQL Expression tree: `Equals`, `LessThan`, `In`, `IsNull`, `And`, `Or`, `Not`, `Range`)
- Same extraction pattern (`instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()`)
- Same value conversion need (`BytesRefs.toString()`, `Foldables.literalValueOf()`)

They differ in:
- Output type: Iceberg `Expression` (functional tree) vs ORC `SearchArgument` (builder stack)
- Type system: Iceberg uses generic `Object` values; ORC needs explicit `PredicateLeaf.Type`
- GreaterThan encoding: Iceberg has `greaterThan()`; ORC requires `startNot().lessThanEquals().end()`
- NotEquals encoding: Iceberg has `notEqual()`; ORC requires `startNot().equals().end()`
- Between/Range: Iceberg composes two bounds with `and()`; ORC has native `between()`

**Recommendation**: Create `OrcPushdownFilters` as a separate class (not shared with Iceberg), but follow the same pattern. The translation logic is ~150-200 lines -- small enough that a dedicated implementation is cleaner than an abstraction layer.

### 7.2 Could a Shared Base Be Extracted?

A hypothetical `AbstractExpressionTranslator<T>` could define:
```java
abstract T handleEquals(String field, Object value);
abstract T handleLessThan(String field, Object value);
abstract T handleAnd(T left, T right);
// ...
```

But this is premature abstraction. With only two targets (Iceberg, ORC) and different output paradigms (functional vs builder), the shared code would be just the pattern matching on ESQL expression types -- which is trivial boilerplate. Adding Parquet later (which uses `FilterApi` with yet another paradigm) wouldn't benefit from this either.

---

## 8. Performance Impact Estimate

### 8.1 ORC Stripe Sizes

Default ORC stripe size is **64MB** (configurable, commonly 64-256MB). A 1GB ORC file has ~16 stripes with default settings.

### 8.2 Selectivity Scenarios

For a query like `EXTERNAL "s3://bucket/logs.orc" | WHERE status == 500`:
- **Without pushdown**: All 16 stripes read (1GB I/O)
- **With pushdown**: If `status` column min/max per stripe shows only 3 stripes contain 500, only 3 stripes read (~192MB I/O). **~80% I/O reduction.**

For timestamp range queries (common in log analytics):
- ORC files with time-ordered data will have excellent stripe pruning on timestamp columns
- A 24-hour file queried for 1 hour could skip ~95% of stripes

### 8.3 Row-Group Filtering

Within a stripe, ORC maintains statistics per ~10,000-row groups. The SearchArgument also filters at this level, providing finer-grained skipping within non-pruned stripes.

---

## 9. Implementation Plan

### Phase 1: ORC Translation (1 day)
1. Create `OrcPushdownFilters.java` in `esql-datasource-orc/src/main/java/`
2. Implement ESQL -> SearchArgument translation for: Equals, NotEquals, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, In, IsNull, IsNotNull, Range, And, Or, Not
3. Create `OrcPushdownFiltersTests.java` with comprehensive tests (mirror `IcebergPushdownFiltersTests`)

### Phase 2: SPI Wiring (1 day)
1. Add filter-aware `read()` variant to `FormatReader`
2. Add `pushedFilter` parameter to `AsyncExternalSourceOperatorFactory`
3. Wire `context.pushedFilter()` through `FileSourceFactory.operatorFactory()`
4. Implement `OrcFilterPushdownSupport` and register in `OrcDataSourcePlugin`
5. Fix `DataSourceModule` to collect `DataSourcePlugin.filterPushdownSupport()` entries

### Phase 3: ORC Reader Integration (0.5 day)
1. Override filter-aware `read()` in `OrcFormatReader`
2. Apply `SearchArgument` to `Reader.Options` when available
3. Test with multi-stripe ORC files to verify stripe skipping

### Phase 4: Testing (0.5 day)
1. Unit tests for translation correctness
2. Integration test: create multi-stripe ORC file, query with filters, verify fewer stripes read
3. Verify RECHECK semantics: FilterExec remains in plan for non-IS_NULL predicates

---

## 10. Key Files Reference

| File | Role |
|---|---|
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcFormatReader.java` | ORC reader (lines 129-133: Reader.Options) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcDataSourcePlugin.java` | Plugin registration (no filter pushdown) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/build.gradle` | ORC 2.2.2, Hadoop 3.4.2 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java` | SPI interface for filter pushdown |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` | FormatReader SPI (no filter param) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/DataSourcePlugin.java` | Plugin SPI with filterPushdownSupport() |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java` | Optimizer rule (lines 241-286: external source path) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java` | File path factory (missing pushedFilter wiring, line 118-151) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java` | Async operator factory (no pushedFilter param) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java` | Module wiring (lines 215-227: FilterPushdownRegistry building) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java` | Template: ESQL -> Iceberg expression translation |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java` | Context record (line 51: pushedFilter field) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java` | Physical plan node (line 63: pushedFilter, line 239: withPushedFilter) |

---

## 11. Open Questions

1. **RECHECK vs YES semantics**: Should ORC pushdown use `RECHECK` for all predicates? Stripe statistics are min/max based, so a stripe whose min=100, max=500 would pass `WHERE value == 200` even if the stripe contains no rows with value=200. The ORC reader will still decompress and iterate those rows. Using `RECHECK` keeps the FilterExec in the plan for correctness. Using `YES` would risk incorrect results.

2. **Parquet filter pushdown interaction**: The framework-level plumbing changes (FormatReader, AsyncExternalSourceOperatorFactory, FileSourceFactory) would also enable Parquet filter pushdown. Should they be done together or is ORC a standalone item?

3. **SearchArgument classpath**: The `SearchArgument` class lives in `orc-core`. It should be available since `orc-core:2.2.2` is already a dependency. However, the class may be in different packages between ORC versions (it moved from `org.apache.hadoop.hive.ql.io.sarg` to `org.apache.orc.storage.ql.io.sarg` and then to `org.apache.orc.filter` in ORC 2.x). Need to verify the exact import path for ORC 2.2.2.

4. **Column name mapping**: The `searchArgument(sarg, columnNames)` method requires a column name array that maps predicate leaf column references to actual ORC column names. This should be straightforward since ESQL uses the same column names as the ORC schema.
