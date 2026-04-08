# Deep Dive: LIMIT Pushdown to External Sources

## 1. Limit Plan Node

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Limit.java`

The `Limit` logical plan node (line 21) extends `UnaryPlan` and implements `TelemetryAware`, `PipelineBreaker`, and `ExecutesOn`.

### Fields (lines 24-36):
- `Expression limit` (line 24) — the limit value expression (typically a `Literal` of type `INTEGER`)
- `boolean duplicated` (line 31, transient) — optimization flag to avoid infinite loops when pushing limits past `MV_EXPAND` or `Join` nodes in `PushDownAndCombineLimits`
- `boolean local` (line 36, transient) — when true, the limit is not a pipeline breaker and applies only to local node data; should always end up inside a fragment

### Key behavior:
- **ExecuteLocation** (lines 145-148): Global limits execute on `COORDINATOR`; local limits (`local == true`) execute on `ANY`.
- Serialization (lines 56-76): Only `limit` and `child` are serialized; `duplicated` and `local` are transient.

---

## 2. ExternalSourceExec

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

### Fields (lines 58-66):
| Field | Type | Serialized? | Notes |
|---|---|---|---|
| `sourcePath` | `String` | Yes | URI/path of the external source |
| `sourceType` | `String` | Yes | Format type (e.g., "parquet", "ndjson") |
| `attributes` | `List<Attribute>` | Yes | Output columns |
| `config` | `Map<String, Object>` | Yes | Source configuration |
| `sourceMetadata` | `Map<String, Object>` | Yes | Opaque metadata (native schema, etc.) |
| `pushedFilter` | `Object` | **No** | Opaque filter, coordinator-only |
| `estimatedRowSize` | `Integer` | Yes | Row size estimate |
| `fileSet` | `FileSet` | **No** | Resolved file listing, coordinator-only |
| `splits` | `List<ExternalSplit>` | Yes | Split list for distributed execution |

### Critical finding: **No limit field exists.**

There is no `limit` or `Expression limit` field on `ExternalSourceExec`. The class has a `pushedFilter` (opaque `Object`, line 63) with `withPushedFilter()` (lines 239-252), but no equivalent for limit.

### Comparison with EsQueryExec:

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/EsQueryExec.java`

`EsQueryExec` has a dedicated `Expression limit` field (line 51) and a `withLimit()` method (lines 247-250). This is the field that `PushLimitToSource` writes into, and that `EsPhysicalOperationProviders.sourcePhysicalOperation()` reads from to configure the Lucene source operator's `Limiter` (line 348 of EsPhysicalOperationProviders.java).

---

## 3. Existing Limit Pushdown Rules

### 3a. PushLimitToSource (Physical, Lucene-only)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushLimitToSource.java`

This is the **key rule** and is very simple (28 lines total):

```java
public class PushLimitToSource extends PhysicalOptimizerRules.OptimizerRule<LimitExec> {
    @Override
    protected PhysicalPlan rule(LimitExec limitExec) {
        PhysicalPlan plan = limitExec;
        PhysicalPlan child = limitExec.child();
        if (child instanceof EsQueryExec queryExec) {
            plan = queryExec.withLimit(limitExec.limit());
        } else if (child instanceof ExchangeExec exchangeExec
                   && exchangeExec.child() instanceof EsQueryExec queryExec) {
            plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
        }
        return plan;
    }
}
```

**Pattern:** Matches `LimitExec -> EsQueryExec` (or `LimitExec -> ExchangeExec -> EsQueryExec`). Absorbs the `LimitExec` by calling `queryExec.withLimit(limitExec.limit())`.

**No handling for ExternalSourceExec.** The rule only matches `EsQueryExec`.

### 3b. PushDownAndCombineLimits (Logical)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PushDownAndCombineLimits.java`

This logical optimizer rule (line 36) pushes `Limit` nodes down through transparent operators (`Eval`, `Project`, `RegexExtract`, etc.) and combines adjacent limits. It does **not** push limits into source nodes — that happens in the physical optimizer.

Key behaviors (lines 52-107):
- **Limit + Limit** → combines to smallest (lines 53-54, 143-164)
- **Limit + Eval/Project/RegexExtract** → pushes limit below (line 65)
- **Limit + MvExpand** → duplicates limit as grandchild (line 71)
- **Limit + Join** → duplicates limit as grandchild (lines 93-102)

### 3c. PushLimitToKnn (Logical)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PushLimitToKnn.java`

Pushes limit value into KNN function expressions as `implicitK`. Not relevant for external sources.

### 3d. PushTopNToSource (Physical)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushTopNToSource.java`

Pushes `TopNExec` (sort + limit) to Lucene source as sorted query. Only targets `EsQueryExec`, not `ExternalSourceExec`.

### Optimizer registration

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalPlanOptimizer.java`

The rules are registered in `rules()` (lines 69-106):
```java
esSourceRules.add(new PushTopNToSource());     // line 73
esSourceRules.add(new PushLimitToSource());    // line 74
esSourceRules.add(new PushFiltersToSource());  // line 75
```

These run in a batch named `"Push to ES"` that executes multiple times (lines 82-83).

---

## 4. PushFiltersToSource as a Pattern

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java`

This rule shows the existing pattern for external source pushdown.

### How it detects ExternalSourceExec (lines 48-57):
```java
protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
    PhysicalPlan plan = filterExec;
    if (filterExec.child() instanceof EsQueryExec queryExec) {
        plan = planFilterExec(filterExec, queryExec, ctx);
    } else if (...) {
        ...
    } else if (filterExec.child() instanceof ExternalSourceExec externalExec) {
        plan = planFilterExecForExternalSource(filterExec, externalExec, ctx.filterPushdownRegistry());
    }
    return plan;
}
```

### How it pushes filters (lines 241-286):
1. Looks up `FilterPushdownSupport` from `FilterPushdownRegistry` by source type
2. Splits filter by AND into individual expressions
3. Calls `pushdownSupport.pushFilters(filters)` which returns a `PushdownResult` containing `pushedFilter()` (opaque) and `remainder()` (non-pushable expressions)
4. Creates new `ExternalSourceExec` via `externalExec.withPushedFilter(combinedFilter)`
5. If there are non-pushable filters, wraps in a new `FilterExec`; otherwise removes `FilterExec` entirely

### Key insight for limit pushdown:
The filter pushdown relies on an SPI (`FilterPushdownRegistry`) because different source types have different filter semantics (Iceberg expressions vs. Parquet row-group statistics vs. no support). A limit pushdown is **much simpler** — every source can benefit from knowing the limit. No SPI lookup is needed.

---

## 5. External Source Operator Execution

### 5a. AsyncExternalSourceOperator

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperator.java`

A thin wrapper around `AsyncExternalSourceBuffer`. The `getOutput()` method (lines 45-52) simply calls `buffer.pollPage()`. There is **no row counting or limit checking** in this operator.

### 5b. AsyncExternalSourceOperatorFactory

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`

Creates the operator. Has three execution paths:
- **SliceQueue mode** (line 173): iterates splits from queue, reads each file via `formatReader.read()`, drains pages into buffer
- **Multi-file mode** (line 176): iterates resolved `fileSet.files()`, reads each file, drains pages
- **Single-file mode** (lines 180-190): reads one file, either native-async or sync-wrapper

**No limit parameter anywhere.** The factory accepts `batchSize` and `maxBufferSize` but no row limit.

### 5c. ExternalSourceOperatorFactory (Sync variant)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceOperatorFactory.java`

Synchronous variant. Inner `ExternalSourceOperator` (lines 127-183) simply iterates `CloseableIterator<Page>` until exhausted. No limit checking.

Inner `SliceQueueSourceOperator` (lines 189-297) similarly has no limit checking.

### 5d. AsyncConnectorSourceOperatorFactory

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncConnectorSourceOperatorFactory.java`

Creates a connector-based source operator. No limit parameter.

### 5e. ExternalSourceDrainUtils

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceDrainUtils.java`

The `drainPages()` method (lines 25-40) pumps pages from a `CloseableIterator<Page>` into `AsyncExternalSourceBuffer` with backpressure. It only stops when:
1. `pages.hasNext()` returns false (data exhausted), or
2. `buffer.noMoreInputs()` returns true (downstream signaled completion)

**Early termination exists** but is reactive: when the driver's `LimitOperator` (or any downstream operator) finishes, it calls `buffer.finish(true)` which sets `noMoreInputs = true`, and `drainPages` exits its loop. This means the background reader thread will eventually stop, but it may have already read and buffered data beyond the limit.

### 5f. AsyncExternalSourceBuffer

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceBuffer.java`

The buffer (line 30) provides thread-safe page queuing. The `finish(boolean drainingPages)` method (lines 196-210) sets `noMoreInputs = true` and optionally discards remaining pages. The `noMoreInputs()` method (line 224) returns the volatile flag that `ExternalSourceDrainUtils.drainPages()` checks.

**No row limit or page limit field.** The only size constraint is `maxSize` (max pages in buffer, default 10).

---

## 6. SourceOperatorContext

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java`

This is a Java `record` (line 41) with these fields:
```java
public record SourceOperatorContext(
    String sourceType,
    StoragePath path,
    List<String> projectedColumns,
    List<Attribute> attributes,
    int batchSize,
    int maxBufferSize,
    Executor executor,
    Map<String, Object> config,
    Map<String, Object> sourceMetadata,
    Object pushedFilter,
    FileSet fileSet,
    @Nullable ExternalSplit split,
    Set<String> partitionColumnNames,
    @Nullable ExternalSliceQueue sliceQueue
)
```

**No limit, maxRows, or similar field.** The context carries `batchSize` (page size, default 1000 from Builder line 206) and `maxBufferSize` (buffer depth, default 10 from Builder line 207) but nothing about total row count.

---

## 7. Format Reader Iteration

### FormatReader Interface

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`

The `read()` method signature (line 58):
```java
CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;
```

**No limit parameter.** The reader reads until the data is exhausted. The `batchSize` controls how many rows per `Page`, not total rows.

### ParquetFormatReader

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

The `ParquetPageIterator` inner class (lines 173-383) reads row groups one at a time:
- `hasNext()` (lines 201-223): if `rowsRemainingInGroup > 0`, returns true; otherwise reads the next row group via `reader.readNextRowGroup()`
- `next()` (lines 226-253): reads up to `batchSize` rows from the current row group, converts to `Page`

**No total row limit.** The iterator reads all row groups until `readNextRowGroup()` returns null.

### How Readers Know When to Stop (Current State):
1. **Data exhausted**: Reader returns all data from the file/source
2. **Downstream backpressure**: `drainPages()` in `ExternalSourceDrainUtils` checks `buffer.noMoreInputs()` each iteration, which becomes true when the `LimitOperator` downstream finishes and calls `buffer.finish(true)` through the operator chain
3. **No proactive limit**: Readers have no mechanism to stop after N rows

---

## 8. What Exactly Needs to Change

### Summary of the Current Flow (without limit pushdown):

```
Query: EXTERNAL "s3://bucket/data.parquet" | LIMIT 100

Physical plan:
  OutputExec
    LimitExec[100]          <-- LimitOperator truncates pages after 100 rows
      ExternalSourceExec    <-- reads ALL data from file
```

The `LimitOperator` (compute engine) truncates output to 100 rows, but the external source reads the **entire file** first. For a 10GB Parquet file, this is devastating.

### Change 1: Add `limit` field to ExternalSourceExec

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`

Add a new field:
```java
private final Expression limit;  // null means no limit pushed
```

Add `withLimit()` method (like `withPushedFilter()`):
```java
public ExternalSourceExec withLimit(Expression newLimit) {
    return new ExternalSourceExec(..., newLimit, ...);
}
```

Update all constructors, `info()`, `equals()`, `hashCode()`, `nodeString()`. The limit does NOT need serialization (coordinator-only node, like `pushedFilter`).

### Change 2: Extend PushLimitToSource to match ExternalSourceExec

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushLimitToSource.java`

Add an `else if` branch:
```java
} else if (child instanceof ExternalSourceExec externalExec) {
    plan = externalExec.withLimit(limitExec.limit());
}
```

This follows the exact same pattern as the EsQueryExec branch. No SPI or registry needed.

### Change 3: Add `limit` (or `maxRows`) to SourceOperatorContext

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java`

Add a new field to the record:
```java
int maxRows  // -1 or Integer.MAX_VALUE means no limit
```

Add to the Builder as well.

### Change 4: Pass limit from ExternalSourceExec into SourceOperatorContext

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java`

In `planExternalSource()` (lines 1223-1279), extract the limit:
```java
int maxRows = -1;
if (externalSource.limit() != null) {
    maxRows = (int) externalSource.limit().fold(context.foldCtx());
}
```

Pass to builder:
```java
.maxRows(maxRows)
```

### Change 5: Honor limit in AsyncExternalSourceOperatorFactory (and variants)

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`

Option A — **Row-counting in drainPages**: Modify `ExternalSourceDrainUtils.drainPages()` or add a new overload that accepts a `maxRows` parameter and counts rows as pages are drained, stopping once the limit is reached. This is the simplest approach and works for all code paths (single-file, multi-file, slice-queue).

Option B — **Limiter in operator**: Like `LuceneSourceOperator`, create a `Limiter` in the factory and share it across parallel drivers. The `AsyncExternalSourceOperator.getOutput()` would check the limiter before emitting pages. This is more aligned with the Lucene pattern but requires modifying the operator itself.

**Recommended: Option A** for MVP, because:
- It stops reading from the source proactively (Option B only stops emitting but keeps reading)
- It works with the existing backpressure model (drainPages loop)
- It's a single-point change

### Change 6: (Optional, post-MVP) Pass limit to FormatReader.read()

**File to modify:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`

Add an overload:
```java
default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, int maxRows) {
    // Default: ignore maxRows, use existing implementation
    return read(object, projectedColumns, batchSize);
}
```

Parquet could override to skip row groups beyond the limit. This is an optimization but not strictly necessary for correctness — the drainPages row-counting in Change 5 already prevents excess reading.

### Change 7: QueryRequest for connectors

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java`

For query-based connectors (Flight, future JDBC), the `QueryRequest` record needs a `limit` field:
```java
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    BlockFactory blockFactory,
    int maxRows  // NEW: -1 means no limit
)
```

This allows connectors to incorporate LIMIT into their queries (e.g., `SELECT ... FROM table LIMIT 100` for JDBC, or metadata hints for Flight).

---

## Implementation Priority

| Change | Effort | Impact | Required for MVP? |
|---|---|---|---|
| 1. Add `limit` to `ExternalSourceExec` | Small | Foundation for all else | **Yes** |
| 2. Extend `PushLimitToSource` | Small | Optimizer wiring | **Yes** |
| 3. Add `maxRows` to `SourceOperatorContext` | Small | Context plumbing | **Yes** |
| 4. Pass limit in `planExternalSource()` | Small | Planner wiring | **Yes** |
| 5. Row-counting in `drainPages` | Medium | Actual early termination | **Yes** |
| 6. `FormatReader.read()` overload | Small | Enables format-specific optimization | No (post-MVP) |
| 7. `QueryRequest` limit field | Small | Connector limit pushdown | No (post-MVP, connectors disabled) |

### Estimated effort: 1-2 days for changes 1-5 (including tests)

---

## Comparison: How Lucene Limit Pushdown Works End-to-End

For reference, the complete Lucene path:

1. **Optimizer**: `PushLimitToSource` matches `LimitExec -> EsQueryExec`, calls `queryExec.withLimit(expr)`, removes `LimitExec`
   - File: `PushLimitToSource.java` lines 17-28

2. **Plan node**: `EsQueryExec.limit` field stores the pushed expression
   - File: `EsQueryExec.java` line 51

3. **Planner**: `EsPhysicalOperationProviders.sourcePhysicalOperation()` folds the expression to an int, passes to `LuceneSourceOperator.Factory`
   - File: `EsPhysicalOperationProviders.java` line 348: `int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold(context.foldCtx()) : NO_LIMIT;`

4. **Operator factory**: Creates a `Limiter` shared across parallel drivers
   - File: `LuceneSourceOperator.java` line 97: `this.limiter = limit == NO_LIMIT ? Limiter.NO_LIMIT : new Limiter(limit);`

5. **Limiter**: Atomic counter tracks rows across drivers; `tryAccumulateHits()` returns accepted count
   - File: `Limiter.java` lines 17-72

The external source path needs an analogous chain, with the key difference that file-based sources use `drainPages()` loop (Change 5) instead of Lucene's scorer-level integration.
