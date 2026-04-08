# TSDB / Time Series Deep Dive in ES|QL

## Table of Contents

1. [METRICS/TS Source Command](#1-metricsts-source-command)
2. [IndexMode.TIME_SERIES Detection](#2-indexmodetime_series-detection)
3. [TSID Construction and Role](#3-tsid-construction-and-role)
4. [Time-Series Specific Functions](#4-time-series-specific-functions)
5. [Time-Series Grouping](#5-time-series-grouping)
6. [@timestamp Requirement](#6-timestamp-requirement)
7. [Data Ordering Requirements](#7-data-ordering-requirements)
8. [Downsampling in ES|QL](#8-downsampling-in-esql)
9. [TSID Prefix Partitioning](#9-tsid-prefix-partitioning)
10. [External Source Restrictions](#10-external-source-restrictions)
11. [Summary Architecture Diagram](#11-summary-architecture-diagram)

---

## 1. METRICS/TS Source Command

### Grammar

The `TS` command is defined in the ANTLR grammar at:

**`x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4:107-109`**:
```
timeSeriesCommand
    : TS indexPatternAndMetadataFields
    ;
```

The `TS` keyword is a lexer token at **`x-pack/plugin/esql/src/main/antlr/lexer/From.g4:14-15`**:
```
// TS command
TS : 'ts' -> pushMode(FROM_MODE);
```

The grammar for `TS` is identical to `FROM` -- both use `indexPatternAndMetadataFields`, which allows index patterns, subqueries, and METADATA clauses. The difference is entirely semantic: `TS` sets `IndexMode.TIME_SERIES`.

### Parser Translation

In the parser, `visitTimeSeriesCommand` at **`LogicalPlanBuilder.java:900-902`** converts the grammar to a plan node:
```java
public LogicalPlan visitTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx) {
    return visitRelation(source(ctx), SourceCommand.TS, ctx.indexPatternAndMetadataFields());
}
```

The `SourceCommand` enum at **`SourceCommand.java:15-18`** maps commands to index modes:
```java
FROM(IndexMode.STANDARD),
TS(IndexMode.TIME_SERIES),
PROMQL(IndexMode.TIME_SERIES);
```

### How TS Differs from FROM

1. **Index mode**: `TS` creates an `EsRelation` with `IndexMode.TIME_SERIES`; `FROM` uses `IndexMode.STANDARD`.
2. **Query filter**: At resolution time, `TS` adds a `TermQueryBuilder` for `_index_mode=time_series` to only match TSDB indices (**`EsqlSession.java:1357-1359`**).
3. **STATS becomes TimeSeriesAggregate**: When a STATS command follows a TS source, the parser creates a `TimeSeriesAggregate` instead of a regular `Aggregate` (**`LogicalPlanBuilder.java:614-622`**).
4. **Implicit sort**: For TS queries without explicit SORT or STATS, the analyzer injects `ORDER BY @timestamp DESC` so most recent points come first (**`Analyzer.java:2017-2061`**, `AddImplicitTimestampSort` rule).
5. **Different default LIMIT**: TS aggregate queries get a higher default limit (10,000) vs. non-TS (1,000) (**`Analyzer.java:1967-1971`**, `AddImplicitLimit` rule).
6. **TimeSeriesSourceOperator**: At the physical level, time-series indices use `TimeSeriesSourceOperator` instead of `LuceneSourceOperator` (**`EsPhysicalOperationProviders.java:465-474`**).

### Additional TS-only Commands

- **`TS_INFO`**: Returns one row per time series with metadata (metric name, data stream, unit, metric type, field type, dimension fields, dimensions). Defined in **`TsInfo.java`**. Verifies it can only be used with TS source (**`TsInfo.java:127`**).
- **`METRICS_INFO`**: Returns metric metadata. Defined in **`MetricsInfo.java`**. Also restricted to TS source (**`MetricsInfo.java:127`**).

---

## 2. IndexMode.TIME_SERIES Detection

### Where TIME_SERIES is Set

`IndexMode` is an enum in the server module (`server/src/main/java/org/elasticsearch/index/IndexMode.java`). It is set on index creation based on the `index.mode` setting.

In ES|QL, the index mode flows through several layers:

1. **UnresolvedRelation**: The parser creates an `UnresolvedRelation` with `IndexMode.TIME_SERIES` from the `TS` command.
2. **Index Resolution**: `EsqlSession.java:1292` passes `indexMode == IndexMode.TIME_SERIES` to the index resolver. A `TermQueryBuilder` filter is applied (**`EsqlSession.java:1357-1358`**):
   ```java
   var indexModeFilter = new TermQueryBuilder(IndexModeFieldMapper.NAME, IndexMode.TIME_SERIES.getName());
   ```
3. **EsRelation**: After resolution, the `EsRelation` carries the `indexMode` field (**`EsRelation.java:43-44`**).
4. **EsQueryExec**: The physical plan node also carries `indexMode` (**`EsQueryExec.java`**), which governs source operator selection.

### Behavioral Changes Based on TIME_SERIES

| Component | Behavior Change |
|-----------|----------------|
| **Source Operator** | Uses `TimeSeriesSourceOperator` instead of `LuceneSourceOperator` (**`EsPhysicalOperationProviders.java:465-474`**) |
| **Data Partitioning** | Defaults to AUTO which resolves to TIME_SERIES partitioning if TSID prefix is available (**`TimeSeriesSourceOperator.java:103-123`**) |
| **STATS command** | Creates `TimeSeriesAggregate` instead of `Aggregate` (**`LogicalPlanBuilder.java:614`**) |
| **Aggregation Operator** | Uses `TimeSeriesAggregationOperator` instead of `HashAggregationOperator` (**`TimeSeriesAggregationOperator.java:49`**) |
| **Block Hash** | Uses `TimeSeriesBlockHash` for optimized (tsid, timestamp) grouping (**`TimeSeriesBlockHash.java:39`**) |
| **Implicit Sort** | Adds `ORDER BY @timestamp DESC` for non-aggregate queries (**`Analyzer.java:2017`**) |
| **Field Load Optimization** | Dimension fields can be loaded after aggregation via `ExtractDimensionFieldsAfterAggregation` (**line 59**) |
| **Metadata Blocks** | Source operator emits 2 extra metadata blocks: `_ts_slice_index` (int) and `_ts_future_max_timestamp` (long) (**`TimeSeriesSourceOperator.java:79-88`**, **`EsQueryExec.java:43-46`**) |
| **aggregate_metric_double** | Enabled when index mode is TIME_SERIES (**`PreAnalyzer.java:99-101`**) |
| **Counter types** | Counter data types (`counter_long`, `counter_integer`, `counter_double`) are meaningful only in TSDB context |

---

## 3. TSID Construction and Role

### What is a TSID?

A TSID (`_tsid`) is a binary identifier that uniquely identifies a time series within a TSDB index. It is constructed from the dimension fields of a document and stored as a doc-values field.

### TSID Construction

The TSID is built by `TsidBuilder` (**`server/src/main/java/org/elasticsearch/cluster/routing/TsidBuilder.java`**).

The builder:
1. Collects all dimension fields from a document (keyword, integer, long, double, boolean, bytes).
2. Sorts dimensions by path name (**`TsidBuilder.java:218`**).
3. Computes a 128-bit MurmurHash3 hash of all (path_hash, value_hash) pairs (**`TsidBuilder.java:217-223`**).
4. Builds a TSID byte array with a prefix layout (**`TsidBuilder.java:226-231`**):
   - **Prefix**: 1 byte hash of dimension field names (clusters similar time series).
   - **Value similarity**: Up to 4 bytes, each a hash of a dimension value (helps encoding).
   - **Full hash**: 16 bytes of the combined hash (avoids collisions).

There are two layouts:
- **Multi-byte prefix** (legacy): 1 byte name hash + up to 4 value similarity bytes + 16 byte hash (**`TsidBuilder.java:258`**, `buildMultiBytePrefixTsid()`).
- **Single-byte prefix** (new, behind feature flag): 1 prefix byte + 16 byte hash (**`TsidBuilder.java:227`**, `buildSingleBytePrefixTsid()`).

### TSID in ES|QL

- **Data Type**: `TSID_DATA_TYPE` is defined as `_tsid` in **`DataType.java:376-381`** with `estimatedSize = Long.BYTES * 2` (16 bytes) and doc-values support.
- **MetadataAttribute**: TSID is treated as a metadata field (`MetadataAttribute.TSID_FIELD`). It can be requested via `TS idx METADATA _tsid`.
- **Implicit in TS STATS**: The `TranslateTimeSeriesAggregate` rule always groups by `_tsid` in the first pass (**`TranslateTimeSeriesAggregate.java:170-177`**). It looks for the TSID attribute in the EsRelation output, or creates a new `MetadataAttribute` for it (**line 179**).
- **Block Hash Key**: In `TimeSeriesBlockHash`, TSIDs are hashed into ordinals (`tsidHash`) and combined with timestamps into groups (`finalHash`) (**`TimeSeriesBlockHash.java:44-45`**).

### Role in Queries

TSIDs serve as the fundamental grouping key for time-series aggregations. Every time-series aggregation (rate, over_time functions, etc.) must first be computed per-TSID. The two-phase aggregation pattern is:
1. **Phase 1**: Group by (_tsid, time_bucket) -- compute per-time-series results.
2. **Phase 2**: Group by user-specified keys -- aggregate across time series.

---

## 4. Time-Series Specific Functions

### Overview

All time-series aggregate functions extend `TimeSeriesAggregateFunction` (**`TimeSeriesAggregateFunction.java:26`**). They share these properties:
- Can only be used with the `TS` command (enforced by `Aggregate.checkTimeSeriesAggregates()` at **`Aggregate.java:252-266`**).
- Marked with `FunctionType.TIME_SERIES_AGGREGATE` (**`FunctionType.java:30`**).
- Must implement `perTimeSeriesAggregation()` which returns the aggregation to use in the first pass (per-TSID) aggregation.
- Some implement `TimestampAware` to receive `@timestamp`.

### Complete Function Catalog

#### Counter-based functions (require counter types):

| Function | File | Description | Counter Required | Returns |
|----------|------|-------------|-----------------|---------|
| `rate()` | `Rate.java:40` | Per-second average rate of increase of a counter | Yes (counter_long/int/double) | double |
| `irate()` | `Irate.java:46` | Instantaneous rate of increase | Yes | double |
| `increase()` | `Increase.java:46` | Absolute increase of a counter in time window | Yes | double |
| `delta()` | `Delta.java:47` | Change in value of a counter | Yes | double |
| `idelta()` | `Idelta.java:47` | Instantaneous delta | Yes | double |
| `deriv()` | `Deriv.java:47` | Per-second derivative | Yes | double |

#### Over-time functions (work on any numeric, applied per time-series):

| Function | File | Description |
|----------|------|-------------|
| `avg_over_time()` | `AvgOverTime.java:50` | Average within time bucket per TSID |
| `sum_over_time()` | `SumOverTime.java:53` | Sum within time bucket per TSID |
| `min_over_time()` | `MinOverTime.java:53` | Min within time bucket per TSID |
| `max_over_time()` | `MaxOverTime.java:53` | Max within time bucket per TSID |
| `count_over_time()` | `CountOverTime.java:51` | Count within time bucket per TSID |
| `count_distinct_over_time()` | `CountDistinctOverTime.java:44` | Count distinct within time bucket per TSID |
| `first_over_time()` | `FirstOverTime.java:55` | First value by timestamp per TSID |
| `last_over_time()` | `LastOverTime.java:56` | Last value by timestamp per TSID |
| `stddev_over_time()` | `StddevOverTime.java:37` | Standard deviation per TSID per bucket |
| `variance_over_time()` | `VarianceOverTime.java:37` | Variance per TSID per bucket |
| `percentile_over_time()` | `PercentileOverTime.java:34` | Percentile per TSID per bucket |
| `present_over_time()` | `PresentOverTime.java:49` | Whether data exists per TSID per bucket |
| `absent_over_time()` | `AbsentOverTime.java:41` | Whether data is absent per TSID per bucket |
| `histogram_merge_over_time()` | `HistogramMergeOverTime.java:44` | Merges exponential histograms/tdigests per TSID per bucket |

#### Implicit wrapping:

- `DefaultTimeSeriesAggregateFunction` (**`DefaultTimeSeriesAggregateFunction.java:26`**): Wraps bare field references in TS STATS. Delegates to `LastOverTime` for numerics, `HistogramMergeOverTime` for histograms. For example, `TS k8s | STATS max(cpu)` implicitly becomes `STATS max(last_over_time(cpu))`.

### Key Properties

- `requiredTimeSeriesSource()`: Returns `true` for counter-based functions (rate, increase, delta, irate, idelta, deriv). These require the time-series source operator's metadata blocks (`_ts_slice_index`, `_ts_future_max_timestamp`) (**`TimeSeriesAggregateFunction.java:50-52`**, **`Rate.java:151-153`**).
- The rate aggregator functions are at the compute level: `RateLongGroupingAggregatorFunction`, `RateIntGroupingAggregatorFunction`, `RateDoubleGroupingAggregatorFunction`.
- Window support: Functions can accept an optional `window` parameter (time_duration) for sliding-window aggregation.

---

## 5. Time-Series Grouping

### Two-Phase Aggregation Model

Time-series aggregation always follows a two-phase pattern, implemented by `TranslateTimeSeriesAggregate` (**`TranslateTimeSeriesAggregate.java:158-366`**):

**Phase 1 (per-time-series)**: Group by `_tsid` (and optionally `time_bucket`).
**Phase 2 (cross-time-series)**: Group by user-specified keys.

Example transformations from the Javadoc (**`TranslateTimeSeriesAggregate.java:60-156`**):

```
TS k8s | STATS max(rate(request))
->
TS k8s
| STATS rate_$1 = rate(request) BY _tsid
| STATS max(rate_$1)

TS k8s | STATS max(rate(request)) BY host
->
TS k8s
| STATS rate_$1=rate(request), DIMENSION_VALUES(host) BY _tsid
| STATS max(rate_$1) BY host=`DIMENSION_VALUES(host)`
```

### DimensionValues

When grouping by dimension fields in the second phase, `TranslateTimeSeriesAggregate.addAttribute()` uses `DimensionValues` (if available) or `Values` (**`TranslateTimeSeriesAggregate.java:424-429`**). `DimensionValues` is a special aggregate that optimizes for dimension fields that are constant per TSID.

### PackDimension/UnpackDimension

For dimension fields, the rule uses `PackDimension` and `UnpackDimension` (**`TranslateTimeSeriesAggregate.java:392-411`**) to pack the first-pass dimension values into a single value per group and unpack them in the second pass. This avoids expensive multi-value handling.

### TimeSeriesGroupByAll

**`TimeSeriesGroupByAll.java:35-111`** implements "group by all dimensions" logic. When a TS STATS has only time-series aggregate functions (no regular aggregates) and no explicit dimension grouping, it wraps the aggregate expressions with `Values()` and adds a `TimeSeriesWithout(List.of())` grouping (meaning "all dimensions"). This is the equivalent of PromQL's "group by all labels".

### InsertDefaultInnerTimeSeriesAggregate

**`InsertDefaultInnerTimeSeriesAggregate.java:57-152`** ensures that every field reference inside a `TimeSeriesAggregate` is wrapped in a `TimeSeriesAggregateFunction`. For example, if you write `TS k8s | STATS max(cpu)`, the bare `cpu` reference gets wrapped in `LastOverTime(cpu)` -- because in time-series context, you need to pick one value per time bucket per TSID before aggregating.

Special handling:
- `Last(field, sort)` gets sort wrapped in `MaxOverTime(sort)` and field in `DefaultTimeSeriesAggregateFunction(field)` (**line 89-97**).
- `First(field, sort)` gets sort wrapped in `MinOverTime(sort)` and field in `FirstOverTime(field)` (**line 98-106**).

### TimeSeriesWithout

**`TimeSeriesWithout.java:38`** is a grouping function that represents "group by all dimensions except ...". When called with no arguments, it means "group by all dimensions". This is translated by `TranslateTimeSeriesWithout` during optimization.

### ExtractDimensionFieldsAfterAggregation

**`ExtractDimensionFieldsAfterAggregation.java:59-179`** is a physical optimizer rule that moves `VALUES(dimension_field)` computations to execute AFTER the aggregation. Since dimension values are constant per TSID, they only need to be loaded once per group (not for every document). It replaces VALUES with `FirstDocId` (to get any doc ID from the group) and then loads dimension values from that single doc.

---

## 6. @timestamp Requirement

### Mandatory for TS STATS

The `TimeSeriesAggregate` plan node carries a `timestamp` expression (**`TimeSeriesAggregate.java:51`**). This is set to an `UnresolvedTimestamp` at parse time (**`LogicalPlanBuilder.java:621`**) and resolved during analysis to the actual `@timestamp` attribute from the index.

The post-analysis verification at **`TimeSeriesAggregate.java:266-278`** enforces:
```java
if ((timestamp instanceof TypedAttribute) == false || timestamp.dataType().isDate() == false) {
    // error: requires @timestamp of type date or date_nanos
}
```

### TimestampAware Interface

**`TimestampAware.java:17-19`** is a marker interface. Functions like `Rate`, `LastOverTime`, `FirstOverTime`, `TBucket` implement it. The analyzer resolves their `@timestamp` parameter during analysis.

### TBucket Function

**`TBucket.java:54-60`** is a grouping function that splits `@timestamp` into time buckets. It only works on the `@timestamp` field (enforced by `TimestampAware`). It can accept:
- A fixed duration: `TBUCKET(1m)` -- equivalent to `BUCKET(@timestamp, 1m)`.
- A target count: `TBUCKET(50)` -- chooses bucket size based on the time range.
- Explicit bounds: `TBUCKET(50, '2025-01-01', '2025-01-31')`.

### Implicit @timestamp Sort

For TS queries without explicit SORT or STATS, the `AddImplicitTimestampSort` rule (**`Analyzer.java:2017-2061`**) injects `ORDER BY @timestamp DESC` so the most recent data points are returned first.

### Commands Blocked Before First TS STATS

`TimeSeriesAggregate.postAnalysisVerification()` (**`TimeSeriesAggregate.java:181-265`**) blocks these commands between TS source and first STATS:
- `SORT` (would break time-series ordering)
- `LIMIT` (would lose data)
- `LOOKUP JOIN` 
- `ENRICH`
- `CHANGE_POINT`
- `MV_EXPAND`

Also blocked:
- `COUNT(*)` -- not meaningful for time-series (**line 286-291**)
- Grouping by metric fields (**line 184-196**)

---

## 7. Data Ordering Requirements

### TSDB Index Sort Order

TSDB indices in Elasticsearch are sorted by `(_tsid ASC, @timestamp DESC)`. This is enforced by the index settings. The data within each shard arrives in this order, which enables:

1. Sequential processing of all data points for a single time series.
2. Efficient rate/counter computation (needs all points from one TSID together).
3. TSID prefix-based partitioning.

### TimeSeriesSourceOperator

**`TimeSeriesSourceOperator.java:29`** extends `LuceneSourceOperator` with two key differences:

1. **Extra metadata blocks** (**line 79-88**): Emits `_ts_slice_index` (which slice/partition this page belongs to) and `_ts_future_max_timestamp` (set to `Long.MAX_VALUE`).

2. **Constrained partitioning** (**line 103-109**): Cannot use SEGMENT or DOC partitioning (which would break the TSID ordering). Falls back to AUTO, which resolves to either SHARD or TIME_SERIES partitioning.

3. **Page size alignment** (**line 96-101**): Pages are aligned to `CHUNK_SIZE = 128` (matching the TSDB codec's numeric block size) for bulk loading, capped at `MAX_TARGET_PAGE_SIZE = 2048`.

### TimeSeriesBlockHash

**`TimeSeriesBlockHash.java:39-358`** is the optimized block hash for time-series aggregation. Key properties:

- Assumes input data is sorted by (tsid, timestamp).
- Expects TSID as a `BytesRefVector` (no nulls, no multi-values) and timestamp as a `LongVector` (**lines 83-91**).
- Exploits ordinal-based TSID vectors to avoid redundant hash lookups (**lines 149-184**).
- Maintains two hash tables: `tsidHash` (BytesRef -> ordinal) and `finalHash` (tsid_ordinal, timestamp -> group_id) (**lines 44-45**).
- Tracks `minTimestamp` and `maxTimestamp` for rounding optimization (**lines 48-49**).

### Why Ordering Matters

The time-series aggregation pipeline depends on sorted input because:
1. **Rate computation**: The rate aggregator must see all data points for a TSID in timestamp order to detect counter resets.
2. **Block hash efficiency**: `TimeSeriesBlockHash` skips hash lookups when consecutive rows have the same TSID.
3. **Partition correctness**: Parallel drivers process disjoint sets of TSIDs (via prefix partitioning), guaranteeing each TSID is processed by exactly one driver.

---

## 8. Downsampling in ES|QL

### Status: Not Directly in ES|QL

There is **no downsampling implementation within the ES|QL codebase** (`x-pack/plugin/esql/src/main/java/`). The term "downsample" appears only in test fixtures and test utilities:

- `CsvTestsDataLoader.java` -- loads downsampled test data
- `EsqlTestUtils.java` -- references downsampled index patterns
- Various test files that test querying downsampled indices

### How Downsampling Works

Downsampling is an Elasticsearch server-level feature (in `x-pack/plugin/downsample/`). It creates pre-aggregated indices:
- A downsampled index stores pre-computed aggregates (min, max, sum, count, etc.) for each time bucket per TSID.
- The index is still `IndexMode.TIME_SERIES` but with reduced granularity.

### ES|QL's Interaction

ES|QL can query downsampled indices transparently using the TS command. The `aggregate_metric_double` field type (used in downsampled indices) is enabled when `IndexMode.TIME_SERIES` is detected (**`PreAnalyzer.java:99-101`**):
```java
if (mode == IndexMode.TIME_SERIES) {
    useAggregateMetricDoubleWhenNotSupported.set(true);
}
```

The `InsertFromAggregateMetricDouble` and `ImplicitCastAggregateMetricDoubles` analyzer rules handle aggregate_metric_double fields, which are the mechanism through which downsampled data is properly aggregated.

---

## 9. TSID Prefix Partitioning

### PartitionedDocValues

**`server/src/main/java/org/elasticsearch/index/codec/tsdb/PartitionedDocValues.java:25-66`** is the interface for partitioning doc values by a prefix (the TSID prefix bytes):

```java
public interface PartitionedDocValues {
    record PrefixPartitions(int numPartitions, int[] prefixes, int[] startDocs) {}
    PrefixPartitions prefixPartitions(PrefixPartitions reused) throws IOException;
    int prefixPartitionBits();
}
```

The `canPartitionByTsidPrefix()` static method (**line 52-65**) checks all leaf readers:
```java
static boolean canPartitionByTsidPrefix(IndexSearcher searcher) throws IOException {
    for (LeafReaderContext leafContext : searcher.getLeafContexts()) {
        var sortedDV = leafContext.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
        if (sortedDV == null) continue; // empty segment
        if (sortedDV instanceof PartitionedDocValues partition && partition.prefixPartitionBits() > 0) continue;
        return false;
    }
    return true;
}
```

### TimeSeriesSourceOperator Partitioning Strategy

**`TimeSeriesSourceOperator.java:112-123`** determines the partitioning strategy:

```java
private static DataPartitioning.AutoStrategy partitioningStrategy(IndexedByShardId<? extends ShardContext> contexts) {
    try {
        for (ShardContext ctx : contexts.iterable()) {
            if (PartitionedDocValues.canPartitionByTsidPrefix(ctx.searcher()) == false) {
                return limit -> q -> LuceneSliceQueue.PartitioningStrategy.SHARD;
            }
        }
    } catch (IOException e) {
        throw new UncheckedIOException(e);
    }
    return limit -> q -> LuceneSliceQueue.PartitioningStrategy.TIME_SERIES;
}
```

If ALL shards support TSID prefix partitioning, the strategy is `TIME_SERIES`; otherwise, it falls back to `SHARD`.

### TimeSeriesPartitioner

**`LuceneSliceQueue.java:476-570`** implements the `TimeSeriesPartitioner`:

1. For each leaf reader, gets the sorted doc values for `_tsid` as `PartitionedDocValues`.
2. Extracts prefix partitions (each prefix represents a group of TSIDs that share the same prefix byte).
3. Groups leaves by the **first byte of the prefix** (shifted right by `prefixBitsShift`).
4. Within each first-byte group, combines prefix groups into slices targeting `taskConcurrency` slices.

This ensures:
- All documents for a TSID are in the same slice (critical for rate/counter correctness).
- Slices are balanced by document count.
- Per-leaf processing cost is bounded by `MAX_DOCS_PER_SLICE` (250,000).

### LuceneSliceQueue PartitioningStrategy.TIME_SERIES

**`LuceneSliceQueue.java:324-333`**:
```java
TIME_SERIES(3) {
    @Override
    List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency) {
        try {
            return new TimeSeriesPartitioner().partition(searcher.getLeafContexts(), taskConcurrency, MAX_DOCS_PER_SLICE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
};
```

### Window Bucket Expansion

`TimeSeriesAggregationOperator.expandWindowBuckets()` (**`TimeSeriesAggregationOperator.java:324-360`**) handles sliding-window aggregation by creating additional groups. For each existing group (tsid, bucket), it fills in missing buckets between `(timestamp - window, timestamp)` so the window aggregation produces correct results:

```java
while (bucket < endTimestamp) {
    if (tsBlockHash.addExtraGroup(tsid, bucket) >= 0) {
        expandingGroups.addGroup(Math.toIntExact(groupId));
    }
    bucket = optimizedTimeBucket.nextRoundingValue(bucket);
}
```

### Output Filtering for Sub-Bucketing

When the internal bucket is finer-grained than the user-visible bucket (for non-multiple windows), `TimeSeriesAggregationOperator.emit()` (**line 136-211**) filters output to only emit groups aligned to the output bucket boundary via `computeOutputAlignedPositions()` (**line 222-238**).

---

## 10. External Source Restrictions

### Verifier Checks

The Verifier blocks time-series aggregates on non-TS sources at **`Aggregate.java:252-266`**:

```java
protected void checkTimeSeriesAggregates(Failures failures) {
    Holder<Boolean> isTimeSeries = new Holder<>(false);
    child().forEachDown(p -> {
        if (p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES) {
            isTimeSeries.set(true);
        }
    });
    if (isTimeSeries.get()) {
        return;
    }
    forEachExpression(
        TimeSeriesAggregateFunction.class,
        r -> failures.add(fail(r, "time_series aggregate[{}] can only be used with the TS command", r.sourceText()))
    );
}
```

This check walks the plan tree looking for `EsRelation` with `IndexMode.TIME_SERIES`. External sources (which use `ExternalRelation`, not `EsRelation`) will never match, so **any use of time-series aggregate functions with EXTERNAL will be rejected**.

### TimeSeriesWithout Restriction

**`Verifier.java:372-382`**:
```java
private static void checkTimeSeriesWithoutOnlyInTimeSeriesAggregate(LogicalPlan p, Failures failures) {
    if (p instanceof Aggregate agg && (p instanceof TimeSeriesAggregate) == false) {
        for (Expression g : agg.groupings()) {
            if (Alias.unwrap(g) instanceof TimeSeriesWithout) {
                failures.add(fail(g, "WITHOUT is only supported in time-series queries (i.e. TS | ...) at the moment"));
            }
        }
    }
}
```

### TS_INFO and METRICS_INFO Restriction

Both `TsInfo.java:127` and `MetricsInfo.java:127` verify they have a TS source:
```java
boolean hasTsSource = child().anyMatch(p -> p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES);
```

External sources will fail this check. Tests confirm this at **`AnalyzerExternalTests.java:88-105`**.

### Parser-Level Restriction

The parser at **`LogicalPlanBuilder.java:611-614`** only creates `TimeSeriesAggregate` when it detects `UnresolvedRelation` with `IndexMode.TIME_SERIES`:
```java
boolean hasTimeSeries = input.anyMatch(p -> p instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.TIME_SERIES);
```
External sources use a different plan node (`UnresolvedExternalRelation`), so they never trigger TimeSeriesAggregate creation.

### Summary of Restrictions

| Feature | Check Location | Blocked For External? |
|---------|---------------|----------------------|
| `rate()`, `increase()`, `delta()`, etc. | `Aggregate.java:252-266` | Yes |
| `*_over_time()` functions | `Aggregate.java:252-266` | Yes |
| `WITHOUT()` grouping | `Verifier.java:372-382` | Yes |
| `TS_INFO` | `TsInfo.java:127` | Yes |
| `METRICS_INFO` | `MetricsInfo.java:127` | Yes |
| `TBUCKET()` | Only meaningful in TS context | Effectively yes |
| `TimeSeriesAggregate` plan creation | `LogicalPlanBuilder.java:611` | Yes (parser never creates it) |

---

## 11. Summary Architecture Diagram

```
User Query: TS k8s | STATS max(rate(request)) BY host, TBUCKET(5m)

                        PARSING
                          |
                 LogicalPlanBuilder.java
                 visitTimeSeriesCommand -> EsRelation(indexMode=TIME_SERIES)
                 visitStatsCommand      -> TimeSeriesAggregate
                          |
                       ANALYSIS
                          |
      InsertDefaultInnerTimeSeriesAggregate  (wraps bare fields in last_over_time)
      TimeSeriesGroupByAll                  (handles bare over-time aggs)
      AddImplicitTimestampSort              (adds ORDER BY @timestamp DESC if no STATS)
      AddImplicitLimit                      (10K for TS aggs, 1K otherwise)
                          |
                     OPTIMIZATION
                          |
      TranslateTimeSeriesAggregate:
        Phase 1: STATS rate_$1=rate(request), DIMENSION_VALUES(host) BY _tsid, bucket(@ts,5m)
        Phase 2: STATS max(rate_$1) BY host=DIMENSION_VALUES(host), bucket(@ts,5m)
                          |
                   PHYSICAL PLANNING
                          |
      TimeSeriesAggregateExec -> TimeSeriesAggregationOperator
      EsQueryExec(TIME_SERIES) -> TimeSeriesSourceOperator
                          |
               LOCAL PHYSICAL OPTIMIZATION
                          |
      ExtractDimensionFieldsAfterAggregation (loads dimensions post-agg)
      ReplaceSourceAttributes (adds _ts_slice_index, _ts_future_max_timestamp metadata)
                          |
                      EXECUTION
                          |
      TimeSeriesSourceOperator
        -> reads sorted (tsid, @timestamp) data
        -> uses TIME_SERIES partitioning (prefix-based)
        -> emits pages with metadata blocks
                          |
      TimeSeriesAggregationOperator
        -> uses TimeSeriesBlockHash
        -> computes per-TSID aggregations
        -> expands window buckets if needed
        -> filters output for sub-bucketing
                          |
      HashAggregationOperator (Phase 2)
        -> standard grouping by user keys
```

### Key Files Reference

| File | Purpose |
|------|---------|
| `EsqlBaseParser.g4:107-109` | TS grammar rule |
| `lexer/From.g4:14-15` | TS lexer token |
| `SourceCommand.java:15-18` | TS -> TIME_SERIES mapping |
| `LogicalPlanBuilder.java:900-902` | TS parser visit |
| `LogicalPlanBuilder.java:604-626` | STATS -> TimeSeriesAggregate |
| `EsRelation.java:32-192` | Index relation with indexMode |
| `TimeSeriesAggregate.java:40-319` | Logical plan for TS aggregation |
| `TimeSeriesAggregateExec.java:33-201` | Physical plan for TS aggregation |
| `TranslateTimeSeriesAggregate.java:158-544` | Two-phase translation rule |
| `InsertDefaultInnerTimeSeriesAggregate.java:57-152` | Implicit wrapping rule |
| `TimeSeriesGroupByAll.java:35-111` | Group-by-all-dimensions rule |
| `ExtractDimensionFieldsAfterAggregation.java:59-179` | Post-agg dimension loading |
| `TimeSeriesAggregateFunction.java:26-76` | Base class for TS agg functions |
| `Rate.java:40-154` | Rate function |
| `DefaultTimeSeriesAggregateFunction.java:26-118` | Implicit TS agg wrapper |
| `LastOverTime.java:45-88` | Default over-time function |
| `TimeSeriesSourceOperator.java:29-124` | Lucene source for TS data |
| `TimeSeriesAggregationOperator.java:49-584` | Compute-level TS aggregation |
| `TimeSeriesBlockHash.java:39-358` | Optimized block hash for (tsid, ts) |
| `LuceneSliceQueue.java:476-570` | TSID prefix partitioner |
| `PartitionedDocValues.java:25-66` | TSID prefix partition interface |
| `TsidBuilder.java:35-260` | TSID construction from dimensions |
| `TimeSeriesIdFieldMapper.java:52` | TSID field mapper |
| `DataType.java:376-381` | TSID_DATA_TYPE definition |
| `TimestampAware.java:17-19` | @timestamp marker interface |
| `TBucket.java:54-60` | Time bucket grouping function |
| `TimeSeriesWithout.java:38` | WITHOUT grouping function |
| `TemporalityAttribute.java:29-101` | TSDB temporality attribute |
| `EsqlSession.java:1354-1363` | TIME_SERIES query filter |
| `PreAnalyzer.java:99-101` | aggregate_metric_double enabling |
| `Analyzer.java:2017-2061` | Implicit timestamp sort |
| `Analyzer.java:1964-1971` | TS-aware limit |
| `Aggregate.java:252-266` | TS agg function validation |
| `Verifier.java:372-382` | WITHOUT validation |
| `EsPhysicalOperationProviders.java:465-474` | TS source operator selection |
| `EsQueryExec.java:43-46` | TIME_SERIES_SOURCE_FIELDS metadata |
