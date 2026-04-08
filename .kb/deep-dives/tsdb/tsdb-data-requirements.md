# TSDB / Time-Series Data Requirements in Elasticsearch

## 1. Index Settings for TSDB

An index becomes a time-series index via a single setting:

```
"index.mode": "time_series"
```

**Required companion settings:**

- `index.routing_path` — must be non-empty. Lists field name patterns that identify which dimension fields are used for routing. Alternatively, `index.dimensions` (a private setting populated automatically for data stream backing indices) can satisfy this requirement.
- `index.time_series.start_time` — lower bound for `@timestamp` in this index (final, set at creation).
- `index.time_series.end_time` — upper bound for `@timestamp` (dynamic, must be > start_time).

**Prohibited/forced settings:**

- Custom `index.sort.field`, `index.sort.order`, `index.sort.mode`, `index.sort.missing` — all **forbidden**. TSDB forces its own sort (see section 4).
- `index.routing_partition_size` — must be 1 (default).
- `_source` — cannot be disabled. Defaults to **synthetic** source mode.
- Custom `_routing` on CRUD operations — forbidden ("routing is forbidden on CRUD operations that target indices in time_series mode").
- Aliases with index_routing or search_routing — forbidden.

**Automatically enabled behaviors:**

- TSDB doc values codec (`useTimeSeriesDocValuesCodec() = true`) — specialized delta/GCD/offset encoding pipeline for numeric doc values.
- ES 8.12 postings format (`useEs812PostingsFormat() = true`).
- `_tsid` metadata field — auto-generated from dimension fields.
- `_ts_routing_hash` metadata field — auto-generated for shard routing.
- `_id` field uses `TsidExtractingIdFieldMapper` — _id is derived from _tsid + @timestamp, not user-provided.

**Source:** `server/src/main/java/org/elasticsearch/index/IndexMode.java` (TIME_SERIES enum constant, lines 131-253).

---

## 2. Required Mappings

### @timestamp (mandatory)

Every time-series index requires a `@timestamp` field of type `date` (or `date_nanos`). TSDB automatically provides a default mapping for `@timestamp` via `getDefaultMapping()`. The `DataStreamTimestampFieldMapper` metadata field is auto-enabled. Timestamp validation is enforced: the value must fall within `[index.time_series.start_time, index.time_series.end_time)`.

### Dimension fields (at least one required)

At least one field must be marked as a dimension (otherwise `routing_path` would be empty and validation fails). A field becomes a dimension via the mapping parameter:

```json
{ "type": "keyword", "time_series_dimension": true }
```

**Field types that support `time_series_dimension: true`:**

| Field Type | Dimension Support | Notes |
|---|---|---|
| `keyword` | Yes | Most common dimension type |
| `ip` | Yes | |
| `byte`, `short`, `integer`, `long` | Yes | |
| `boolean` | Yes | |
| `flattened` | Yes | Via `time_series_dimensions` parameter (list of sub-key names) |
| `geo_point` | **No** | Validator explicitly rejects it |
| `float`, `double`, `half_float`, `scaled_float` | **No** | Validator rejects: "not supported for a numeric field that is not byte, short, integer, or long" |

**Constraints on dimension fields:**

- Must have `doc_values: true` (validator enforces this).
- Cannot be in nested objects.
- Must match `index.routing_path` patterns — the routing_path validation requires matching fields to have `time_series_dimension: true`.
- Dimension field limit: controlled by `index.mapping.dimension_fields.limit` (default 21 on older indices, 512 on newer ones).

### Metric fields (optional but important)

Fields can be tagged with `time_series_metric` to declare their metric type:

```json
{ "type": "long", "time_series_metric": "counter" }
```

**Metric types:**

| Metric Type | Supported Aggs (for downsampling) | Supported Field Types |
|---|---|---|
| `gauge` | max, min, value_count, sum | All numeric types |
| `counter` | last_value only | All numeric types |
| `histogram` | (none for downsampling) | aggregate_metric_double |
| `position` | (none for downsampling) | geo_point only |

Metric type affects how fields are aggregated during downsampling (see section 5).

### pass_through objects

`PassThroughObjectMapper` can set `time_series_dimension: true` on all sub-fields, propagating dimension status to children. This is used by ECS data streams.

**Source:** `server/src/main/java/org/elasticsearch/index/mapper/TimeSeriesParams.java`, dimension/metric validation in `KeywordFieldMapper`, `NumberFieldMapper`, `IpFieldMapper`, `BooleanFieldMapper`, `FlattenedFieldMapper`, `GeoPointFieldMapper`.

---

## 3. TSID Construction

The `_tsid` (Time Series ID) is a binary value that uniquely identifies a time series within an index. It is constructed from all dimension field values for a document.

### Construction process

1. **Collect dimension values**: During document parsing, all fields marked `time_series_dimension: true` have their values collected into `RoutingPathFields` — a sorted map of `BytesRef(field_name) -> List<BytesReference(encoded_value)>`.

2. **Build the TSID**:
   - **Legacy path** (older indices, before `TIME_SERIES_ID_HASHING`): The TSID is a direct serialization of all dimension field names and values, sorted by field name. Multi-valued dimensions are **not supported** on legacy indices.
   - **Current path** (newer indices): The TSID is a **hash** of the routing path fields (`routingPathFields.buildHash()`). This produces a compact, fixed-size identifier.

3. **Storage**: The TSID is stored as a `SortedDocValuesField` (ordinal-based, de-duplicated across documents with the same TSID).

### _id derivation from _tsid

The document `_id` is **not user-provided** in TSDB. It is deterministically derived from:
- The routing hash (4 bytes)
- A MurmurHash3-128 of the _tsid (8 bytes, first half)
- The @timestamp (8 bytes, big-endian)

Total: 20 bytes, base64url-encoded.

**Synthetic _id** (newer indices with `TSDB_SYNTHETIC_ID_FEATURE_FLAG`): The _id is stored differently — `[_tsid bytes + (Long.MAX_VALUE - timestamp) + routing_hash]`. The `(Long.MAX_VALUE - timestamp)` encoding ensures that _id lexicographic order matches the index sort order (newest timestamps first within a TSID), which is critical for Lucene's doc values update mechanism.

**Implication**: Two documents with the same `_tsid` + `@timestamp` produce the same `_id`, making them effectively the same document (update-in-place semantics).

**Source:** `server/src/main/java/org/elasticsearch/index/mapper/TimeSeriesIdFieldMapper.java`, `server/src/main/java/org/elasticsearch/index/mapper/TsidExtractingIdFieldMapper.java`.

---

## 4. Data Ordering

### Index sort (mandatory, hard-coded)

TSDB indices have a **forced index sort** that cannot be overridden:

```
Sort fields: [_tsid ASC, @timestamp DESC]
```

Specifically:
- **Primary sort**: `_tsid` ascending (min mode)
- **Secondary sort**: `@timestamp` descending (max mode)
- Missing values: `_last` for both

This is defined in `IndexSortConfig.IndexSortConfigDefaults.TIME_SERIES_SORT`:
```java
TIME_SERIES_SORT = new SortDefault(
    List.of(TimeSeriesIdFieldMapper.NAME, DataStreamTimestampFieldMapper.DEFAULT_PATH),
    List.of("asc", "desc"),
    List.of("min", "max"),
    List.of("_last", "_last")
);
```

Custom index sort settings are **explicitly prohibited** — `IndexMode.TIME_SERIES.validateWithOtherSettings()` rejects any attempt to set `index.sort.field`, `index.sort.order`, `index.sort.mode`, or `index.sort.missing`.

### How this ordering is enforced

1. **At indexing time**: Lucene's `IndexWriter` uses the index sort to merge segments. Documents within each segment are physically stored in (_tsid ASC, @timestamp DESC) order.

2. **At search time**: `TimeSeriesIndexSearcher` traverses documents in this order:
   - Groups all leaf readers (segments) by their current TSID.
   - Uses a priority queue to merge across segments, processing all documents for one TSID before moving to the next.
   - Within each TSID, documents are ordered by timestamp (descending — newest first).
   - This merged traversal is what enables time-series aggregations (rate, downsampling) to work correctly.

3. **Codec-level**: The TSDB doc values codec exploits this ordering for compression — delta encoding of timestamps and values within each TSID group yields excellent compression ratios.

### Why this order matters

- **Grouping**: All documents for the same time series are physically adjacent, enabling efficient streaming aggregation without hash tables.
- **Counter reset detection**: Processing timestamps in descending order (newest to oldest) allows detecting counter resets by comparing adjacent values.
- **Downsampling**: The `DownsampleShardIndexer` uses `TimeSeriesIndexSearcher` to traverse in TSID order, bucketing by time intervals.
- **_id synthesis**: The synthetic _id format encodes `(Long.MAX_VALUE - timestamp)` to match this sort order for Lucene's internal update mechanics.

**Source:** `server/src/main/java/org/elasticsearch/index/IndexSortConfig.java` (lines 118-128), `server/src/main/java/org/elasticsearch/search/aggregations/support/TimeSeriesIndexSearcher.java`.

---

## 5. Metric Types

### Gauge vs Counter

The `time_series_metric` mapping parameter classifies numeric fields:

- **`gauge`**: A value that can go up or down (e.g., temperature, memory usage, queue depth). Supported downsampling aggregations: `max`, `min`, `value_count`, `sum`.

- **`counter`**: A monotonically increasing value that can only reset to zero (e.g., request count, bytes transferred). Supported downsampling aggregation: `last_value` only. Counters are special because:
  - You can't meaningfully sum or average counter values across time intervals.
  - The only meaningful operations are rate-of-change and reset-aware difference.
  - During downsampling, only the last value per interval is preserved.

- **`histogram`**: For pre-aggregated histogram data (aggregate_metric_double). Not scalar.

- **`position`**: For geo_point fields. Not scalar.

### Effect on aggregation behavior

The metric type determines:
1. **Which aggregations are valid during downsampling** — counters only support `last_value`.
2. **Which ES|QL data types are used** — counter fields map to `counter_long`, `counter_integer`, `counter_double` types in ES|QL, which restrict which functions can operate on them (only `rate()` and `increase()` are meaningful).
3. **How values are interpreted** — the `rate()` function requires counter fields and treats decreases as resets.

**Source:** `server/src/main/java/org/elasticsearch/index/mapper/TimeSeriesParams.java` (MetricType enum, lines 36-77).

---

## 6. What Makes rate() Work

### Data requirements for rate()

The `rate()` function computes the per-second average rate of increase of a counter field. It requires:

1. **Counter-typed field**: The field parameter must be typed as `counter_long`, `counter_integer`, or `counter_double`. The type resolution explicitly enforces this (`DataType.isCounter(dt)`).

2. **Time-series source command**: `rate()` declares `requiredTimeSeriesSource() = true`, meaning it can only be used under a `TS` source command in ES|QL. This ensures the data is from a TSDB index with proper sort order.

3. **Timestamps**: The rate aggregator receives timestamp values alongside counter values. The ESQL rate function is `TimestampAware` — it receives the `@timestamp` expression. The aggregator function receives 4 channels: values, timestamps, slice indices, and future max timestamps.

4. **Descending timestamp order within each TSID**: The data arrives sorted by (_tsid ASC, @timestamp DESC). The rate aggregator buffers data points in slices sorted in descending timestamp order and uses a priority queue to merge slices. Each slice's values are processed from newest to oldest.

### How rate() computes the result

The `RateLongGroupingAggregatorFunction` (and Double/Int variants) works in three phases:

1. **Raw accumulation**: Data points (timestamp, value) are buffered per group in slices. Each slice is sorted by descending timestamp. A new slice is created when timestamps are non-monotonic across page boundaries.

2. **Flush to ReducedState**: Slices for each group are merged via priority queue (newest first). The merge creates an `Interval(t1, v1, t2, v2)` — the first/last timestamp-value pairs. During merge, counter resets are tracked: if `value > prevValue` (remember: processing newest-to-oldest, so a counter going backwards means a reset occurred looking forward in time), the reset amount is accumulated.

3. **Final computation**:
   - Intervals from different shards are sorted and combined.
   - Cross-interval counter resets are detected.
   - The rate formula: `(lastValue - firstValue + resets) * dateFactor / (lastTimestamp - firstTimestamp)`
   - `dateFactor` is 1000.0 for millisecond timestamps (date) or 1,000,000,000.0 for nanosecond timestamps (date_nanos), converting to per-second rate.
   - Boundary interpolation/extrapolation follows the PromQL algorithm (credit noted in code).

### Intermediate state

The rate aggregator's intermediate state (for cross-shard aggregation) consists of:
- `timestamps` (long block) — pairs of (t1, t2) per interval
- `values` (long block) — pairs of (v1, v2) per interval
- `sampleCounts` (long) — number of data points seen
- `resets` (double) — accumulated reset compensation

**Source:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/Rate.java`, `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/aggregation/RateLongGroupingAggregatorFunction.java`, `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/AbstractRateGroupingFunction.java`.

---

## 7. Counter Resets

### What is a counter reset?

A counter reset occurs when a monotonically increasing counter drops to a lower value, typically because a service restarted. For example: `[100, 200, 300, 50, 100]` — the drop from 300 to 50 is a reset.

### How resets are detected and handled

**In the ESQL rate aggregator** (RateLongGroupingAggregatorFunction):

Data is processed in **descending** timestamp order (newest first). A reset is detected when a value is **greater** than the previous value in the processing order (which means it was smaller in chronological order — i.e., the counter went down then back up).

During the flush-and-merge phase:
```java
if (val > prevValue) {
    state.resets += val;
}
```
This accumulates the "jump-up" amounts as reset compensation.

During cross-interval combination (evaluateFinal):
```java
// Between intervals from different shards
if (prev.v1 > next.v2) {
    state.resets += prev.v1;
}
```

And in `deltaBetweenStates()`:
```java
// If the end value is smaller than the start value, a counter reset occurred.
// In this case, the delta is considered equal to the end value.
return (endValue >= startValue) ? endValue - startValue : endValue;
```

**In the analytics TimeSeriesRateAggregator** (non-ESQL, for search aggregations):

The `checkForResets()` method is simpler — it processes data in descending timestamp order (via `TimeSeriesIndexSearcher`). If `latestValue > currentStartValue` (i.e., going backwards in time the value increased, meaning it decreased forward in time — a reset), it adds the end value to a `resetCompensation` accumulator:
```java
private double checkForResets(double latestValue) {
    if (latestValue > currentStartValue) {
        // reset detected
        resetCompensation += currentEndValue;
        currentEndValue = latestValue;
    }
    return latestValue;
}
```

### Key insight for both implementations

Counter reset handling relies critically on the **(_tsid ASC, @timestamp DESC) sort order**. Because documents are processed newest-to-oldest within each time series, a "value going up" in the processing order means "value went down" in real time — which signals a reset. The reset compensation is then added to the rate calculation to account for the lost counter value.

Without the guaranteed sort order, counter resets could not be reliably detected in a single streaming pass.

---

## Summary: What the Data Must Look Like

For time-series to work in Elasticsearch, the data must satisfy these requirements:

| Requirement | Enforcement Point | Consequence if Missing |
|---|---|---|
| `index.mode = time_series` | Index creation | Not a TSDB index, no TSDB features |
| `@timestamp` field (date/date_nanos) | Default mapping + validation | Index creation fails |
| At least one dimension field (`time_series_dimension: true`) | `routing_path` validation | Index creation fails |
| `routing_path` matching dimension fields | `validateWithOtherSettings()` | Index creation fails |
| Timestamp within `[start_time, end_time)` | Document parsing | Document rejected |
| Physical sort order: (_tsid ASC, @timestamp DESC) | Index sort (Lucene segment merges) | Automatic, cannot be overridden |
| _id derived from _tsid + @timestamp | `TsidExtractingIdFieldMapper` | Automatic, user cannot set _id |
| Counter fields marked `time_series_metric: counter` | Mapping parameter | rate() won't work (wrong data type) |
| Counter fields typed as counter_long/integer/double | ES|QL type system | rate() rejects non-counter fields |
| No custom routing | Validation | Routing rejected at CRUD time |
| _source enabled | Validation | Index creation fails |
