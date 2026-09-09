---
applies_to:
  stack: ga
  serverless: ga
navigation_title: "Histogram fields"
---

# Query histogram fields in {{esql}} [esql-histogram-fields]

Histogram fields store pre-aggregated value distributions rather than individual data points.
They are useful anywhere you have high-volume numeric data and need percentile, average, or
count queries without the storage cost of keeping every raw value. The most common source is
observability pipelines: an OpenTelemetry agent can record thousands of HTTP request durations
per minute and ship them as a single exponential histogram per collection interval.

This page explains the histogram field types available in {{esql}}, how to query them, and
how to create value-distribution histograms from them.

## Why histogram fields exist

When you need to understand the distribution of values across a large population, storing
every individual observation is often impractical. A histogram compresses those observations
into a compact summary that still supports percentile queries, averages, and counts. This
makes histograms useful anywhere you have high-volume numeric data and care about the shape
of the distribution, not just a single aggregate.

For example, knowing that your average HTTP response time is 3ms tells you little about
outliers. A histogram preserves enough detail to answer questions like "what is the 99th
percentile?" without storing every request.

### Histograms in metrics and observability

Histograms are widely used in metrics pipelines.
[OpenTelemetry](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram)
and [Prometheus](https://prometheus.io/docs/specs/native_histograms/) both define exponential
histogram formats that dynamically adapt bucket boundaries to the data, giving a guaranteed
upper bound on relative error for every percentile. Classic Prometheus-style histograms use
fixed buckets instead, which requires knowing the value distribution up front.

For a deeper explanation of how exponential bucketing works, refer to the
[OpenTelemetry exponential histograms introduction](https://opentelemetry.io/blog/2022/exponential-histograms/).

## Histogram field types

{{esql}} recognizes three histogram field types:

`exponential_histogram`
:   The recommended type for new data. Uses an exponential bucketing scheme that dynamically
    adapts to your data and provides a guaranteed upper bound on relative error for every
    percentile. This is the native format used by
    [OpenTelemetry exponential histograms](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram)
    and maps directly to
    [Prometheus native histograms](https://prometheus.io/docs/specs/native_histograms/).
    Most aggregation functions work natively with this type.

`tdigest`
:   Stores a [T-Digest](https://github.com/tdunning/t-digest) data structure. Provides good
    accuracy for extreme percentiles (like p99) but loses accuracy for mid-range percentiles
    like the median. Before {{es}} 9.4, OpenTelemetry histograms were converted to T-Digest
    for storage. Use this type when your data is already stored as T-Digest values.

`histogram` (legacy)
:   The original [histogram field type](/reference/elasticsearch/mapping-reference/histogram.md).
    This type can represent either T-Digest or HDR histogram data, but {{esql}} only supports
    T-Digest. Cast `histogram` fields with `::tdigest` to query them, or with
    `::exponential_histogram` to convert them for use alongside newer data. Refer to
    [Cast between histogram types](#cast-between-histogram-types).

To check which type a metric uses, run
[`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md) against the
data stream.

### Choosing between exponential histogram and T-Digest

For new data, use `exponential_histogram`. It provides a guaranteed upper bound on relative
error for every percentile and eliminates the lossy conversion step that T-Digest requires.
Percentile computation on exponential histograms is also more efficient at query time.

Use `tdigest` only when your data is already stored in that format. T-Digest provides good
accuracy at the tails of the distribution (p99, p99.9) but lower accuracy for mid-range
percentiles like the median. It also only supports delta temporality (not cumulative).

## Aggregate histogram fields

Apply regular [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md)
directly to histogram fields. Aggregations act as if you were running them on the raw
observations that produced the histogram. For example, `COUNT(responseTime)` returns the
total number of HTTP requests whose response times were recorded, not the number of
histogram documents. The following functions support histogram inputs:

| Function | What it returns for histogram fields |
|---|---|
| [`COUNT`](/reference/query-languages/esql/functions-operators/aggregation-functions/count.md) | Total number of values recorded across all histograms |
| [`SUM`](/reference/query-languages/esql/functions-operators/aggregation-functions/sum.md) | Sum of all recorded values |
| [`AVG`](/reference/query-languages/esql/functions-operators/aggregation-functions/avg.md) | Average of all recorded values (computed as `SUM / COUNT`) |
| [`MIN`](/reference/query-languages/esql/functions-operators/aggregation-functions/min.md) | Minimum recorded value |
| [`MAX`](/reference/query-languages/esql/functions-operators/aggregation-functions/max.md) | Maximum recorded value |
| [`MEDIAN`](/reference/query-languages/esql/functions-operators/aggregation-functions/median.md) | Estimated median (50th percentile) |
| [`PERCENTILE`](/reference/query-languages/esql/functions-operators/aggregation-functions/percentile.md) | Estimated value at a given percentile |
| [`FIRST`](/reference/query-languages/esql/functions-operators/aggregation-functions/first.md) / [`EARLIEST`](/reference/query-languages/esql/functions-operators/aggregation-functions/earliest.md) | First histogram value by sort order / timestamp {applies_to}`stack: ga 9.5` |
| [`LAST`](/reference/query-languages/esql/functions-operators/aggregation-functions/last.md) / [`LATEST`](/reference/query-languages/esql/functions-operators/aggregation-functions/latest.md) | Last histogram value by sort order / timestamp {applies_to}`stack: ga 9.5` |

For example, to calculate the count, average, and 99th-percentile duration of garbage collection
events per action:

```esql
FROM metrics-*
| STATS count = COUNT(jvm.gc.duration),
        avg = AVG(jvm.gc.duration),
        p99 = PERCENTILE(jvm.gc.duration, 99)
  BY jvm.gc.action
```

Because `PERCENTILE` works on the histogram directly, you can query any percentile at
runtime without having to pre-define bucket boundaries at index time. This is a key advantage
of the `exponential_histogram` type over classic fixed-bucket approaches.

### Using `TS` with histogram fields

[`TS`](/reference/query-languages/esql/commands/ts.md) is the recommended source command
for time series data. Before your aggregation runs, `TS` performs an implicit per-series
merge: all histogram documents within each time bucket and series are combined into a single histogram,
and the metric's temporality (delta or cumulative) is respected during the merge. The outer
aggregation then operates on these merged per-series histograms.

You do not need a time series aggregation function like
[`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md) or
[`AVG_OVER_TIME`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/avg_over_time.md)
to query histogram fields. Use the regular aggregation functions from the table above, combined
with `TBUCKET` for time-based grouping:

```esql
TS metrics-*
| WHERE TRANGE(1 hour)
| STATS count = COUNT(jvm.gc.duration),
        avg = AVG(jvm.gc.duration),
        p99 = PERCENTILE(jvm.gc.duration, 99)
  BY jvm.gc.action, TBUCKET(5 minutes)
```

The [time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
(`*_OVER_TIME` variants) also accept histogram inputs for windowed aggregation within a time
series. You can use these when you need finer control over the aggregation window, for example
[`FIRST_OVER_TIME`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/first_over_time.md)
or [`LAST_OVER_TIME`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/last_over_time.md)
to select the histogram from a specific point in the
time series. For most use cases, the regular aggregation functions are sufficient.

### Using `FROM` with histogram fields

[`FROM`](/reference/query-languages/esql/commands/from.md) also supports histogram
aggregations. Unlike `TS`, `FROM` does not perform an implicit per-series merge or handle
metric temporality. Each histogram document is aggregated directly.

For additive aggregations like `COUNT` and `SUM`, the results are mathematically equivalent
to `TS`. For distribution-sensitive aggregations like `PERCENTILE` and `MEDIAN`, `TS` may
produce more accurate results because it merges histograms per series before aggregating
across series.

```esql
FROM metrics-*
| STATS count = COUNT(responseTime),
        avg = AVG(responseTime),
        p99 = PERCENTILE(responseTime, 99)
  BY instance
```

## Create value-distribution histograms

```{applies_to}
stack: ga 9.6
```

To break down the values recorded in a histogram field into fixed-width buckets, combine
[`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/bucket.md)
with [`COUNT`](/reference/query-languages/esql/functions-operators/aggregation-functions/count.md).

When applied to a histogram field, `BUCKET` returns `double_range` buckets instead of single
values. A histogram that spans several buckets contributes a row to each of them. Pass the
bucket as the second argument to `COUNT` to count the histogram values that fall into each
bucket:

```esql
FROM exp_histo_sample
| WHERE instance == "instance-0"
| STATS count = COUNT(responseTime, bucket) BY bucket = BUCKET(responseTime, 1)
| SORT RANGE_MIN(bucket)
```

| count:long | bucket:double_range |
| --- | --- |
| 8723 | 0.0..1.0 |
| 112 | 1.0..2.0 |
| 1 | 2.0..3.0 |
| 2 | 3.0..4.0 |
| 2 | 5.0..6.0 |
| 1 | 6.0..7.0 |

Use [`RANGE_MIN`](/reference/query-languages/esql/functions-operators/date-time-functions/range_min.md)
or [`RANGE_MAX`](/reference/query-languages/esql/functions-operators/date-time-functions/range_max.md)
to extract the start or end of each `double_range` bucket for sorting or further computation.

::::{note}
Histograms record approximate value distributions, so the counts per bucket are estimates.
::::

The same pattern works for `tdigest` fields. [Cast the field](#cast-between-histogram-types) if needed:

```esql
FROM histogram_timeseries_index
| WHERE instance == "instance-0"
| STATS count = COUNT(responseTime::tdigest, bucket) BY bucket = BUCKET(responseTime::tdigest, 1)
| SORT RANGE_MIN(bucket)
```

| count:long | bucket:double_range |
| --- | --- |
| 8733 | 0.0..1.0 |
| 100 | 1.0..2.0 |
| 4 | 2.0..3.0 |
| 2 | 3.0..4.0 |
| 0 | 4.0..5.0 |
| 0 | 5.0..6.0 |
| 2 | 6.0..7.0 |

A T-Digest does not track which ranges between its centroids are empty. `BUCKET` returns every
bucket between the smallest and the largest centroid, so some buckets may show a count of `0`.
Exponential histograms skip empty buckets.

## Cast between histogram types

Use the [casting operator (`::`)](/reference/query-languages/esql/functions-operators/operators.md#esql-cast-operator) to convert between histogram types inline:

- `field::exponential_histogram` converts to an exponential histogram. This is the recommended
  default.
- `field::tdigest` converts to a T-Digest. Use this when you know the data was originally stored
  as T-Digest centroids.

```esql
FROM metrics-*
| STATS avg = AVG(response_time::exponential_histogram) BY instance
```

You can also use the explicit conversion functions
[`TO_EXPONENTIAL_HISTOGRAM`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_exponential_histogram.md)
and [`TO_TDIGEST`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_tdigest.md)
in an [`EVAL`](/reference/query-languages/esql/commands/eval.md) step. Both functions accept all three histogram types as input and return the
target type (identity conversion is a no-op).

## Query historical data alongside new data

Before {{es}} 9.4, OpenTelemetry histograms were converted to T-Digest for storage in the
`histogram` field type. After upgrading, newer indices use `exponential_histogram` while
older indices still contain `histogram` data.

Thanks to [union types](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types),
you can query across both by adding a `::exponential_histogram` cast:

```esql
FROM metrics-*
| STATS avg = AVG(jvm.gc.duration::exponential_histogram) BY jvm.gc.action
```

When this query encounters `histogram` fields, it converts them to exponential histograms.
When it encounters `exponential_histogram` fields, the cast has no effect. If you are building
queries or dashboards that may run on pre-9.4 data, adding `::exponential_histogram` casts is
recommended.

Use [`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md) to inspect
which field types are in use across backing indices.

## Supported operators and expressions

Histogram fields support the following operators and expressions:

- **Equality**: `==` and `!=` compare two histogram values of the same type. Cross-type
  comparison (for example, `exponential_histogram == tdigest`) is not supported.
- **Null checks**: `IS NULL` and `IS NOT NULL` work on all histogram types.
- **Conditional expressions**: [`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md)
  and [`COALESCE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/coalesce.md)
  can return histogram values.

## Ingest OpenTelemetry exponential histograms

To send OpenTelemetry exponential histograms directly to {{es}}, point your OTel SDK or agent
at the [{{es}} OTLP/HTTP endpoint](docs-content://manage-data/data-store/data-streams/tsds-ingest-otlp.md)
and configure the following environment variables:

```yaml
OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: delta
OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION: BASE2_EXPONENTIAL_BUCKET_HISTOGRAM
```

- **Temporality preference**: {{es}} supports both delta and cumulative
  ({applies_to}`stack: ga 9.5`) temporality for `exponential_histogram` fields. Delta
  temporality (where the histogram is cleared after each export) is recommended for most use
  cases. `tdigest` fields support delta temporality only.
- **Default histogram aggregation**: By default, OpenTelemetry exports histograms in the
  classic fixed-bucket format. Setting this to `BASE2_EXPONENTIAL_BUCKET_HISTOGRAM` uses
  exponential histograms instead.

The histograms are stored natively as `exponential_histogram` fields and are queryable
immediately in {{esql}}.

## Limitations

- Sorting on histogram fields is not allowed. Use [`SORT`](/reference/query-languages/esql/commands/sort.md) on aggregated results (like `RANGE_MIN(bucket)`) instead of on the histogram field itself.
- [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md) does not accept histogram fields. Use other aggregation functions on the histogram directly.
- [`VALUES`](/reference/query-languages/esql/functions-operators/aggregation-functions/values.md) does not accept histogram types.
- Multivalue functions like [`MV_FIRST`](/reference/query-languages/esql/functions-operators/mv-functions/mv_first.md), [`MV_LAST`](/reference/query-languages/esql/functions-operators/mv-functions/mv_last.md), and [`MV_COUNT`](/reference/query-languages/esql/functions-operators/mv-functions/mv_count.md) reject histogram fields.
- Counts and percentiles derived from histogram fields are estimates because the underlying data structures store distributions, not exact values.
- [`TO_STRING`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_string.md) works on `exponential_histogram` and `histogram` fields but does not currently support `tdigest`.

## Further reading

- [OTel histograms: Working with ES|QL histogram metrics](https://www.elastic.co/search-labs/blog/otel-histogram-metrics-esql):
  A walkthrough of querying OpenTelemetry exponential histograms in {{esql}}, including
  percentile analysis of JVM garbage collection metrics.
- [Work with histogram metrics](/reference/query-languages/esql/commands/ts.md#work-with-histogram-metrics):
  Histogram-specific guidance in the `TS` command reference.
- [Exponential histogram field type](/reference/elasticsearch/mapping-reference/exponential-histogram.md):
  Mapping reference for the `exponential_histogram` field type.
- [Downsampling time series data](docs-content://manage-data/data-store/data-streams/downsampling-time-series-data-stream.md):
  How histogram fields are preserved during downsampling.
- [Create a histogram from regular data](/reference/query-languages/esql/esql-getting-started.md#esql-getting-started-histogram):
  Use `BUCKET` on plain numeric or date fields to group rows into buckets.
- [`BUCKET` function reference](/reference/query-languages/esql/functions-operators/grouping-functions/bucket.md):
  Full syntax and examples for the `BUCKET` grouping function.
