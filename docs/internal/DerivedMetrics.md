# Derived Metrics

Derived metrics let a data stream emit compact operational metrics into a managed TSDS while documents are written to the source stream. The goal is to make common metric queries scale with the number of configured metrics and dimensions instead of the source write rate.

## Scope

The V1 metadata model supports:

- `data_stream_options.derived_metrics`
- built-in ingest metrics with default `["ingest.*"]`
- one default interval, overridable per metric, each writing to its own destination
- additive global and per-metric dimensions
- user-defined `counter`, `gauge`, and `histogram` metrics
- gauge aggregations including `first_value` and `last_value`
- script-free predicates and values

Counter, gauge and histogram metrics, along with the built-in ingest metrics, are collected and emitted at runtime.

## Data Stream Option

```json
{
  "data_stream_options": {
    "derived_metrics": {
      "enabled": true,
      "builtin": ["ingest.*"],
      "default_interval": "10s",
      "destinations": {
        "1m": { "lifecycle": { "data_retention": "90d" } }
      },
      "dimensions": ["service.name", "cloud.region"],
      "metrics": [
        {
          "name": "http.requests",
          "type": "counter",
          "when": {
            "exists": {
              "field": "http.request.method"
            }
          },
          "value": 1,
          "dimensions": ["http.request.method", "http.response.status_code"]
        }
      ]
    }
  }
}
```

Omitted fields use defaults when the concrete data stream option is built:

- `enabled`: `true`
- `builtin`: `["ingest.*"]`
- `default_interval`: `10s`
- `destinations`: `{}`
- `dimensions`: `[]`
- `metrics`: `[]`

Template composition is additive for `builtin`, `dimensions`, and `metrics`. `default_interval` and each entry of `destinations` are
overwritten by the most specific template that defines them. `enabled` is overwritten by the most specific template that defines it. A duplicate user metric name is allowed only when the full metric definition is identical.

## User Metric Fields

Every user metric has:

- `name`: metric name outside the reserved `ingest.*` namespace.
- `type`: `counter`, `gauge`, or `histogram`.
- `when`: optional predicate.
- `value`: numeric constant or field reference.
- `dimensions`: optional extra dimensions added to global dimensions.
- `interval`: optional override of `default_interval`. The metric is accumulated separately and written to that interval's own
  destination, so the interval must have an entry in `destinations`.

Counters default `value` to `1` when omitted.

Gauges support `aggregation`:

- `first_value`
- `last_value`
- `min`
- `max`
- `avg`
- `sum`

Non-gauge metrics reject `aggregation`.

Histograms keep the whole distribution of the observed values rather than reducing them to a number, so they take no `aggregation`. Each
series accumulates into a bounded exponential histogram of `data_streams.derived_metrics.histogram_buckets` buckets, which is the knob
that trades a histogram series' precision against its size. A histogram series is by far the most expensive kind: hundreds of buckets
against the handful of primitives a scalar series needs, which is why the circuit breaker matters more here than anywhere else.

## Predicates

Supported predicates are intentionally limited to deterministic, script-free forms that runtime code can evaluate on the write path:

```json
{ "exists": { "field": "event.duration" } }
```

```json
{ "term": { "event.outcome": "failure" } }
```

```json
{ "terms": { "http.response.status_code": [500, 502, 503] } }
```

```json
{ "range": { "event.duration": { "gt": 0, "lte": 1000000000 } } }
```

```json
{
  "and": [
    { "exists": { "field": "event.duration" } },
    { "term": { "event.outcome": "success" } }
  ]
}
```

`or` and `not` are also supported.

## Runtime

The runtime lives in the `data-streams` module, under `org.elasticsearch.datastreams.derivedmetrics`.

`DerivedMetricsIndexingListener` observes writes through `IndexingOperationListener`. Only operations whose origin is the primary are
observed, so a document is counted exactly once regardless of how many replicas it is written to. Resolving an index to its data stream
configuration is cached per cluster state version, so the steady-state per-document cost is one volatile read and a comparison.

`CompiledDerivedMetrics` is the write-path form of the configuration: predicates are compiled once, built-in selectors are expanded, and
the set of source paths any metric needs is known up front. When that set is empty — the common case for a stream that only asks for the
built-in ingest metrics without dimensions — the write path never parses `_source` at all. When it is not empty, only those paths are
parsed, using the same filtered-parsing technique `IndexRouting` uses for routing fields — including, as `IndexRouting` does, compiling
the filter once per configuration rather than once per document.

Everything else that can be precomputed is: predicate and dimension paths are split into segments at compile time rather than on every
lookup, and metrics that configure the same dimensions share a resolution slot so their values are read out of `_source` once per document
between them rather than once per metric. That last one is the difference between a linear and a quadratic write path, since global
dimensions apply to every metric by definition.

### What observing a write costs

Measured by `DerivedMetricsObservationBench` in the `benchmarks` project — run it with `-prof gc`, because the allocation number matters
at least as much as the time. This path runs once per document on the indexing thread, inside the shard's operation permit.

| configuration | ns/op | B/op |
|---|---|---|
| built-in ingest metrics, no dimensions | 423 | 160 |
| built-in ingest metrics, one dimension | 1,781 | 3,130 |
| built-in plus a predicate-guarded counter, five dimensions | 3,620 | 6,884 |
| one histogram metric over a field | 1,793 | 3,580 |

The shape of that table is the important part. A stream that only wants the built-in ingest metrics never reads `_source`, and costs
essentially nothing. The moment a single dimension is configured the write path has to parse a filtered slice of the document, and that
parse then dominates: for the five-dimension case roughly two thirds of the remaining bytes are the filtered parse itself, and everything
derived from it is the other third. If this needs to get cheaper again, the parse is where to look, not the accumulation.

`DerivedMetricsBuffer` holds one table per metric per interval bucket, and within a table one accumulator slot per series. A series is
interned to a dense ordinal by `DerivedMetricsSeriesTable` and its state lives in parallel `BigArrays` arrays indexed by that ordinal —
the same shape the metric aggregations use. That is 56 bytes per series with no per-series object, and recording an observation against a
series that already exists allocates nothing at all, because the dimension tuple is encoded into a reusable per-thread buffer and looked
up by hash.

Because the storage comes from `BigArrays` against the `derived_metrics` circuit breaker, the memory is bounded and reportable through
`_nodes/stats/breakers`. The breaker's limit defaults to 5% of the heap, so a node with a small heap gets a proportionally small budget
without anyone configuring one.

Buckets are aligned to the epoch, so every node agrees on boundaries without coordination. `DerivedMetricsService` flushes buckets whose interval ended more than a grace period ago, emitting one
document per bucket, and emits nothing at all for intervals with no observations.

Nothing is coordinated across nodes. Each node emits partial series carrying its own `derived_metrics.node` dimension, and queries reduce
across that dimension to get stream-wide values. This is what avoids a cluster-wide hot counter on the write path.

Series count is the one thing that grows with the data, because dimension values come from documents. It is capped per node by
`data_streams.derived_metrics.max_series_per_node` and per source stream by `max_series_per_stream`. The per-stream cap exists because the
node budget would otherwise be first-come-first-served, letting one high-cardinality stream starve every other stream on the node.

Emission is bounded too. A flush converts and sends in bulk-sized chunks rather than materialising every closed bucket first, and no
more than `max_in_flight_bulks` bulks' worth of documents are outstanding at once. Without that ceiling a destination that cannot keep up
would let every flush add to a queue with nothing bounding it; documents shed for this reason are logged. The ceiling counts documents
rather than requests, because otherwise it would depend on how the documents happened to be divided up — flushing early emits a handful
at a time, and a request-based ceiling would shed almost all of them while barely any memory was actually in flight.

### Memory pressure

`data_streams.derived_metrics.memory_pressure_policy` decides what happens when the buffer can take no more, either because a series cap
was reached or because the circuit breaker refused the memory a new series needed.

`flush_early`, the default, emits what has been collected so far as a partial bucket and carries on collecting. Nothing is lost, because
partials of one bucket are reduced together at query time exactly as partials from different nodes already are. The costs are more
documents while the pressure lasts, and a timestamp that sits a few milliseconds after its bucket start rather than exactly on it.

The offset is not cosmetic. A time series `_id` is `createId(routingHash, tsid, timestamp)` and the destination is written with
`op_type=create`, so two partials of the same series in the same bucket would produce the same `_id` and the second would be silently
rejected. Partial *N* is therefore stamped at `bucketStart + N` milliseconds. The alternative — putting the partial number in a dimension —
was rejected because the tsid *is* the series identity: partials would become separate series, downsampling would keep them apart, and
tsid cardinality would grow precisely when the node is already under pressure. The offset keeps one logical series one series, keeps the
document inside the same `date_histogram` bucket, and orders the partials for `first_value` and `last_value`. Intervals are at least one
second, so up to a thousand partials fit before the offset could reach the next bucket.

`drop` discards the observation instead. Document volume stays perfectly flat and timestamps stay exactly on bucket boundaries, at the
cost of losing data. This is what Micrometer and the Prometheus Java client do, and it is the right choice for anyone aligning query
windows to bucket boundaries by hand. Either way the buffer names the setting in a warning when it happens.

### Histograms

A histogram metric emits `metric.histogram` instead of `metric.value`. The field is an `exponential_histogram`, which already carries its
own sum, count, min and max, so no scalar travels alongside it and merging partials is the field's own concern rather than something a
query has to reconstruct.

That mapper ships in `x-pack-analytics` rather than in the server, so the destination template depends on it. The plugin is always bundled
in the default distribution, and the OTel metrics templates map the same type unconditionally, so this is the same coupling those already
take on. On a build without that plugin, installing the destination template fails outright rather than only histogram metrics failing —
worth knowing, because it takes the whole feature with it.

Exemplars are not collected. There is nowhere in a time series data stream to put them today.

### Retention

Each destination is given a lifecycle once, when it is first created, from the `destinations` entry for its interval. Without one it
falls back to the cluster-wide `data_streams.lifecycle.retention.default`, and to 30 days when that is unset, so a destination is never
unbounded by accident.

The lifecycle is applied once and never reconciled: changing `destinations` does not alter destinations that already exist, and a
lifecycle edited by hand on a destination is left alone. See `docs/internal/DerivedMetricsLifecycleDesign.md`.

### Settings

| setting | default | meaning |
|---|---|---|
| `data_streams.derived_metrics.flush_interval` | `1s` | how often closed buckets are emitted |
| `data_streams.derived_metrics.flush_grace_period` | `5s` | how long a bucket stays open past the end of its interval |
| `data_streams.derived_metrics.max_series_per_node` | `10000` | per-node series cap |
| `data_streams.derived_metrics.max_series_per_stream` | the node cap | per-source-stream cap, so one stream cannot spend the whole node budget |
| `data_streams.derived_metrics.bulk_size` | `1000` | documents per bulk request to the destination |
| `data_streams.derived_metrics.max_in_flight_bulks` | `8` | ceiling on emission outstanding at once, counted as this many `bulk_size` documents |
| `data_streams.derived_metrics.histogram_buckets` | `160` | bucket capacity of each histogram series, trading precision against size |
| `data_streams.derived_metrics.memory_pressure_policy` | `flush_early` | `flush_early` emits a partial bucket and keeps collecting; `drop` sheds the observation |

## Managed Destination

Each source data stream writes to one hidden time series data stream per interval it uses,
`derived-metrics-<source data stream>-<interval>`. Splitting per interval is what makes retention resolution-dependent, and it means a
query over one destination never has to filter by interval. It is created on demand
by the first metric document written to it, backed by the managed `derived-metrics@template` index template that Elasticsearch installs
in every project. `derived-metrics-*` is therefore a reserved namespace: a user data stream with that prefix would be captured by the
managed template.

The destination is hidden rather than dot-prefixed, so it stays out of ordinary wildcard expressions while remaining directly queryable.

Emitted documents look like this:

| field | type | meaning |
|---|---|---|
| `@timestamp` | `date` | start of the interval bucket |
| `metric.name` | keyword dimension | the metric name |
| `metric.value` | `double`, gauge | this node's partial value for the interval; for an `avg` gauge this is the **sum** |
| `metric.count` | `long`, gauge | observation count, present only on `avg` gauges |
| `derived_metrics.source` | keyword dimension | the source data stream |
| `derived_metrics.interval` | keyword dimension | the interval, matching the destination's suffix |
| `derived_metrics.node` | keyword dimension | the emitting node |
| `dimensions.*` | keyword dimensions | user dimensions, dynamically mapped |

A user dimension `service.name` is written as `dimensions.service.name`. Dimensions a document does not have are simply absent, rather
than filled with a placeholder value.

`metric.value` is a gauge even for counter metrics, because what is emitted is a per-interval partial with no counter reset semantics to
preserve. Counters are summed at query time.

Elasticsearch controls the destination entirely. Users do not configure `data_stream.dataset`, `data_stream.namespace`, the destination
index mode, the routing path, or any internal dimension field. Those are either constant for a one-source destination or managed
internally for compatibility and future multi-source policies.

## Built-In Ingest Metrics

`ingest.*` expands to:

- `ingest.docs.count`
- `ingest.docs.rate`
- `ingest.bytes.count`
- `ingest.bytes.rate`
- `ingest.failures.count`
- `ingest.failures.rate`

Counts are emitted as the sum of what the node observed during the interval. Rates are that same sum divided by the interval length in
seconds. Both are partials: a stream-wide value is the sum across the emitting-node dimension, which for a rate is meaningful because
every partial covers the same interval.

`ingest.docs.*` and `ingest.bytes.*` count successful writes; `ingest.failures.*` counts failed ones. Byte counts come from the size of
the document's source. Global dimensions apply to the built-ins as well as to user metrics.

### Averages

An `avg` gauge does **not** emit the mean. It emits its sum in `metric.value` and the observation count in `metric.count`, so the mean is
`SUM(metric.value) / SUM(metric.count)`.

This matters because a mean cannot be re-aggregated. Averaging per-interval means weights every interval equally regardless of how busy
it was, which on a stream whose busy intervals differ from its quiet ones reads 30–50% low — measured at 131ms against a true 186ms in
one sample and 81ms against 156ms in another.

## Query Examples

Counter:

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "metric.name": "http.requests" } },
        { "term": { "derived_metrics.interval": "1m" } }
      ]
    }
  },
  "aggs": {
    "per_minute": {
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1m"
      },
      "aggs": {
        "requests": {
          "sum": {
            "field": "metric.value"
          }
        }
      }
    }
  }
}
```

Gauge:

```json
{
  "query": {
    "term": {
      "metric.name": "queue.depth"
    }
  },
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service.name"
      },
      "aggs": {
        "depth": {
          "max": {
            "field": "metric.value"
          }
        }
      }
    }
  }
}
```

Histogram:

```json
{
  "query": {
    "term": {
      "metric.name": "http.request.duration"
    }
  },
  "aggs": {
    "latency": {
      "histogram": {
        "field": "metric.histogram"
      }
    }
  }
}
```

