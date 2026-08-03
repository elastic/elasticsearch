# Derived Metrics: intervals and lifecycle

Design for giving derived metrics destinations a retention policy. Not yet implemented.

## Why

The destination has no lifecycle today, so `derived-metrics-*` grows forever. That is a poor look for
a feature whose premise is bounding volume.

Retention is a property of a *destination*, and it is usually resolution-dependent: keep a 10s series
for days, a 1m series for months. Today every configured interval lands in one destination per
source, so retention could only ever be all-or-nothing across resolutions. Splitting the destination
per interval is therefore a prerequisite, not a separate change.

## The shape

`intervals` (a list) becomes `interval` (one value) plus a `destinations` map keyed by interval. A
metric may override the interval, which routes it to that interval's destination.

```json
{
  "data_stream_options": {
    "derived_metrics": {
      "interval": "10s",
      "destinations": {
        "10s": { "lifecycle": { "data_retention": "7d" } },
        "1m":  { "lifecycle": { "data_retention": "90d" } }
      },
      "metrics": [
        { "name": "queue.depth", "type": "gauge", "value": { "field": "queue.depth" }, "interval": "1m" }
      ]
    }
  }
}
```

- `interval` — the default interval, `10s` when omitted. Every metric that does not say otherwise is
  accumulated at this resolution.
- `destinations` — per-destination settings, keyed by interval. `lifecycle` takes the existing
  `DataStreamLifecycle` shape, so `data_retention` and `downsampling` are inherited wholesale rather
  than reinvented.
- `metrics[].interval` — an override. The metric is accumulated separately and written to that
  interval's destination.

Destinations are named `derived-metrics-<source>-<interval>`, so a source using `10s` and `1m`
produces `derived-metrics-logs-app-default-10s` and `-1m`.

Because the interval is in the destination name, a query over one destination no longer needs to
filter on `derived_metrics.interval`. The dimension stays, since it is still needed to interpret a
document in isolation.

### Validation

- An override to an interval with no `destinations` entry is **rejected**. Retention has to be a
  deliberate decision for every destination a config creates.
- The default `interval` needs no `destinations` entry; without one it falls back to the default
  retention below. This keeps the zero-config case working.
- The number of distinct intervals per source stays capped, reusing the existing limit of 8. Each one
  is a data stream and a shard.

### Default retention

When a destination has no `lifecycle`, fall back to the cluster-wide
`data_streams.lifecycle.retention.default`, and to **30 days** when that is unset. A derived metrics
destination is never unbounded by accident.

## Coarse resolutions come from downsampling

Multiple accumulated intervals are no longer the mechanism for multiple resolutions. One interval is
accumulated on the write path; coarser views come from TSDS downsampling, configured through the same
`lifecycle` object:

```json
"10s": {
  "lifecycle": {
    "data_retention": "90d",
    "downsampling": [ { "after": "1d", "fixed_interval": "1m" } ]
  }
}
```

This keeps the write path doing one accumulation per series instead of one per interval, and gets
resolution-dependent retention from machinery that already exists.

**The known sharp edge:** downsampling rewrites a `gauge` field into an `aggregate_metric_double`.
After the first round, `metric.value` is no longer a number — it is `metric.value.sum`, `.min`,
`.max`, `.value_count`. A query spanning the boundary sees the field change shape mid-range. We are
accepting this and documenting it rather than pre-emptively emitting `aggregate_metric_double`, which
would cost more per document on every write for a problem most streams never reach.

The interval override remains for metrics that genuinely need a different *live* resolution, which
downsampling cannot provide.

## Applying the lifecycle

Destinations are auto-created by the first document written to them, from one shared template. A
per-source, per-interval lifecycle cannot live in that template, so it is applied to each destination
after it exists — **once, at creation, and never again**.

The source configuration is not continuously reconciled. A lifecycle edited by hand on a destination
stays edited.

**The consequence, which must be documented prominently:** changing `lifecycle` in `derived_metrics`
does not affect destinations that already exist. Applying it means either deleting the destination
or setting it directly:

```
PUT _data_stream/derived-metrics-<source>-<interval>/_lifecycle
```

This is the deliberate trade for not having Elasticsearch silently revert deliberate operator
changes. If it proves too surprising in practice, the fallback is the third option we rejected:
reconcile, but skip destinations whose lifecycle no longer matches what we last wrote.

## No cleanup on disable or delete

Disabling derived metrics stops emission and leaves the destination. Deleting the source data stream
leaves it too. Retention expires it either way.

Keeping cheap metric history after the expensive raw data is gone is a large part of the point, and
deletion is not undoable. The cost is that orphaned destinations linger for their retention period.

## Implementation notes

- `DataStreamDerivedMetrics` changes shape: `intervals` list → `interval` + `destinations` map, and
  `Metric` gains an optional `interval`. The feature is unreleased, so the existing
  `derived_metrics_in_data_stream_options` transport version can absorb the new serialization; no
  backwards compatibility is owed.
- `CompiledDerivedMetrics.CompiledMetric` carries its resolved interval; `CompiledDerivedMetrics`
  exposes the set of intervals in use rather than a list applied to every metric.
- `DerivedMetricsDestination.destinationFor` takes the interval. `DerivedMetricsEmitter` routes by the
  series' interval, and `DerivedMetricsService` groups a flush by destination as well as by project.
- Something has to apply the lifecycle at destination creation. The natural home is
  `DerivedMetricsTemplateRegistry`, which already watches cluster state on the master: when a
  `derived-metrics-*` data stream appears without a lifecycle, look up the source configuration and
  set it.
- Renaming the destination touches `DerivedMetricsIT`, `310_derived_metrics.yml`,
  `docs/internal/DerivedMetrics.md`, and the whole of `derived-metrics-demo/`.
