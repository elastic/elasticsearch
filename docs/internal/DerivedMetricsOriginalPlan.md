# Derived Metrics Original Plan

This document preserves the original feature plan for derived metrics on data streams.

## Problem

High-volume data streams can carry enough write traffic that asking operational questions directly from the source stream is expensive. A stream receiving one million events per second should still expose common metrics with near-constant derived document intake, bounded by the number of configured metrics, dimensions, and intervals rather than the source write volume.

The proposed feature observes writes into a source data stream, buffers metric state, and emits compact metric documents into a managed time series data stream (TSDS). The derived stream can then be queried with normal metrics-style aggregations.

## Configuration Shape

Derived metrics are configured under `data_stream_options.derived_metrics`. The option is named for the managed metric output produced for a data stream, while leaving the underlying destination stream opaque to users.

```json
{
  "data_stream_options": {
    "derived_metrics": {
      "enabled": true,
      "builtin": ["ingest.*"],
      "intervals": ["10s", "1m", "30m"],
      "dimensions": ["service.name", "cloud.region"],
      "metrics": [
        {
          "name": "http.requests",
          "type": "counter",
          "when": { "exists": { "field": "http.request.method" } },
          "value": 1,
          "dimensions": ["http.request.method", "http.response.status_code"]
        },
        {
          "name": "queue.depth",
          "type": "gauge",
          "value": { "field": "queue.depth" },
          "aggregation": "first_value"
        },
        {
          "name": "http.request.duration",
          "type": "histogram",
          "value": { "field": "event.duration" }
        }
      ]
    }
  }
}
```

## Defaults

The default built-in metric selection is `["ingest.*"]`.

Initial built-ins:

- `ingest.docs.count`
- `ingest.docs.rate`
- `ingest.bytes.count`
- `ingest.bytes.rate`
- `ingest.failures.count`
- `ingest.failures.rate`

## Dimensions

User dimensions are additive. Elasticsearch always adds internal dimensions required to make distributed emission queryable, such as the source data stream and the emitting node.

`data_stream.dataset` and `data_stream.namespace` do not need to be user-selected dimensions when Elasticsearch creates one managed derived TSDS per source data stream. They are constant for that destination. Elasticsearch may still store them internally for ECS compatibility and to make future multi-source targets safe.

Metric-level dimensions add to global dimensions. Users should not be able to override internal fields or produce an invalid TSDS mapping.

## Distributed Writes

Writes can arrive through many coordinating and data nodes. V1 should allow node-local buffers to emit partial metric documents. Queries combine those partials by aggregating across an internal emitting-node dimension.

This avoids a hot centralized counter on the write path. It also means rates and counters are query-time reductions across emitted partial series.

## Intervals

Intervals are variadic: one metric definition can emit multiple resolutions, for example `10s`, `1m`, and `30m`.

Each interval is represented as a dimension or field in the managed TSDS so queries can choose the resolution explicitly.

If no documents are written in an interval, Elasticsearch should not emit zero-valued documents. Query consumers handle gaps with existing date histogram gap policies.

## User Metrics

V1 includes user metrics. This is the main unlock because users can derive compact metrics from arbitrary source events without querying the raw stream.

Supported metric types:

- `counter`: increments by a constant or numeric field when the predicate matches.
- `gauge`: records a field value with an aggregation mode.
- `histogram`: records a numeric field into a histogram representation.

Gauge aggregation modes:

- `first_value`
- `last_value`
- `min`
- `max`
- `avg`
- `sum`

Initial predicates:

- `exists`
- `term`
- `terms`
- numeric `range`
- `and`
- `or`
- `not`

No scripts in V1. Values are constants or field references.

## Metric Stories

HTTP/API:

- `http.requests`: counter by method, status code, route, service, and region.
- `http.errors`: counter filtered to 5xx responses.
- `http.client.errors`: counter filtered to 4xx responses.
- `http.request.duration`: histogram from `event.duration`.
- `http.response.bytes`: counter from `http.response.body.bytes`.

Queue and worker systems:

- `queue.depth`: gauge from `queue.depth`, queried with `max` or `last_value`.
- `queue.enqueue.count`: counter for enqueue events.
- `queue.dequeue.count`: counter for dequeue events.
- `queue.lag.ms`: gauge or histogram from queue lag fields.
- `job.duration`: histogram from job runtime.
- `job.failures`: counter by job type and failure reason.

Batch and ETL:

- `records.processed`: counter from record count fields.
- `records.rejected`: counter filtered to rejected batches.
- `batch.size`: histogram from batch size.
- `checkpoint.delay`: gauge from checkpoint age.
- `pipeline.watermark.lag`: gauge from watermark lag.

Application health:

- `log.errors`: counter filtered to `log.level:error`.
- `log.warnings`: counter filtered to `log.level:warn`.
- `exceptions`: counter by exception class.
- `feature.usage`: counter by feature flag or endpoint.
- `tenant.activity`: counter by tenant or organization dimension.

Security and audit:

- `auth.failures`: counter by realm, outcome, source IP, or tenant.
- `auth.successes`: counter by realm and outcome.
- `access.denied`: counter by resource and principal type.
- `token.refresh.count`: counter by client ID.
- `privilege.escalation.events`: counter filtered to specific audit actions.

Infrastructure and edge:

- `cdn.requests`: counter by cache status, POP, method, and status.
- `cdn.bytes`: counter from transferred bytes.
- `lb.connection.errors`: counter filtered to load balancer failures.
- `dns.query.count`: counter by response code.
- `tls.handshake.duration`: histogram from handshake duration.

Business events:

- `orders.created`: counter by market, channel, and tenant.
- `payments.failed`: counter by provider and failure code.
- `cart.value`: histogram from cart amount.
- `subscription.churn`: counter filtered to cancellation events.
- `license.seats.active`: gauge by account.

## Multiple Write Targets

V1 starts with template-local configuration: a data stream owns its derived metrics configuration and gets one managed destination TSDS.

The design should leave room for policy-level selectors:

- all data streams
- all streams of a type, such as `logs-*-*`
- selected stream patterns

Those higher-level policies must not make the destination fields user-configurable in a way that conflicts with TSDS dimension requirements.

## Query Model

Counters are queried by summing their emitted values and optionally applying `rate` over a date histogram.

Gauges are queried with the matching aggregation:

- `last_value` for current state.
- `first_value` for opening state in an interval.
- `min`, `max`, or `avg` for range summaries.
- `sum` when the gauge represents an additive point-in-time value across dimensions.

Histograms are queried with percentile and distribution aggregations.

All query examples should include the interval selector and aggregate over internal emitting-node dimensions unless a caller intentionally wants per-node partials.

