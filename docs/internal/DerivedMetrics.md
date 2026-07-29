# Derived Metrics

Derived metrics let a data stream emit compact operational metrics into a managed TSDS while documents are written to the source stream. The goal is to make common metric queries scale with the number of configured metrics and dimensions instead of the source write rate.

## Scope

The V1 metadata model supports:

- `data_stream_options.derived_metrics`
- built-in ingest metrics with default `["ingest.*"]`
- variadic intervals
- additive global and per-metric dimensions
- user-defined `counter`, `gauge`, and `histogram` metrics
- gauge aggregations including `first_value` and `last_value`
- script-free predicates and values

Runtime collection and TSDS emission are separate follow-up work. The metadata model is intentionally shaped so runtime code can create managed destination streams and internal dimensions without exposing error-prone destination settings to users.

## Data Stream Option

```json
{
  "data_stream_options": {
    "derived_metrics": {
      "enabled": true,
      "builtin": ["ingest.*"],
      "intervals": ["10s", "1m"],
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
- `intervals`: `["10s"]`
- `dimensions`: `[]`
- `metrics`: `[]`

Template composition is additive for `builtin`, `intervals`, `dimensions`, and `metrics`. `enabled` is overwritten by the most specific template that defines it. A duplicate user metric name is allowed only when the full metric definition is identical.

## User Metric Fields

Every user metric has:

- `name`: metric name outside the reserved `ingest.*` namespace.
- `type`: `counter`, `gauge`, or `histogram`.
- `when`: optional predicate.
- `value`: numeric constant or field reference.
- `dimensions`: optional extra dimensions added to global dimensions.

Counters default `value` to `1` when omitted.

Gauges support `aggregation`:

- `first_value`
- `last_value`
- `min`
- `max`
- `avg`
- `sum`

Non-gauge metrics reject `aggregation`.

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

## Runtime Model

The planned runtime service should:

- observe write successes and failures for source data streams
- evaluate built-in and user metric definitions on the write path
- buffer node-local partial state per source stream, interval, metric, and dimension set
- flush one compact metric document per active series and interval
- emit no documents for intervals with no observations
- index into a managed TSDS destination derived from the source stream

Distributed writes should not require a cluster-wide hot counter. Each writing node can emit partial series with an internal node dimension. Queries that want source-level values sum or reduce across that internal dimension.

## Managed Destination

The destination should be opaque to users. A source data stream such as `logs-my_app-default` can map to a managed TSDS such as `.metrics-derived.logs-my_app-default` or another reserved internal naming scheme.

Elasticsearch controls required TSDS dimensions:

- source data stream identity
- interval
- metric name
- emitting node or shard identity
- user global dimensions
- user per-metric dimensions

Users should not configure `data_stream.dataset`, `data_stream.namespace`, destination index mode, routing path, or internal dimension fields directly. Those fields are either constant for a one-source destination or managed internally for compatibility and future multi-source policies.

## Built-In Ingest Metrics

`ingest.*` expands to:

- `ingest.docs.count`
- `ingest.docs.rate`
- `ingest.bytes.count`
- `ingest.bytes.rate`
- `ingest.failures.count`
- `ingest.failures.rate`

Counts are emitted as counter-style values. Rates can either be emitted directly from interval state or derived at query time from counts and interval duration. Query-time rate derivation avoids conflicting partial rates from many nodes.

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

