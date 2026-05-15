---
description: Prometheus-compatible HTTP endpoints for PromQL queries and metric discovery against time series data in Elasticsearch.
navigation_title: HTTP API
applies_to:
  stack: preview 9.4.0
  serverless: preview
products:
  - id: elasticsearch
---

# PromQL HTTP API [promql-http-api]

::::{warning}
This functionality is in technical preview and might be changed or removed in a future release.
Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::

These endpoints run under the `/_prometheus/` prefix.
They are intended for Prometheus-compatible tooling such as Grafana data sources, autocompletion, variable queries, and similar clients.

These APIs only consider metric data stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).

## Index scoping [promql-http-api-index-scope]

Every path has two forms:

- Cluster default: `GET /_prometheus/api/v1/<path>`
- Explicit index expression: `GET /_prometheus/{index}/api/v1/<path>`

The `{index}` segment is an {{es}} index expression (for example, `metrics-generic.prometheus-*`) that restricts which indices are considered in the query.
This can reduce latency on clusters that contain many large time series data streams when you query a subset of indices.

When you omit `{index}` in the path, qualifying indices are identified through the default index expression `metrics-*`.

## Query endpoints [promql-http-api-query-endpoints]

These endpoints mirror the Prometheus [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) and [instant query](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries) APIs.

### Range query [promql-http-api-query-range]

`GET /_prometheus/api/v1/query_range`\
`GET /_prometheus/{index}/api/v1/query_range`

This endpoint evaluates a PromQL expression over a time window and returns matrix data (`resultType: "matrix"`).

| Parameter | Required | Description |
| --- | --- | --- |
| `query` | Yes | PromQL expression |
| `start` | Yes | Range start, [Timestamp](#promql-http-api-param-timestamp) |
| `end` | Yes | Range end, [Timestamp](#promql-http-api-param-timestamp) |
| `step` | Yes | Resolution between samples, [Step width](#promql-http-api-param-step) |
| `limit` | No (default: `0`) | Maximum number of series returned, [`limit`](#promql-http-api-limit) |

The `timeout`, `lookback_delta`, and `stats` parameters are not supported yet (see [Limitations](promql-limitations.md#promql-limitations-unsupported-query-params)).

### Instant query [promql-http-api-query-instant]

`GET /_prometheus/api/v1/query`\
`GET /_prometheus/{index}/api/v1/query`

This endpoint evaluates at a single instant and returns vector data (`resultType: "vector"`).

| Parameter | Required | Description |
| --- | --- | --- |
| `query` | Yes | PromQL expression |
| `time` | No (default: now) | Evaluation instant, [Timestamp](#promql-http-api-param-timestamp). The handler still uses an internal five-minute range ending at this time (see [Limitations](promql-limitations.md)) |
| `limit` | No (default: `0`) | Maximum number of series returned, [`limit`](#promql-http-api-limit) |

The `timeout`, `lookback_delta`, and `stats` parameters are not supported yet (see [Limitations](promql-limitations.md#promql-limitations-unsupported-query-params)).

## Metadata and discovery endpoints [promql-http-api-metadata]

These entrypoints mirror the Prometheus [metric metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata), [label-name discovery](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names), [label-value queries](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values), and [series discovery by matchers](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers) APIs so UIs can browse metrics, build [time series selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-series-selectors), and populate template variables.

### Label names [promql-http-api-labels]

`GET /_prometheus/api/v1/labels`\
`GET /_prometheus/{index}/api/v1/labels`

This endpoint returns sorted label names present on matching series.
`match[]` is not required. If you omit every `match[]`, results are still limited to the `start` and `end` time range.

| Parameter | Required | Description |
| --- | --- | --- |
| `match[]` | No (default: no selectors) | Optional repeated [selectors](#promql-http-api-param-match) (for example, `match[]=up{job="prometheus"}`). Omit all to return label names for every series in the selected time window |
| `start` | No (default: 24h before `end`) | Start of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `end` | No (default: now) | End of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `limit` | No (default: `0`) | Maximum number of label names in the response, [`limit`](#promql-http-api-limit) |

### Label values [promql-http-api-label-values]

`GET /_prometheus/api/v1/label/{name}/values`\
`GET /_prometheus/{index}/api/v1/label/{name}/values`

This endpoint returns sorted, deduplicated values for one label.
`match[]` is not required. If you omit every `match[]`, results are still limited to the `start` and `end` time range.
Label names can use OpenMetrics `U__` encoding for characters that are not valid in Prometheus label names. The server decodes them before matching series.

| Parameter | Required | Description |
| --- | --- | --- |
| `match[]` | No (default: no selectors) | Optional repeated [selectors](#promql-http-api-param-match). Same semantics as [`labels`](#promql-http-api-labels) |
| `start` | No (default: 24h before `end`) | Start of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `end` | No (default: now) | End of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `limit` | No (default: `0`) | Maximum number of values returned for this label, [`limit`](#promql-http-api-limit) |

Unknown label names are returned as an empty successful result (`data: []`), matching typical Prometheus client expectations.

### Series [promql-http-api-series]

`GET /_prometheus/api/v1/series`\
`GET /_prometheus/{index}/api/v1/series`

This endpoint returns the set of series matching the given selectors.
At least one `match[]` parameter is required.

| Parameter | Required | Description |
| --- | --- | --- |
| `match[]` | Yes (at least one) | Repeated [selectors](#promql-http-api-param-match) |
| `start` | No (default: 24h before `end`) | Start of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `end` | No (default: now) | End of time range, [Timestamp](#promql-http-api-param-timestamp) |
| `limit` | No (default: `0`) | Maximum number of series in the response, [`limit`](#promql-http-api-limit) |

### Metric metadata [promql-http-api-metadata-endpoint]

{applies_to}`stack: preview 9.5.0` {applies_to}`serverless: preview`

`GET /_prometheus/api/v1/metadata`\
`GET /_prometheus/{index}/api/v1/metadata`

This endpoint returns metric-level information such as type, help, and unit, analogous to Prometheus `TYPE`, `HELP`, and `UNIT` lines.

The `help` field is always an empty string for now (see [Limitations](promql-limitations.md#promql-limitations-metadata-help)).

| Parameter | Required | Description |
| --- | --- | --- |
| `metric` | No (default: all metrics in lookback) | Restrict to a single metric name |
| `limit` | No (default: `0`) | Maximum number of distinct metrics in the response, [`limit`](#promql-http-api-limit) |
| `limit_per_metric` | No (default: `0`) | Maximum number of metadata entries returned per metric, [`limit`](#promql-http-api-limit) |

The `metadata` route does not support `match[]`, `start`, or `end`.
{{es}} discovers type and unit using the {{esql}} [`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md) command over [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS), with a fixed 24-hour lookback ending when the request runs.

## Request parameter formats [promql-http-api-parameters]

Parameter encodings match the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/). See the upstream [format overview](https://prometheus.io/docs/prometheus/latest/querying/api/#format-overview).

### Timestamps [promql-http-api-param-timestamp]

Values may be an [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) timestamp or Unix time in seconds as a numeric string, with optional fractional digits for sub-second precision. This matches Prometheus request parsing. Sample timestamps inside JSON results use Unix seconds, as in Prometheus.

### Selectors (`match[]`) [promql-http-api-param-match]

Each value must be URL-encoded. Syntax is the same as Prometheus [time series selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-series-selectors) (for example, `up` or `http_requests_total{job="api"}`).

Repeatable query parameters use a `[]` suffix, matching Prometheus. Whether any `match[]` must be present is defined in each endpoint’s parameter table.

### Step [promql-http-api-param-step]

The `step` query parameter accepts:

- A non-negative decimal integer string: seconds between samples (for example, `15` for 15s resolution).
- Or Prometheus-style duration literals such as `30s`, `5m`, or `1h30m`: a non-negative integer plus a unit suffix (`ms`, `s`, `m`, `h`, `d`, `w`, or `y`), repeated and concatenated when needed (for example, `1h30m`). See Prometheus [float literals and time durations](https://prometheus.io/docs/prometheus/latest/querying/basics/#float-literals-and-time-durations).

### `limit` [promql-http-api-limit]

`limit` defaults to `0`, which means no cap from the request (Prometheus-style unlimited) on routes that accept it. {{es}} may still truncate very large responses when enforcing [`esql.query.timeseries_result_truncation_max_size`](/reference/query-languages/esql/limitations.md#esql-max-rows).

The [metadata](#promql-http-api-metadata-endpoint) route also takes `limit_per_metric` with the same `0` default (no per-metric cap from the request).

## Response format [promql-http-api-response]

Responses use JSON. Successful calls return HTTP 200 with:

```json
{
  "status": "success",
  "data": { ... }
}
```

The `data` object shape depends on the route (for example, `resultType` and `result` for `query` / `query_range`).

When the `limit` is reached and the server detects truncation, the response might include a top-level `warnings` array (strings). The preview uses a fixed message such as `results truncated due to limit`.

Numeric sample values in query results are JSON strings (including `NaN`, `+Inf`, and `-Inf`), matching common Prometheus JSON encoding.

Errors return a non-2xx HTTP status and a JSON body of the form:

```json
{
  "status": "error",
  "errorType": "bad_data",
  "error": "<message>"
}
```

Client errors (HTTP 4xx) mean the request or expression is not valid for this implementation, for example, malformed parameters, parameters not supported yet on a route, invalid selectors, or PromQL that {{es}} does not evaluate yet (see [Limitations](promql-limitations.md)). Those failures usually return `errorType: bad_data`.

Server errors (HTTP 5xx) and timeout responses reflect operational failures inside {{es}}. Those responses typically use `errorType: timeout` or `execution` depending on the situation.

## Further reading

- [Prometheus query API](https://prometheus.io/docs/prometheus/latest/querying/api/)
- [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md)
- [`PROMQL` command ({{esql}})](/reference/query-languages/esql/commands/promql.md)
- [Limitations](promql-limitations.md)
