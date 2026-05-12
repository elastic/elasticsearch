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

Expect metric data to be stored in {{es}} as [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).

## How requests are executed [promql-http-api-execution]

For every supported route, {{es}} turns the incoming HTTP parameters into an {{esql}} plan, executes it through the same {{esql}} runtime used elsewhere, and converts the tabular response into Prometheus-style JSON (`resultType`, `result` arrays, and similar).

See [Limitations](promql-limitations.md) for what is not supported yet and how behavior differs from Prometheus.

## Index scoping [promql-http-api-index-scope]

Every path has two forms:

- Cluster default: `GET /_prometheus/api/v1/<path>`
- Explicit index expression: `GET /_prometheus/{index}/api/v1/<path>`

The `{index}` segment is an {{es}} index expression (for example, `metrics-prod-*`) that restricts which indices participate before expression evaluation or metadata collection.
That pre-filter reduces the input size when many unrelated [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS) exist in the cluster, improving latency and keeping resource consumption at bay.
When you omit `{index}` in the path, qualifying indices are identified through the default index expression `metrics-*` (the {{esql}} `PROMQL` default).

## `limit` [promql-http-api-limit]

`limit` defaults to `0`, which means no cap from the request (Prometheus-style unlimited). The server might still truncate very large responses. See [`esql.query.timeseries_result_truncation_max_size`](/reference/query-languages/esql/limitations.md#esql-max-rows).

## Query endpoints [promql-http-api-query-endpoints]

Each route documents the parameters this implementation accepts. Compare with the matching section in the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) for a one-to-one view of upstream names and semantics.

### Range query: `query_range` [promql-http-api-query-range]

`GET /_prometheus/api/v1/query_range`\
`GET /_prometheus/{index}/api/v1/query_range`

This mirrors [Prometheus range queries](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries). It evaluates a PromQL expression over a time window and returns matrix data (`resultType: "matrix"`).

| Parameter | Required | Description |
| --- | --- | --- |
| `query` | Yes | PromQL expression |
| `start` | Yes | Range start (Prometheus-compatible timestamp) |
| `end` | Yes | Range end |
| `step` | Yes | Resolution / step width |
| `limit` | No (default: `0`) | Maximum number of series returned |

The `timeout`, `lookback_delta`, and `stats` parameters are not supported yet (see [Limitations](promql-limitations.md#promql-limitations-unsupported-query-params)).

### Instant query: `query` [promql-http-api-query-instant]

`GET /_prometheus/api/v1/query`\
`GET /_prometheus/{index}/api/v1/query`

This mirrors [Prometheus instant queries](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries). It evaluates at a single instant and returns vector data (`resultType: "vector"`).

| Parameter | Required | Description |
| --- | --- | --- |
| `query` | Yes | PromQL expression |
| `time` | No (default: now) | Evaluation instant; the handler still uses an internal 5 minute range ending at this time (see [Limitations](promql-limitations.md)) |
| `limit` | No (default: `0`) | Maximum number of series returned |

The `timeout`, `lookback_delta`, and `stats` parameters are not supported yet (see [Limitations](promql-limitations.md#promql-limitations-unsupported-query-params)).

## Metadata and discovery endpoints [promql-http-api-metadata]

These entrypoints mirror Prometheus metadata, labels, label values, and series APIs so UIs can browse metrics, build selectors, and populate template variables.

### Selectors and time windows

The `labels`, `label/{name}/values`, and `series` routes accept repeated `match[]` parameters containing Prometheus [time series selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-series-selectors) (for example, `http_requests_total{job="api"}`).
Supplying selectors narrows results to the series that match, which keeps responses tractable on large clusters.
Omitting every `match[]` on `labels` or `label/{name}/values` applies only the time window filter, so results include all series in that window.
They honor `start` and `end` as shown in each table’s Required column.

```text
GET /_prometheus/api/v1/series?match[]=http_requests_total{job="api"}
GET /_prometheus/api/v1/labels?match[]=http_requests_total
GET /_prometheus/api/v1/label/instance/values?match[]=http_requests_total{job="api"}
```

The first call lists every series for `http_requests_total` with `job="api"`.
The second returns only label names that appear on `http_requests_total`.
The third returns `instance` values seen on matching series.

### Metric metadata: `metadata` [promql-http-api-metadata-endpoint]

{applies_to}`stack: preview 9.5.0` {applies_to}`serverless: preview`

`GET /_prometheus/api/v1/metadata`\
`GET /_prometheus/{index}/api/v1/metadata`

This mirrors [Prometheus metric metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata). It returns metric-level information such as type, help, and unit, analogous to Prometheus `TYPE`, `HELP`, and `UNIT` lines.

The `help` field is always an empty string for now (see [Limitations](promql-limitations.md#promql-limitations-metadata-help)).

| Parameter | Required | Description |
| --- | --- | --- |
| `metric` | No (default: all metrics in lookback) | Restrict to a single metric name |
| `limit` | No (default: `0`) | Maximum number of distinct metrics in the response |
| `limit_per_metric` | No (default: `0`) | Maximum number of metadata entries returned per metric |

Unlike the other discovery routes, `metadata` does not accept `match[]`, `start`, or `end`.
{{es}} discovers type and unit using the {{esql}} [`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md) command over [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS), with a fixed 24-hour lookback ending when the request runs.

For metadata routes, these limits apply only when both `limit` and `limit_per_metric` are greater than zero.

### Label names: `labels` [promql-http-api-labels]

`GET /_prometheus/api/v1/labels`\
`GET /_prometheus/{index}/api/v1/labels`

This mirrors [Prometheus label-name discovery](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names). It returns sorted label names present on matching series.

| Parameter | Required | Description |
| --- | --- | --- |
| `match[]` | No (default: no selectors) | Repeated selector strings (for example, `match[]=up{job="prometheus"}`) |
| `start` | No (default: 24h before `end`) | Start of time range |
| `end` | No (default: now) | End of time range |
| `limit` | No (default: `0`) | Maximum number of label names in the response |

### Label values: `label/{name}/values` [promql-http-api-label-values]

`GET /_prometheus/api/v1/label/{name}/values`\
`GET /_prometheus/{index}/api/v1/label/{name}/values`

This mirrors [Prometheus label-value queries](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values). It returns sorted, deduplicated values for one label.
Label names can use OpenMetrics `U__` encoding for characters that are not valid in Prometheus label names. The server decodes them before matching [series](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-series).

Parameters match the `labels` endpoint (`match[]`, `start`, `end`, `limit`), including Required column defaults. Here, `limit` caps how many values are returned for this label.
Unknown label names are returned as an empty successful result (`data: []`), matching typical Prometheus client expectations.

### Series: `series` [promql-http-api-series]

`GET /_prometheus/api/v1/series`\
`GET /_prometheus/{index}/api/v1/series`

This mirrors [Prometheus series discovery by matchers](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers). It returns the set of series matching the given selectors.

| Parameter | Required | Description |
| --- | --- | --- |
| `match[]` | Yes (at least one) | Repeated selector strings |
| `start` | No (default: 24h before `end`) | Start of time range |
| `end` | No (default: now) | End of time range |
| `limit` | No (default: `0`) | Maximum number of series in the response |

## Timestamps, step width, and `match[]` encoding [promql-http-api-parameters]

For `query` (`time`), `query_range` (`start`, `end`), and discovery routes (`start`, `end`), timestamps use the same parsing as the PromQL layer in {{es}}:

- Unix time in seconds — a numeric string, optionally with a fractional part (sub-second precision).
- RFC 3339 / ISO-8601 instants — for example `2015-07-01T20:10:51.781Z` ([RFC 3339](https://www.rfc-editor.org/rfc/rfc3339), parsed with Java `Instant.parse`).

Step width on `query_range`:

- A decimal integer string: seconds between samples (for example `15` for 15s resolution).
- Or Prometheus-style duration literals such as `30s`, `5m`, or `1h30m`: a non-negative integer and a unit suffix (`ms`, `s`, `m`, `h`, `d`, `w`, or `y`), repeated and concatenated when needed (for example `1h30m`).

`match[]` on `labels`, `label/{name}/values`, and `series` is a repeatable query parameter (`match[]=` can appear multiple times). Selector strings must be URL-encoded (same as the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) expectation for `<series_selector>` placeholders).

## Response format [promql-http-api-response]

Responses use JSON. Successful calls return HTTP 200 with:

```json
{
  "status": "success",
  "data": { ... }
}
```

The `data` object shape depends on the route (for example `resultType` and `result` for `query` / `query_range`).

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

Client errors (HTTP 4xx) mean the request or expression is not valid for this implementation—for example malformed parameters, parameters not supported yet on a route, invalid selectors, or PromQL that {{es}} does not evaluate yet (see [Limitations](promql-limitations.md)). Those failures usually return `errorType: bad_data`.

Server errors (HTTP 5xx) and timeout responses reflect operational failures inside {{es}}. Those responses typically use `errorType: timeout` or `execution` depending on the situation.

## Further reading

- [Prometheus query API](https://prometheus.io/docs/prometheus/latest/querying/api/)
- [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md)
- [`PROMQL` command ({{esql}})](/reference/query-languages/esql/commands/promql.md)
