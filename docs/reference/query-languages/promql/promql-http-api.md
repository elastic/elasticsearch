---
description: Prometheus-compatible clients can use HTTP endpoints for PromQL queries and metric discovery against time series data in Elasticsearch.
navigation_title: HTTP API
type: overview
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# {{promql}} HTTP API [promql-http-api]

{{prom}}-compatible clients, such as Grafana, use this API to query time series metrics stored in {{es}}.
Calls run under the `/_prometheus/` prefix and follow the [{{prom}} HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/).
{{esql}} queries run {{promql}} with the [`PROMQL` source command](/reference/query-languages/esql/commands/promql.md).

Parameters, methods, and response bodies for each call are in the [{{es}} {{prom}} APIs]({{es-apis}}group/endpoint-prometheus) and [{{es-serverless}} {{prom}} APIs]({{es-serverless-apis}}group/endpoint-prometheus).
For differences from {{prom}}, refer to [{{prom}} compatibility](prometheus-compatibility.md).
To connect Grafana, refer to [Use {{es}} as a {{prom}} data source in Grafana](promql-grafana.md).

For an introduction to running queries with {{promql}} through the HTTP API, go to [](promql/promql-tutorial.md).

## Index scoping [promql-http-api-index-scope]

The base URL you give the client chooses which indices each call reads. Every path has two forms:

- `/_prometheus/api/v1/<path>` reads indices that match `metrics-*`.
- `/_prometheus/{index}/api/v1/<path>` reads the indices in the `{index}` expression, such as `metrics-generic.prometheus-*`.

An alias is accepted in `{index}`. API key privileges apply to the alias name.

Calls read indices with `index.mode: time_series`, such as [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).

The [build information]({{es-apis}}operation/operation-prometheus-status-buildinfo) call accepts `{index}` so a client can use one scoped base URL for every call. The response is the same for either path.

## Request parameter formats [promql-http-api-parameters]

Parameter encodings match the [{{prom}} HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/). See the upstream [format overview](https://prometheus.io/docs/prometheus/latest/querying/api/#format-overview).

For `GET` requests, send parameters in the query string.
For endpoints that support `POST`, send parameters in an `application/x-www-form-urlencoded` request body.
Do not repeat a parameter in both the URL and the form body.

::::{note}
Form-encoded `POST` requests are accepted only when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true` on the {{es}} HTTP interface, and the request is authenticated (not anonymous). The check uses the {{es}} HTTP TLS setting, so deployments that terminate TLS before {{es}} and expose plain HTTP to the node must use `GET` with query-string parameters instead. Unauthenticated or plain-HTTP `POST` requests with `application/x-www-form-urlencoded` bodies return HTTP `406 Not Acceptable`.
::::

## Response format [promql-http-api-response]

Responses use JSON. Successful calls return HTTP 200 with:

```json
{
  "status": "success",
  "data": { ... }
}
```

The `data` object shape depends on the route (for example, `resultType` and `result` for [query]({{es-apis}}operation/operation-prometheus-query) and [query_range]({{es-apis}}operation/operation-prometheus-query-range)).

When the `limit` is reached and the server detects truncation, the response might include a top-level `warnings` array (strings). The preview uses a fixed message such as `results truncated due to limit`.

Numeric sample values in query results are JSON strings (including `NaN`, `+Inf`, and `-Inf`), matching common {{prom}} JSON encoding.

Errors return a non-2xx HTTP status and a JSON body of the form:

```json
{
  "status": "error",
  "errorType": "bad_data",
  "error": "<message>"
}
```

Client errors (HTTP 4xx) mean the request or expression is not valid for this implementation, for example, malformed parameters, parameters not supported yet on a route, invalid selectors, or {{promql}} that {{es}} does not evaluate yet (refer to [{{prom}} compatibility](prometheus-compatibility.md)).
Those failures usually return `errorType: bad_data`.

Server errors (HTTP 5xx) and timeout responses reflect operational failures inside {{es}}.
Those responses typically use `errorType: timeout` or `execution` depending on the situation.

## Related pages

- [{{es}} {{prom}} APIs]({{es-apis}}group/endpoint-prometheus)
- [{{es-serverless}} {{prom}} APIs]({{es-serverless-apis}}group/endpoint-prometheus).
- [{{prom}} remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md)
- [`PROMQL` command ({{esql}})](/reference/query-languages/esql/commands/promql.md)
- [Use {{es}} as a {{prom}} data source in Grafana](promql-grafana.md)
- [{{prom}} compatibility](prometheus-compatibility.md)
