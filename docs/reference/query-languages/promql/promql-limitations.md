---
description: Execution and HTTP constraints for PromQL in Elasticsearch, including unsupported constructs and instant-query behavior.
navigation_title: Limitations
applies_to:
  stack: preview 9.4.0
  serverless: preview
products:
  - id: elasticsearch
---

# PromQL limitations [promql-limitations]

PromQL reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
The following constraints apply to execution in {{es}}, including the [Prometheus-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless stated otherwise.
They describe behavioral differences and unsupported areas compared with upstream Prometheus.

::::{warning}
This functionality is in technical preview and might be changed or removed in a future release.
Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::

## GET requests only (HTTP API) [promql-limitations-get-only]

Only `GET` is supported on `/_prometheus/` routes in this preview.
`POST` with `application/x-www-form-urlencoded` bodies is rejected as a CSRF safeguard.
If your Prometheus-compatible client defaults to `POST` for queries, configure it to use `GET` instead.

## Unsupported Prometheus query parameters (HTTP API) [promql-limitations-unsupported-query-params]

The [PromQL HTTP API](promql-http-api.md) documents only the parameters each route accepts. Extra parameters from the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) are not supported yet. {{es}} does not ignore them: the request fails with 400 Bad Request. Configure clients and integrations to omit them (for example, there is no per-request `timeout` query parameter). Cancellation and runtime limits follow {{esql}} and cluster settings.

## Instant query is an approximation [promql-limitations-instant-query]

For now, `/api/v1/query` is implemented as a five-minute range query ending at `time`, returning the last sample per series.
Optional `lookback_delta` from the Prometheus API is not supported yet on this route. See [Unsupported Prometheus query parameters](#promql-limitations-unsupported-query-params) and the [`query` endpoint](promql-http-api.md#promql-http-api-query-instant) documentation.

## Staleness markers [promql-limitations-staleness]

When a scrape target disappears, Prometheus ingests staleness markers.
Instant vector selectors then omit those series from instant-vector results, so metrics that stopped reporting do not appear as still current.

{{es}} does not apply Prometheus staleness markers yet.
For now, a series stops appearing in results only once all its samples fall outside the evaluation window, rather than disappearing as soon as data stops arriving.

## Unsupported PromQL constructs [promql-limitations-unsupported-constructs]

The majority of PromQL expressions run unchanged.
The following constructs are not evaluated yet, so they return a client error (4xx):

- Binary set operators: `and`, `or`, `unless`
- Group modifiers: `on(...)`, `group_left`, `group_right`
- Functions: `histogram_quantile`, `predict_linear`, `label_join`

## Metric metadata `help` (HTTP API) [promql-limitations-metadata-help]

On [`/api/v1/metadata`](promql-http-api.md#promql-http-api-metadata-endpoint), each metric includes a `help` string shaped like Prometheus `HELP` lines.
In this preview the `help` field is always an empty string. Help text from metric definitions is not surfaced yet.

## Exemplar queries (HTTP API) [promql-limitations-exemplars]

`/api/v1/query_exemplars` is not implemented yet.
Turn off exemplar queries in your Prometheus-compatible client for now (for example, the Grafana data source exemplars option) so it does not call that endpoint.
