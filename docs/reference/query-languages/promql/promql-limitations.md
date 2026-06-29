---
description: Execution and HTTP constraints for PromQL in Elasticsearch, including unsupported constructs and instant-query behavior.
navigation_title: Limitations
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# PromQL limitations [promql-limitations]

PromQL reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
The following constraints apply to execution in {{es}}, including the [Prometheus-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless stated otherwise.
They describe behavioral differences and unsupported areas compared with upstream Prometheus.

## Form-encoded POST requests (HTTP API) [promql-limitations-form-post]

Routes that document `POST` accept parameters in an `application/x-www-form-urlencoded` body only when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true` on the Elasticsearch HTTP interface, and the request is authenticated.
TLS that terminates before Elasticsearch (plain HTTP to the node) does not satisfy this check.
Use `GET` with query-string parameters when `POST` is unavailable.

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

- Binary set operators: `and` and `unless`. The `or` operator is supported only at the top level of an expression; nested `or` returns a client error (4xx).
- Group modifiers: `on(...)`, `group_left`, `group_right`
- Functions: see [Not yet supported](functions.md#promql-not-supported) for the full list of recognized but unimplemented functions.

## Native histograms [promql-limitations-native-histograms]

Prometheus native histograms are not supported yet.
PromQL functions operate on float samples; series stored as native histograms are not evaluated.

## Metric metadata `help` (HTTP API) [promql-limitations-metadata-help]

On [`/api/v1/metadata`](promql-http-api.md#promql-http-api-metadata-endpoint), each metric includes a `help` string shaped like Prometheus `HELP` lines.
Metric definition help text is not surfaced yet, so the `help` field remains an empty string.

## Exemplar queries (HTTP API) [promql-limitations-exemplars]

`/api/v1/query_exemplars` is not implemented yet, so exemplar queries are not supported.
To avoid errors, turn off exemplar queries in your Prometheus-compatible client.
In Grafana, go to **Data sources → Elasticsearch → Exemplars** and disable all configured exemplar links.
