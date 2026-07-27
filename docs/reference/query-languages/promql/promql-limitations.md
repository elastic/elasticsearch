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

- Binary set operators: `and` and `unless`. The `or` operator is supported only at the top level of an expression and a top-level `or` chain supports at most 8 operands; a nested `or`, or a chain of more than 8 operands, returns a client error (4xx).
- Comparison operators: evaluated only at the top level of an expression and only with a scalar literal on the right-hand side. Comparisons between two instant vectors, and nested comparisons, return a client error (4xx).
- Group modifiers: `on(...)`, `ignoring(...)`, `group_left`, `group_right`
- Functions: see [Not yet supported](functions.md#promql-not-supported) for the full list of recognized but unimplemented functions.

## Native histograms [promql-limitations-native-histograms]

{applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`

{{es}} provides basic support for Prometheus native histograms (the `exponential_histogram` type in {{es}}).
The following query patterns work today:

- `histogram_quantile` on native histograms, including after aggregation: `histogram_quantile(0.9, sum by (job) (increase(metric[10m])))`
- `histogram_count`, `histogram_sum`, and `histogram_avg` on native histograms
- `increase` on native histograms
- `sum` aggregation on native histograms to aggregate across series

In particular, the following features are not available yet for native histograms:

- `rate`: If possible, use `increase` instead. The `rate` function produces fractional bucket counts that native histograms do not support yet. Most queries that use `rate` can be rewritten with `increase` (for example, `histogram_quantile(0.99, sum by (job) (increase(metric[5m])))` instead of using `rate`).
- Native histograms as direct result types**: Queries that return a raw native histogram (such as a bare selector `my_histogram` or `increase(my_histogram[5m])` without wrapping in a histogram function) are not supported. Wrap selectors in `histogram_quantile`, `histogram_count`, `histogram_sum`, or `histogram_avg` to obtain scalar results.
- `irate` and `delta`
- Arithmetic operators on native histograms: `+`, `-`, `*`, `/`
- `histogram_fraction` and `histogram_stddev`

## Metric metadata `help` (HTTP API) [promql-limitations-metadata-help]

On [`/api/v1/metadata`](promql-http-api.md#promql-http-api-metadata-endpoint), each metric includes a `help` string shaped like Prometheus `HELP` lines.
Metric definition help text is not surfaced yet, so the `help` field remains an empty string.

## Exemplar queries (HTTP API) [promql-limitations-exemplars]

`/api/v1/query_exemplars` is not implemented yet, so exemplar queries are not supported.
To avoid errors, turn off exemplar queries in your Prometheus-compatible client.
In Grafana, go to **Data sources → Elasticsearch → Exemplars** and disable all configured exemplar links.
