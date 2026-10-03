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

% Cleanup: once 9.6 is released, remove all `=9.4` entries, here and in
% _snippets/promql-unsupported-constructs.md. They are kept at the bottom of each list, section,
% and of the page so they are easy to trim. Rationale: Elastic maintains only the two most
% recent minors, so 9.4 leaves maintenance when 9.6 ships, and PromQL was only a tech preview in 9.4.
% At the same time, consider simplifying the page frontmatter to `stack: ga 9.5+`.

PromQL reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
The following constraints apply to execution in {{es}}, including the [Prometheus-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless stated otherwise.
They describe behavioral differences and unsupported areas compared with upstream Prometheus.

## Form-encoded POST requests (HTTP API) [promql-limitations-form-post]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`

Routes that document `POST` accept parameters in an `application/x-www-form-urlencoded` body only when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true` on the Elasticsearch HTTP interface, and the request is authenticated.
TLS that terminates before Elasticsearch (plain HTTP to the node) does not satisfy this check.
Use `GET` with query-string parameters when `POST` is unavailable.

## Unsupported Prometheus query parameters (HTTP API) [promql-limitations-unsupported-query-params]

The [PromQL HTTP API](promql-http-api.md) documents only the parameters each route accepts. Extra parameters from the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) are not supported yet. {{es}} does not ignore them: the request fails with 400 Bad Request. Configure clients and integrations to omit them (for example, there is no per-request `timeout` query parameter). Cancellation and runtime limits follow {{esql}} and cluster settings.

## Instant queries [promql-limitations-instant-query]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `/api/v1/query` evaluates the expression at `time`. Selectors without a range look back a fixed five minutes for the latest sample, which matches the Prometheus default lookback delta.

Optional `lookback_delta` from the Prometheus API is not supported yet on this route. See [Unsupported Prometheus query parameters](#promql-limitations-unsupported-query-params) and the [`query` endpoint](promql-http-api.md#promql-http-api-query-instant) documentation.

{applies_to}`stack: preview =9.4` `/api/v1/query` is implemented as a five-minute range query ending at `time`, returning the last sample per series.

## Staleness markers [promql-limitations-staleness]

When a scrape target disappears, Prometheus ingests staleness markers.
Instant vector selectors then omit those series from instant-vector results, so metrics that stopped reporting do not appear as still current.

{{es}} does not apply Prometheus staleness markers yet.
For now, a series stops appearing in results only once all its samples fall outside the evaluation window, rather than disappearing as soon as data stops arriving.

## Unsupported PromQL constructs [promql-limitations-unsupported-constructs]

:::{include} _snippets/promql-unsupported-constructs.md
:::

## Native histograms [promql-limitations-native-histograms]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`

{{es}} provides basic support for Prometheus native histograms (the `exponential_histogram` type in {{es}}).
The following query patterns work today:

- `histogram_quantile` on native histograms, including after aggregation: `histogram_quantile(0.9, sum by (job) (increase(metric[10m])))`
- `histogram_count`, `histogram_sum`, `histogram_avg`, and `histogram_fraction` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` on native histograms
- `increase` on native histograms
- `sum` aggregation on native histograms to aggregate across series

In particular, the following features are not available yet for native histograms:

- `rate`: If possible, use `increase` instead. The `rate` function produces fractional bucket counts that native histograms do not support yet. Most queries that use `rate` can be rewritten with `increase` (for example, `histogram_quantile(0.99, sum by (job) (increase(metric[5m])))` instead of using `rate`).
- Native histograms as direct result types: Queries that return a raw native histogram (such as a bare selector `my_histogram` or `increase(my_histogram[5m])` without wrapping in a histogram function) are not supported. Wrap selectors in `histogram_quantile`, `histogram_count`, `histogram_sum`, `histogram_avg`, or `histogram_fraction` to obtain scalar results.
- `irate` and `delta`
- Arithmetic operators on native histograms: `+`, `-`, `*`, `/`
- `histogram_stddev`

## Metric metadata `help` (HTTP API) [promql-limitations-metadata-help]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`

On [`/api/v1/metadata`](promql-http-api.md#promql-http-api-metadata-endpoint), each metric includes a `help` string shaped like Prometheus `HELP` lines.
Metric definition help text is not surfaced yet, so the `help` field remains an empty string.

## Exemplar queries (HTTP API) [promql-limitations-exemplars]

`/api/v1/query_exemplars` is not implemented yet, so exemplar queries are not supported.
To avoid errors, turn off exemplar queries in your Prometheus-compatible client.
In Grafana, go to **Data sources → Elasticsearch → Exemplars** and disable all configured exemplar links.

## Time bucket alignment [promql-limitations-time-buckets]

{applies_to}`stack: preview =9.4`

Time buckets align to fixed calendar boundaries rather than the query start time.
This can cause slight differences from Prometheus, especially for short ranges or large step sizes.
