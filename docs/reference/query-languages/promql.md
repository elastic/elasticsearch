---
description: Query metrics in Elasticsearch time series data streams with PromQL through the ES|QL runtime and a Prometheus-compatible HTTP API.
navigation_title: PromQL
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# {{promql}} reference [promql-language]

The [{{prom}} Query Language ({{promql}})](https://prometheus.io/docs/prometheus/latest/querying/basics/) is a functional query language to select and aggregate metrics.


## What is {{promql}} in {{es}}? [promql-what]

{{es}} supports {{promql}} queries for metrics in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
{{promql}} support is not limited to metrics ingested through [{{prom}} remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md).
All TSDS are supported, including metrics ingested through [OpenTelemetry Protocol (OTLP)](docs-content://manage-data/data-store/data-streams/tsds-ingest-otlp.md), and the [bulk API]({{es-apis}}operation/operation-bulk).

{{es}} supports {{promql}} in two ways: through a [{{prom}}-compatible HTTP API](promql/promql-http-api.md) (for {{prom}}-compatible clients) and as a [`PROMQL` source command](/reference/query-languages/esql/commands/promql.md) inside piped {{esql}} queries that allow post-processing through regular {{esql}}.

## How does it work? [promql-how]

When you use the {{prom}}-compatible HTTP API or embed {{promql}} in an {{esql}} query through the `PROMQL` source command, {{es}} parses {{promql}} into {{esql}} logical plans and evaluates those plans against TSDS metrics.

{{es}} uses the same {{esql}} compute engine for {{promql}} as for the [`TS` source command](/reference/query-languages/esql/commands/ts.md).

## In this section

* [Get started](promql/promql-tutorial.md): Ingest sample {{prom}} metrics and query them with {{promql}} through the HTTP API and {{esql}}.
* [HTTP API](promql/promql-http-api.md): {{prom}}-compatible `/_prometheus/` endpoints for queries and discovery.
* [Functions](promql/functions.md): {{promql}} function reference organized by category, including {{es}}-specific differences from {{prom}}.
* [Operators](promql/operators.md): {{promql}} operator reference organized by category, including {{es}}-specific differences from {{prom}}.
* [{{prom}} data source in Grafana](promql/promql-grafana.md): Use {{es}} as a {{prom}} data source in Grafana for dashboards, autocomplete, and Metrics Drilldown.
* [{{prom}} compatibility](promql/prometheus-compatibility.md): What {{es}} supports, what isn't supported yet, and what behaves differently from {{prom}}, for both {{promql}} and the HTTP API.

