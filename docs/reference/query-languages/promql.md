---
description: Query metrics in Elasticsearch time series data streams with PromQL through the ES|QL runtime and a Prometheus-compatible HTTP API.
navigation_title: PromQL
applies_to:
  stack: preview 9.4.0
  serverless: preview
products:
  - id: elasticsearch
---

# PromQL reference [promql-language]

PromQL in {{es}} queries metrics in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS) using the same label and series model as [Prometheus](https://prometheus.io/docs/prometheus/latest/querying/basics/).

::::{warning}
This functionality is in technical preview and might be changed or removed in a future release.
Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::

## What is PromQL in {{es}}? [promql-what]

PromQL is the Prometheus query language. In {{es}}, the {{esql}} compute engine plans and executes expressions against TSDS-backed indices. Labels map to dimensions and metric names to the index mapping. Common ways to ingest metrics into a TSDS include [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md), [OpenTelemetry Protocol (OTLP)](docs-content://manage-data/data-store/data-streams/tsds-ingest-otlp.md) ingestion, and the [bulk API]({{es-apis}}operation/operation-bulk).

## How does it work? [promql-how]

When you use the Prometheus-compatible HTTP API or embed PromQL in an {{esql}} query through the `PROMQL` source command, {{es}} parses PromQL into {{esql}} logical plans, evaluates those plans against TSDS metrics, and returns tabular or Prometheus-shaped results.

Execution relies on the same {{esql}} compute engine {{es}} uses when you invoke the `TS` source command.

Programmatic access (Prometheus-compatible clients)\
Use the `/_prometheus/` HTTP API so tools that speak the [Prometheus query API](https://prometheus.io/docs/prometheus/latest/querying/api/), for example Grafana, can run PromQL against {{es}}.

{{esql}} queries\
Use the {{esql}} `PROMQL` source command when you want PromQL as part of a piped {{esql}} query.

## In this section

* [HTTP API](promql/promql-http-api.md): Prometheus-compatible `/_prometheus/` endpoints for queries and discovery.
* [Limitations](promql/promql-limitations.md): How behavior differs from Prometheus, including unsupported PromQL constructs, HTTP behavior, instant-query nuances, staleness semantics, exemplars, and related topics.

## Relevant sections

* [`PROMQL` command ({{esql}})](/reference/query-languages/esql/commands/promql.md): PromQL inside piped {{esql}} queries.
