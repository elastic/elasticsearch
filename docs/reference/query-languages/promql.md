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

The [Prometheus Query Language (PromQL)](https://prometheus.io/docs/prometheus/latest/querying/basics/) is a functional query language to select and aggregate metrics.


::::{warning}
This functionality is in technical preview and might be changed or removed in a future release.
Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::

## What is PromQL in {{es}}? [promql-what]

{{es}} supports PromQL queries for metrics in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
PromQL support is not limited to metrics ingested through [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md).
All TSDS are supported, including metrics ingested through [OpenTelemetry Protocol (OTLP)](docs-content://manage-data/data-store/data-streams/tsds-ingest-otlp.md), and the [bulk API]({{es-apis}}operation/operation-bulk).

{{es}} supports PromQL in two ways: through a [Prometheus-compatible HTTP API](promql/promql-http-api.md) (for Prometheus-compatible clients) and as a [`PROMQL` source command](/reference/query-languages/esql/commands/promql.md) inside piped {{esql}} queries that allow post-processing through regular {{esql}}.

## How does it work? [promql-how]

When you use the Prometheus-compatible HTTP API or embed PromQL in an {{esql}} query through the `PROMQL` source command, {{es}} parses PromQL into {{esql}} logical plans and evaluates those plans against TSDS metrics.

{{es}} uses the same {{esql}} compute engine for PromQL as for the [`TS` source command](/reference/query-languages/esql/commands/ts.md).

## In this section

* [HTTP API](promql/promql-http-api.md): Prometheus-compatible `/_prometheus/` endpoints for queries and discovery.
* [Limitations](promql/promql-limitations.md): How behavior differs from Prometheus, including unsupported PromQL constructs, HTTP behavior, instant-query nuances, staleness semantics, exemplars, and related topics.

