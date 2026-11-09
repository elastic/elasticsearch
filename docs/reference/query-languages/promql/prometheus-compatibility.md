---
description: How PromQL in Elasticsearch differs from Prometheus, including unsupported constructs, evaluation differences, and HTTP API constraints.
navigation_title: Prometheus compatibility
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# {{prom}} compatibility [prometheus-compatibility]

% Each section lives in _snippets/compatibility/ so other pages can include it. Sections and headings follow the
% Prometheus querying docs (basics, operators, functions, HTTP API). Rows are numbered across all sections. A row
% without a version badge applies to every version.

{{promql}} in {{es}} reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
Most {{promql}} queries run unchanged and return the same results as {{prom}} 3.x.
This page lists the {{promql}} features {{es}} doesn't support yet and where its behavior differs by design, so you can tell in advance whether a query runs and what it returns.
It applies to both the [{{prom}}-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless a section says otherwise.

The following sections follow the [{{prom}} querying documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/), so each difference sits under the heading that documents the construct in {{prom}}.
Each section has up to three parts:

- **Supported**: what runs and returns the same result as in {{prom}}.
- **Not supported yet**: {{es}} rejects the query with an error. {{prom}} supports every construct in these tables, some only with a feature flag.
- **Behaves differently**: the query runs in both, but the result differs.

Rows link the issue that tracks the work, where there is one.
A row with a version badge applies only to those versions. A row without one applies to every version.

## Basics [prometheus-compatibility-basics]

:::{include} _snippets/compatibility/basics.md
:::

## Operators [prometheus-compatibility-operators]

:::{include} _snippets/compatibility/operators.md
:::

## Functions [prometheus-compatibility-functions]

:::{include} _snippets/compatibility/functions.md
:::

## HTTP API [prometheus-compatibility-http-api]

:::{include} _snippets/compatibility/http-api.md
:::
