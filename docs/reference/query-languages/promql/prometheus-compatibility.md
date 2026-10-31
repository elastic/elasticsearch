---
description: How PromQL in Elasticsearch differs from Prometheus, including unsupported constructs, evaluation differences, and HTTP API constraints.
navigation_title: Prometheus compatibility
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# Prometheus compatibility [prometheus-compatibility]

% Each section lives in _snippets/compatibility/ so other pages can include it. Sections and headings follow the
% Prometheus querying docs (basics, operators, functions, HTTP API). Rows are numbered across all sections. A row
% without a version badge applies to every version.

PromQL in {{es}} reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
Most PromQL queries run unchanged and return the same results as Prometheus v3.x.x.
This page lists the PromQL features {{es}} doesn't support yet and where its behavior differs by design, so you can tell in advance whether a query runs and what it returns.
It applies to both the [Prometheus-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless a section says otherwise.

- [Basics](#prometheus-compatibility-basics): data types, literals, selectors, modifiers, subqueries, and staleness.
- [Operators](#prometheus-compatibility-operators): binary and aggregation operators, and vector matching.
- [Functions](#prometheus-compatibility-functions).
- [HTTP API](#prometheus-compatibility-http-api).

Each row compares what {{es}} does with what Prometheus does for the same query, and links the issue that tracks the work where there is one.
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
