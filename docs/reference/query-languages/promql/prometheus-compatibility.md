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

% Cleanup: once 9.6 is released, remove all `=9.4` rows in _snippets/compatibility/.
% They are kept at the bottom of each table so they are easy to trim. Rationale: Elastic maintains only the two most
% recent minors, so 9.4 leaves maintenance when 9.6 ships, and PromQL was only a tech preview in 9.4.
% At the same time, consider simplifying the page frontmatter to `stack: ga 9.5+`.
% Each section lives in _snippets/compatibility/ so other pages can include it. Rows are numbered across all
% sections. A row without a version badge applies to every version.

PromQL in {{es}} reads metrics stored in [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
Most PromQL queries run unchanged and return the same results as Prometheus 3.
This page lists the PromQL features {{es}} doesn't support yet and where its behavior differs by design, so you can tell in advance whether a query runs and what it returns.
It applies to both the [Prometheus-compatible HTTP API](promql-http-api.md) and the {{esql}} [`PROMQL`](/reference/query-languages/esql/commands/promql.md) source command, unless a section says otherwise.

Each row compares what {{es}} does with what Prometheus does for the same query, and links the issue that tracks the work where there is one.
A row with a version badge applies only to those versions. A row without one applies to every version.

- [Unsupported constructs](#prometheus-compatibility-unsupported-constructs) return an error.
- [Lookback and time](#prometheus-compatibility-lookback), [metric names](#prometheus-compatibility-metric-name), and [function results](#prometheus-compatibility-functions) list queries that run but return a different result.
- [Native histograms](#prometheus-compatibility-native-histograms) and the [HTTP API](#prometheus-compatibility-http-api) cover the remaining differences.

Individual functions and operators can have further differences, which are listed under **Differences from Prometheus** on each [function](functions.md) and [operator](operators.md) reference entry.

## Unsupported constructs [prometheus-compatibility-unsupported-constructs]

:::{include} _snippets/compatibility/unsupported-constructs.md
:::

## Lookback and time [prometheus-compatibility-lookback]

:::{include} _snippets/compatibility/lookback.md
:::

## Metric names in results [prometheus-compatibility-metric-name]

:::{include} _snippets/compatibility/metric-names.md
:::

## Function results [prometheus-compatibility-functions]

:::{include} _snippets/compatibility/function-results.md
:::

## Native histograms [prometheus-compatibility-native-histograms]

:::{include} _snippets/compatibility/native-histograms.md
:::

## HTTP API [prometheus-compatibility-http-api]

:::{include} _snippets/compatibility/http-api.md
:::
