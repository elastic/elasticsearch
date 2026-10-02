---
description: PromQL function reference for Elasticsearch, grouped by category, with Elasticsearch-specific differences from Prometheus.
navigation_title: Functions
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# PromQL functions [promql-functions]

These functions are based on the [Prometheus query functions](https://prometheus.io/docs/prometheus/latest/querying/functions/). Refer to the Prometheus documentation for the full semantics, and note any {{es}}-specific differences called out per function.

## Functions overview [promql-functions-overview]

### Range vector functions

Functions that take a range vector and return an instant vector.

::::{dropdown} Range vector function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/range-vector-overview.md
:::
::::

### Aggregation functions

Functions that aggregate an instant vector across series.

::::{dropdown} Aggregation function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/aggregation-overview.md
:::
::::

### Histogram functions

Functions that operate on histogram metrics.

::::{dropdown} Histogram function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/histogram-overview.md
:::
::::

### Math functions

Mathematical, trigonometric, and rounding functions.

::::{dropdown} Math function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/math-overview.md
:::
::::

### Date and time functions

Functions that extract date and time components from timestamps.

::::{dropdown} Date and time function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/date-time-overview.md
:::
::::

### Conversion functions

Functions that convert between scalars and instant vectors.

::::{dropdown} Conversion function list
:open:
:::{include} _snippets/generated/x-pack-esql/functions/lists/conversion-overview.md
:::
::::

## Not yet supported [promql-not-supported]

The following PromQL functions are recognized but not yet supported in {{es}}. Using them returns a client error (4xx):

:::{include} _snippets/generated/x-pack-esql/functions/not-supported.md
:::
