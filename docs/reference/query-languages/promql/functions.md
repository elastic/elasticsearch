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

PromQL functions in {{es}} are grouped into the following categories:

* [Range vector functions](functions/range-vector.md): Functions that take a range vector and return an instant vector, such as `rate`, `increase`, and the `_over_time` aggregates.
* [Aggregation functions](functions/aggregation.md): Functions that aggregate an instant vector across series, such as `sum`, `avg`, and `quantile`.
* [Histogram functions](functions/histogram.md): Functions that operate on histogram metrics.
* [Math functions](functions/math.md): Mathematical, trigonometric, and rounding functions.
* [Date and time functions](functions/date-time.md): Functions that extract date and time components from timestamps.
* [Conversion functions](functions/conversion.md): Functions that convert between scalars and instant vectors.

## Not yet supported [promql-not-supported]

The following PromQL functions are recognized but not yet supported in {{es}}. Using them returns a client error (4xx):

:::{include} _snippets/generated/x-pack-esql/functions/not-supported.md
:::
