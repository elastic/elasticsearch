---
description: PromQL comparison operators in Elasticsearch.
navigation_title: Comparison operators
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# Comparison operators [promql-comparison-operators]

These binary operators compare scalars and instant vectors. By default they act as filters; with the [`bool` modifier](../operators.md#promql-operators-bool-modifier) they return `0` or `1`.

In {{es}}, a comparison is evaluated only at the top level of an expression and only with a scalar literal on the right-hand side. Comparisons between two instant vectors, and nested comparisons, return a client error (4xx). See [PromQL limitations](../promql-limitations.md#promql-limitations-unsupported-constructs).

:::{include} ../_snippets/generated/x-pack-esql/operators/lists/comparison.md
:::
