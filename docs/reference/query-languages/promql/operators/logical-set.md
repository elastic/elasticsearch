---
description: PromQL logical/set operators in Elasticsearch.
navigation_title: Logical/set operators
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# Logical/set operators [promql-logical-set-operators]

These binary operators combine two instant vectors by matching their label sets. In {{es}}, the `and` and `unless` operators are not supported yet. {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` The `or` (union) operator is evaluated only at the top level of an expression. For details, see [Not yet supported](../operators.md#promql-operators-not-supported) and [PromQL limitations](../promql-limitations.md#promql-limitations-unsupported-constructs).

:::{include} ../_snippets/generated/x-pack-esql/operators/lists/logical-set.md
:::
