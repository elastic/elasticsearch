---
description: PromQL range vector functions in Elasticsearch, including rate, increase, and the _over_time aggregates.
navigation_title: Range vector functions
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# Range vector functions [promql-range-vector-functions]

These functions take a range vector and return an instant vector with one value per series, computed over the selected time window.

:::{include} ../_snippets/generated/x-pack-esql/functions/lists/range-vector.md
:::
