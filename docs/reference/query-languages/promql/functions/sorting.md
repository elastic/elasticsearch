---
description: PromQL sorting functions in Elasticsearch that order instant-vector series by sample value or by label values.
navigation_title: Sorting functions
applies_to:
  stack: ga 9.6
  serverless: ga
products:
  - id: elasticsearch
---

# Sorting functions [promql-sorting-functions]

These functions order the series of an instant vector by sample value (`sort` / `sort_desc`) or by listed label values (`sort_by_label` / `sort_by_label_desc`). Ordering is observable only on instant queries; range queries keep the input order.

:::{include} ../_snippets/generated/x-pack-esql/functions/lists/sorting.md
:::
