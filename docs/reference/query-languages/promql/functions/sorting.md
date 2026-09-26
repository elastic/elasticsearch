---
description: PromQL sorting functions in Elasticsearch that order instant-vector series by sample value.
navigation_title: Sorting functions
applies_to:
  stack: ga 9.6
  serverless: ga
products:
  - id: elasticsearch
---

# Sorting functions [promql-sorting-functions]

These functions order the series of an instant vector by sample value. Ordering is observable only on instant queries. Range queries return the series in their input order, without applying the requested ordering.

:::{include} ../_snippets/generated/x-pack-esql/functions/lists/sorting.md
:::
