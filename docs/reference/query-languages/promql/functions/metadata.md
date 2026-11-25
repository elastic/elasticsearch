---
description: PromQL metadata functions in Elasticsearch that derive labels from existing label values or from a selector.
navigation_title: Metadata functions
applies_to:
  stack: ga 9.6
  serverless: ga
products:
  - id: elasticsearch
---

# Metadata functions [promql-metadata-functions]

These functions work on the labels of a result: `label_join` and `label_replace` derive a new label, or overwrite an existing one, from the values of other labels, and `absent` returns a series whose labels come from its selector's equality matchers.

:::{include} ../_snippets/generated/x-pack-esql/functions/lists/metadata.md
:::
