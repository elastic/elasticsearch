---
applies_to:
  stack: ga
  serverless: ga
navigation_title: "Grouping functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-group-functions
---

# {{esql}} grouping functions [esql-group-functions]


The [`STATS`](/reference/query-languages/esql/commands/stats-by.md) command supports these grouping functions:

:::{include} ../_snippets/lists/grouping-functions.md
:::

The [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md) command supports these grouping functions:

* [`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-bucket)
* [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-tbucket)


:::{include} ../_snippets/functions/layout/bucket.md
:::

:::{include} ../_snippets/functions/layout/tbucket.md
:::


:::{note}
The `CATEGORIZE` function requires a [platinum license](https://www.elastic.co/subscriptions).
:::

:::{include} ../_snippets/functions/layout/categorize.md
:::

