---
navigation_title: "Grouping functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-group-functions
---

# {{esql}} grouping functions [esql-group-functions]


The [`STATS`](/reference/query-languages/esql/commands/processing-commands.md#esql-stats-by) command supports these grouping functions:

:::{include} ../_snippets/lists/grouping-functions.md
:::


:::{include} ../_snippets/functions/layout/bucket.md
:::

:::{note} 
The `CATEGORIZE` function requires a [platinum license](https://www.elastic.co/subscriptions).
:::

:::{include} ../_snippets/functions/layout/categorize.md
:::

