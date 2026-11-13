---
applies_to:
  stack: ga
  serverless: ga
navigation_title: "Type conversion functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-type-conversion-functions
---

# {{esql}} type conversion functions [esql-type-conversion-functions]


::::{tip}
{{esql}} supports implicit casting from string literals to certain data types. Refer to [implicit casting](/reference/query-languages/esql/esql-implicit-casting.md) for details.

::::


A value that can't be converted, such as `"abc"::long`, returns `null` with a warning.

{applies_to}`stack: ga 9.7` {applies_to}`serverless: ga` So does a value whose type the conversion doesn't accept, such as `ip_field::long` or `true::ip`. In earlier versions, the query fails. For a field mapped to different types across indices, refer to [union types](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types).

A value that can't be converted, such as `"abc"::long`, returns `null` with a warning.

{applies_to}`stack: ga 9.7` {applies_to}`serverless: ga` So does a value whose type the conversion doesn't accept, such as `ip_field::long` or `true::ip`. In earlier versions, the query fails. For a field mapped to different types across indices, refer to [union types](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types).

{{esql}} supports these type conversion functions:

:::{include} ../_snippets/lists/type-conversion-functions.md
:::
