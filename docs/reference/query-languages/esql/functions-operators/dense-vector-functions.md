---
applies_to:
  stack: preview
  serverless: preview
navigation_title: "Dense vector functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-dense-vector-functions
---

# {{esql}} dense vector functions [esql-dense-vector-functions]

:::{tip}
For more examples of these functions in action, refer to [the {{esql}} for search tutorial](/reference/query-languages/esql/esql-search-tutorial.md#vector-search-with-knn-similarity-functions-and-text_embedding).
:::

{{esql}} supports dense vector functions for vector similarity calculations and
k-nearest neighbor search.
Dense vector functions work with [
`dense_vector` fields](/reference/elasticsearch/mapping-reference/dense-vector.md)
and `dense_vector` expressions.

{applies_to}`stack: preview 9.6+` {applies_to}`serverless: preview`
`KNN` on a `dense_vector` expression is a
[runtime search](/reference/query-languages/esql/esql-runtime-search.md#esql-runtime-search-knn): an exact search
over every row that reaches it, instead of an approximate search in the vector index.

{{esql}} supports these vector functions:

:::{include} ../_snippets/lists/dense-vector-functions.md
:::
