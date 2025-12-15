---
applies_to:
  stack: preview
  serverless: preview
navigation_title: "Dense vector functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-dense-vector-functions
---

# {{esql}} dense vector functions [esql-dense-vector-functions]

{{esql}} supports dense vector functions for vector similarity calculations and
k-nearest neighbor search.
Dsense vector functions work with [
`dense_vector` fields](/reference/elasticsearch/mapping-reference/dense-vector.md)
and require appropriate field mappings.

{{esql}} supports these vector functions:

:::{include} ../_snippets/lists/dense-vector-functions.md
:::

:::{include} ../_snippets/functions/layout/knn.md
:::

:::{include} ../_snippets/functions/layout/text_embedding.md
:::

:::{include} ../_snippets/functions/layout/v_cosine.md
:::

:::{include} ../_snippets/functions/layout/v_dot_product.md
:::

:::{include} ../_snippets/functions/layout/v_hamming.md
:::

:::{include} ../_snippets/functions/layout/v_l1_norm.md
:::

lists/dense-vector-functions.md
:::{include} ../_snippets/functions/layout/v_l2_norm.md
:::

% V_MAGNITUDE is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/dense-vector-functions.md
% :::{include} ../_snippets/functions/layout/v_magnitude.md
% :::
