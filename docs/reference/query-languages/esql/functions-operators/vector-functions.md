---
applies_to:
  stack: preview
  serverless: preview
navigation_title: "Vector functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-vector-functions
---

# {{esql}} vector functions [esql-vector-functions]

{{esql}} supports vector functions for dense vector similarity calculations and k-nearest neighbor search.

These functions are primarily used for:
- Computing similarity between dense vectors using various similarity metrics
- Finding the k-nearest neighbors in vector space
- Calculating vector magnitudes and norms

:::{note}
Vector functions are currently in **preview** and may change in future versions. 
They work with `dense_vector` fields and require appropriate field mappings.
:::

{{esql}} supports these vector functions:

:::{include} ../_snippets/lists/vector-functions.md
:::

:::{include} ../_snippets/functions/layout/knn.md
:::

:::{include} ../_snippets/functions/layout/v_cosine.md
:::

:::{include} ../_snippets/functions/layout/v_dot_product.md
:::

:::{include} ../_snippets/functions/layout/v_hamming.md
:::

:::{include} ../_snippets/functions/layout/v_l1_norm.md
:::

:::{include} ../_snippets/functions/layout/v_l2_norm.md
:::

:::{include} ../_snippets/functions/layout/v_magnitude.md
:::