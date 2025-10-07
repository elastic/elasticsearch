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

:::{note}
Vector functions are currently in **preview** and may change in future versions.
They work with `dense_vector` fields and require appropriate field mappings.
:::

{{esql}} supports these vector functions:

:::{include} ../_snippets/lists/vector-functions.md
:::

:::{include} ../_snippets/functions/layout/knn.md
:::

% V_COSINE is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_cosine.md
% :::

% V_DOT_PRODUCT is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_dot_product.md
% :::

% V_HAMMING is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_hamming.md
% :::

% V_L1_NORM is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_l1_norm.md
% :::

% V_L2_NORM is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_l2_norm.md
% :::

% V_MAGNITUDE is currently a hidden feature
% To make it visible again, uncomment this and the line in
% lists/vector-functions.md
% :::{include} ../_snippets/functions/layout/v_magnitude.md
% :::
