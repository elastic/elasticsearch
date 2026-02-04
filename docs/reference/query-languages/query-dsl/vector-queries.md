---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/vector-queries.html
---

# Vector queries [vector-queries]

Vector queries are specialized queries that work on vector fields to efficiently perform [semantic search](docs-content://solutions/search/semantic-search.md).

[`knn` query](/reference/query-languages/query-dsl/query-dsl-knn-query.md)
:   A query that finds the *k* nearest vectors to a query vector for [`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md) fields, as measured by a similarity metric.

[`sparse_vector` query](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md)
:   A query used to search [`sparse_vector`](/reference/elasticsearch/mapping-reference/sparse-vector.md) field types.

[`semantic` query](/reference/query-languages/query-dsl/query-dsl-semantic-query.md)
:   A query that allows you to perform semantic search on [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) fields.


## Deprecated vector queries [_deprecated_vector_queries]

The following queries have been deprecated and will be removed in the near future. Use the [`sparse_vector` query](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) query instead.

[`text_expansion` query](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md)
:   A query that allows you to perform sparse vector search on [`sparse_vector`](/reference/elasticsearch/mapping-reference/sparse-vector.md) or [`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md) fields.

[`weighted_tokens` query](/reference/query-languages/query-dsl/query-dsl-weighted-tokens-query.md)
:   Allows to perform text expansion queries optimizing for performance.






