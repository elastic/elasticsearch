---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/specialized-queries.html
---

# Specialized queries [specialized-queries]

This group contains queries which do not fit into the other groups:

[`distance_feature` query](/reference/query-languages/query-dsl/query-dsl-distance-feature-query.md)
:   A query that computes scores based on the dynamically computed distances between the origin and documents' `date`, `date_nanos`, and `geo_point` fields. It is able to efficiently skip non-competitive hits.

[`more_like_this` query](/reference/query-languages/query-dsl/query-dsl-mlt-query.md)
:   This query finds documents which are similar to the specified text, document, or collection of documents.

[`percolate` query](/reference/query-languages/query-dsl/query-dsl-percolate-query.md)
:   This query finds queries that are stored as documents that match with the specified document.

[`rank_feature` query](/reference/query-languages/query-dsl/query-dsl-rank-feature-query.md)
:   A query that computes scores based on the values of numeric features and is able to efficiently skip non-competitive hits.

[`script` query](/reference/query-languages/query-dsl/query-dsl-script-query.md)
:   This query allows a script to act as a filter. Also see the [`function_score` query](/reference/query-languages/query-dsl/query-dsl-function-score-query.md).

[`script_score` query](/reference/query-languages/query-dsl/query-dsl-script-score-query.md)
:   A query that allows to modify the score of a sub-query with a script.

[`wrapper` query](/reference/query-languages/query-dsl/query-dsl-wrapper-query.md)
:   A query that accepts other queries as json or yaml string.

[`pinned` query](/reference/query-languages/query-dsl/query-dsl-pinned-query.md)
:   A query that promotes selected documents over others matching a given query.

[`rule` query](/reference/query-languages/query-dsl/query-dsl-rule-query.md)
:   A query that supports applying query-based contextual rules, defined using the [Query Rules API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules), to a given query.










