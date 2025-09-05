---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/compound-queries.html
---

# Compound queries [compound-queries]

Compound queries wrap other compound or leaf queries, either to combine their results and scores, to change their behaviour, or to switch from query to filter context.

The queries in this group are:

[`bool` query](/reference/query-languages/query-dsl/query-dsl-bool-query.md)
:   The default query for combining multiple leaf or compound query clauses, as `must`, `should`, `must_not`, or `filter` clauses. The `must` and `should` clauses have their scores combined — the more matching clauses, the better — while the `must_not` and `filter` clauses are executed in filter context.

[`boosting` query](/reference/query-languages/query-dsl/query-dsl-boosting-query.md)
:   Return documents which match a `positive` query, but reduce the score of documents which also match a `negative` query.

[`constant_score` query](/reference/query-languages/query-dsl/query-dsl-constant-score-query.md)
:   A query which wraps another query, but executes it in filter context. All matching documents are given the same constant `_score`.

[`dis_max` query](/reference/query-languages/query-dsl/query-dsl-dis-max-query.md)
:   A query which accepts multiple queries, and returns any documents which match any of the query clauses. While the `bool` query combines the scores from all matching queries, the `dis_max` query uses the score of the single best- matching query clause.

[`function_score` query](/reference/query-languages/query-dsl/query-dsl-function-score-query.md)
:   Modify the scores returned by the main query with functions to take into account factors like popularity, recency, distance, or custom algorithms implemented with scripting.






