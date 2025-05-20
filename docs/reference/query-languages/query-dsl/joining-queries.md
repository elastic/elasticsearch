---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/joining-queries.html
---

# Joining queries [joining-queries]

Performing full SQL-style joins in a distributed system like Elasticsearch is prohibitively expensive. Instead, Elasticsearch offers two forms of join which are designed to scale horizontally.

[`nested` query](/reference/query-languages/query-dsl/query-dsl-nested-query.md)
:   Documents may contain fields of type [`nested`](/reference/elasticsearch/mapping-reference/nested.md). These fields are used to index arrays of objects, where each object can be queried (with the `nested` query) as an independent document.

[`has_child`](/reference/query-languages/query-dsl/query-dsl-has-child-query.md) and [`has_parent`](/reference/query-languages/query-dsl/query-dsl-has-parent-query.md) queries
:   A [`join` field relationship](/reference/elasticsearch/mapping-reference/parent-join.md) can exist between documents within a single index. The `has_child` query returns parent documents whose child documents match the specified query, while the `has_parent` query returns child documents whose parent document matches the specified query.

Also see the [terms-lookup mechanism](/reference/query-languages/query-dsl/query-dsl-terms-query.md#query-dsl-terms-lookup) in the `terms` query, which allows you to build a `terms` query from values contained in another document.


## Notes [joining-queries-notes]


### Allow expensive queries [_allow_expensive_queries_2]

Joining queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.





