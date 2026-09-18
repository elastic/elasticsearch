---
applies_to:
  stack: ga
  serverless: ga
navigation_title: Query multiple sources
---

# Query multiple indices or clusters with {{esql}}

{{esql}} allows you to query across multiple indices, clusters, and projects. Learn more in the following sections:

* [Query multiple indices](esql-multi-index.md)
* [Query across clusters](esql-cross-clusters.md)
* [Query across {{serverless-short}} projects](esql-cross-serverless-projects.md) {applies_to}`serverless: preview`
* [Query data in external storage using ES|QL Data Federation](esql-data-federation.md) {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`

To combine or reuse query results using subqueries, views, or `FORK`, refer to [Combine and reuse ES|QL queries](esql-combine-reuse-queries.md).

::::{include} _snippets/common/query-performance-tip.md
::::
