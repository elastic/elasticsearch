---
applies_to:
  stack: ga
  serverless: ga
navigation_title: Query multiple sources
---

# Query multiple indices or clusters with {{esql}}

{{esql}} allows you to query across multiple indices, clusters, and projects, and combine
data from multiple sources. Learn more in the following sections:

* [Query multiple indices](esql-multi-index.md)
* [Query across clusters](esql-cross-clusters.md)
* [Query across {{serverless-short}} projects](esql-cross-serverless-projects.md) {applies_to}`serverless: preview`
* [Combine result sets with {{esql}} subqueries in a `FROM` command](esql-subquery.md) {applies_to}`stack: preview 9.4, ga 9.5+` {applies_to}`serverless: ga`
* [Define virtual indices using ES|QL views](esql-views.md) {applies_to}`stack: preview 9.4.0` {applies_to}`serverless: preview`

::::{include} _snippets/common/query-performance-tip.md
::::
