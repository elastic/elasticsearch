---
navigation_title: "Combine and reuse queries"
applies_to:
  stack:
  serverless:
products:
  - id: elasticsearch
---

# Combine and reuse {{esql}} queries

{{esql}} provides several ways to combine, filter, and reuse query results beyond querying a single index. Choose the mechanism that best fits your goal.

| Mechanism | What it does | When to use |
|---|---|---|
| [Subquery](/reference/query-languages/esql/esql-subquery.md) | Nests a query inside another query, either to [combine result sets](/reference/query-languages/esql/esql-from-subquery.md) or to [filter rows](/reference/query-languages/esql/esql-in-subquery.md) | You need to use the results of one query inside another |
| [View](/reference/query-languages/esql/esql-views.md) | Saves a named query as a virtual index that any `FROM` can reference | You want to define a query once and reuse it across multiple requests |
| [`FORK`](/reference/query-languages/esql/commands/fork.md) | Sends the same incoming rows through multiple independent branches | You want to run different processing on the same data in one query |

## Comparing views, subqueries, and FORK

:::{include} _snippets/common/comparing_views_subqueries_fork.md
:::

## Related alternatives

Depending on your goal, one of these alternatives may be a better fit:

* [`LOOKUP JOIN`](/reference/query-languages/esql/esql-lookup-join.md): enrich rows by joining against a lookup index on a key field.
* [`ENRICH`](/reference/query-languages/esql/esql-enrich-data.md): augment rows with data from an enrich policy.
* [Query multiple indices](/reference/query-languages/esql/esql-multi-index.md): use comma-separated index patterns or wildcards in a single `FROM` to query across indices without subqueries.
