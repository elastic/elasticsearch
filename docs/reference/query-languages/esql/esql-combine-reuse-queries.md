---
navigation_title: "Combine and reuse queries"
applies_to:
  stack: preview 9.4.0
  serverless: preview
products:
  - id: elasticsearch
---

# Combine and reuse {{esql}} queries

{{esql}} provides several ways to combine, filter, and reuse query results beyond querying a single index. Choose the mechanism that best fits your goal.

| Mechanism | What it does | When to use |
|---|---|---|
| [`FROM` subquery](/reference/query-languages/esql/esql-subquery.md) | Runs independent queries and combines their rows into one table | You need to union results from different sources, each with its own processing |
| [`IN` subquery](/reference/query-languages/esql/esql-in-subquery.md) | Runs a subquery that returns exactly one column and uses those values to filter the outer query | You need to keep or exclude rows based on the results of another query |
| [ES\|QL view](/reference/query-languages/esql/esql-views.md) | Saves a query as a virtual index you can reference by name in any `FROM` | You want to reuse the same query across multiple requests without repeating it |
| [`FORK`](/reference/query-languages/esql/commands/fork.md) | Sends the same incoming rows through multiple independent branches | You want to run different processing on the same data in one query |

## Comparing views, subqueries, and FORK

:::{include} _snippets/common/comparing_views_subqueries_fork.md
:::

## Related alternatives

Depending on your goal, one of these alternatives may be a better fit:

* [`LOOKUP JOIN`](/reference/query-languages/esql/esql-lookup-join.md): enrich rows by joining against a lookup index on a key field.
* [`ENRICH`](/reference/query-languages/esql/esql-enrich-data.md): augment rows with data from an enrich policy.
* [Query multiple indices](/reference/query-languages/esql/esql-multi-index.md): use comma-separated index patterns or wildcards in a single `FROM` to query across indices without subqueries.
