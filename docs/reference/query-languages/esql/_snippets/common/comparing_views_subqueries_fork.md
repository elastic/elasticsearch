[Views](/reference/query-languages/esql/esql-views.md),
[`FROM` subqueries](/reference/query-languages/esql/esql-from-subquery.md),
[`IN` subqueries](/reference/query-languages/esql/esql-in-subquery.md), and
[`FORK`](/reference/query-languages/esql/commands/fork.md) share several traits but differ in important ways.

`IN` subqueries operate differently from the other three. They filter rows rather than producing branches, so the similarities and differences below apply to `FROM` subqueries, views, and `FORK` only.

### Similarities

* **Dynamic execution.** All three run at query time, so results reflect the current state of the data.
* **Union of columns.** Columns from multiple branches are merged into a single table. Missing columns are filled with `null` values.
* **Supported commands.** Complex processing commands can be used inside both views and `FROM` subqueries, as detailed in the [description of `FROM` subqueries](/reference/query-languages/esql/esql-from-subquery.md#description).
* **Nested branching.** Support depends on the version.

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
Views, `FROM` subqueries, and `FORK` can be nested. The complete query
remains subject to the branch and command limits.
:::
:::{applies-item} stack: preview 9.4-9.5
Nested branching is generally not supported. Views can use
[query compaction](/reference/query-languages/esql/esql-views.md#query-compaction)
when the nested branches can be flattened into one branch point.
:::
::::

* **Branch limits.** The limits depend on the branching mechanism and version:

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
* **Maximum branch count.** A single `FORK` is capped at 8 branches by default
  (`esql.query.max_branch_count_per_merge`).
* View
  [query compaction](/reference/query-languages/esql/esql-views.md#query-compaction)
  flattens nested view unions only when the flat width is within
  `esql.query.max_branch_count` (20 by default). The same setting caps nested
  leaves across the whole query.
* **Maximum branch level.** Nested branch points created by views, `FROM`
  subqueries, and `FORK` are capped at 5 levels by default
  (`esql.query.max_branch_level`). This limit counts nested branch points, not
  view nesting depth.
:::
:::{applies-item} stack: preview 9.1-9.3, ga 9.4-9.5
* **Maximum branch count.** `FORK` is capped at 8 branches.
* {applies_to}`stack: preview 9.4-9.5` Views are capped at 8 branches.
:::
::::

### How FORK differs

* `FORK` does not include a source command. Every branch receives the same incoming rows and columns.
* `FROM` subqueries and views each have their own source command (`FROM`, `TS`, or `ROW`), so branches can read from different sources with different columns.

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
A query can use multiple `FORK` commands when views or multi-source `FROM`
commands create separate source branches, but each uninterrupted pipeline can
contain only one. Refer to the
[`FORK` limitations](/reference/query-languages/esql/commands/fork.md#limitations)
for supported patterns and examples.
:::
:::{applies-item} stack: preview 9.1-9.3, ga 9.4-9.5
Only one `FORK` command is allowed per query, so nested branches aren't possible.
`FROM` subqueries and views have similar restrictions, but views can sometimes
flatten nested branches through
[query compaction](/reference/query-languages/esql/esql-views.md#query-compaction).
:::
::::

### How views differ from FROM subqueries

* Views must be defined using the [REST API](/reference/query-languages/esql/esql-views.md#create-and-manage-views) before they can be used in queries. Subqueries are written inline and require no setup.
* Views have names that are unique within the index namespace. A view cannot share a name with an index.
* Views can be nested up to a depth of 10. Cyclic references are not allowed. For example, if `viewA` references `viewB` and `viewB` references `viewC`, then `viewC` cannot reference `viewA`. Cycles are detected at query time.

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
`FROM` subqueries and views can contain further `FROM` subqueries and `FORK`
commands, subject to the branching limits.
:::
:::{applies-item} stack: preview 9.4-9.5
No more than one branch point can exist across a view nesting chain. `FROM`
subqueries don't support further `FROM` subqueries or `FORK` inside them, but
they can contain `IN` subqueries. Views allow nested branching under
[limited conditions](/reference/query-languages/esql/esql-views.md#combining-branching-and-nesting).
:::
::::
