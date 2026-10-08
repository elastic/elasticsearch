[Views](/reference/query-languages/esql/esql-views.md),
[`FROM` subqueries](/reference/query-languages/esql/esql-from-subquery.md),
[`IN` subqueries](/reference/query-languages/esql/esql-in-subquery.md), and
[`FORK`](/reference/query-languages/esql/commands/fork.md) share several traits but differ in important ways.

`IN` subqueries operate differently from the other three. They filter rows rather than producing branches, so the similarities and differences below apply to `FROM` subqueries, views, and `FORK` only.

### Similarities

* **Dynamic execution.** All three run at query time, so results reflect the current state of the data.
* **Union of columns.** Columns from multiple branches are merged into a single table. Missing columns are filled with `null` values.
* **Supported commands.** Complex processing commands can be used inside both views and `FROM` subqueries, as detailed in the [description of `FROM` subqueries](/reference/query-languages/esql/esql-from-subquery.md#description).
* **No nested branching.** Nested branching is generally not supported, but views can work around this through [query compaction](/reference/query-languages/esql/esql-views.md#query-compaction).
* **Maximum branch count.** All three share the same maximum branch count of 8.

### How FORK differs

* `FORK` does not include a source command. Every branch receives the same incoming rows and columns.
* `FROM` subqueries and views each have their own source command (`FROM`, `TS`, or `ROW`), so branches can read from different sources with different columns.
* Only one `FORK` command is allowed per query, so nested branches are not possible. `FROM` subqueries and views have similar restrictions, but views can partially work around them through query compaction.

### How views differ from FROM subqueries

* Views must be defined using the [REST API](/reference/query-languages/esql/esql-views.md#create-and-manage-views) before they can be used in queries. Subqueries are written inline and require no setup.
* Views have names that are unique within the index namespace. A view cannot share a name with an index.
* Views can be nested (up to a depth of 10), with two restrictions:
  * Cyclic references are not allowed. For example, if `viewA` references `viewB` and `viewB` references `viewC`, then `viewC` cannot reference `viewA`. Cycles are detected at query time.
  * No more than one branching point can exist across the nesting chain.
* `FROM` subqueries do not support further `FROM` subqueries or `FORK` inside them, but can contain `IN` subqueries. Views allow nested branching under [limited conditions](/reference/query-languages/esql/esql-views.md#nesting-and-branching).
