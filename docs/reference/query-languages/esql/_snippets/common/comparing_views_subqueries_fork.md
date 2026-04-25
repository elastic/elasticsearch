[Views](/reference/query-languages/esql/esql-views.md),
[subqueries](/reference/query-languages/esql/esql-subquery.md) and the
[`FORK`](/reference/query-languages/esql/commands/fork.md) command are related.
There are many similarities and differences between them.

### High level definitions

* `FORK` allows data coming from previous commands, like an initial `FROM index` command, to be processed in parallel in multiple different branches, each performing different commands on the same original data.
* Subqueries also enable parallel processing, but allow each branch to use a different source index with a different `FROM` command per branch.
Views are reusable, named queries that act like virtual indices. Each view has its own `FROM` command and processing pipeline, and can be referenced like a regular index.

### Similarities

* **Dynamic execution.** All three mechanisms will process the entire set of query definitions at query time, resulting in an up-to-date response when source indexes are changed and the query is re-run.
* **Union of columns.** Columns from the results of multiple branches are merged into the main query, expanding the table of results, and inserting `null` values if any branch has different columns than the others.
* **Supported commands.** Complex processing commands can be used inside both views and subqueries, as detailed in the [description of subqueries](/reference/query-languages/esql/esql-subquery.md#description).
* **No nested branching.** Nested branching is generally not supported, but views can work around this limitation through query compaction.
* **Maximum branch count.** All of these approaches to parallel processing are bound by the same maximum branch count of 8.

### FORK differences

The `FORK` command never includes a `FROM` command, and relies entirely on an existing query to provide the incoming columns.
This also means that all branches will receive identical incoming data, the same columns and the same rows.
This is not true of subqueries or views, which can receive completely different columns and rows from their own `FROM` commands.
Only one `FORK` command is allowed per query, so nested branches are not possible.
This limitation is partially true for views and subqueries, but to a lesser extent as described below.

### Differences between views and subqueries

Views have names, and these names are unique within the index namespace. This means a view cannot have the same name as an index, and vice versa.
Views can be nested within one another, as long as neither of the following two rules are broken:
* Cyclic references are not allowed. For example, if `viewA` references `viewB` and `viewB` references `viewC` it is not allowed to have `viewC` reference `viewA`.
    * Detection of cyclic references is done at main query execution time
* Multiple branching points do not exist

This last point highlights a difference between views and subqueries.
While subqueries simply disallow the use of further subqueries or `FORK` within a subquery, views will allow this under limited conditions.
