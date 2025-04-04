## `LIMIT` [esql-limit]

The `LIMIT` processing command enables you to limit the number of rows that are returned.

**Syntax**

```esql
LIMIT max_number_of_rows
```

**Parameters**

`max_number_of_rows`
:   The maximum number of rows to return.

**Description**

The `LIMIT` processing command enables you to limit the number of rows that are returned. Queries do not return more than 10,000 rows, regardless of the `LIMIT` command’s value.

This limit only applies to the number of rows that are retrieved by the query. Queries and aggregations run on the full data set.

To overcome this limitation:

* Reduce the result set size by modifying the query to only return relevant data. Use [`WHERE`](#esql-where) to select a smaller subset of the data.
* Shift any post-query processing to the query itself. You can use the {{esql}} [`STATS`](#esql-stats-by) command to aggregate data in the query.

The default and maximum limits can be changed using these dynamic cluster settings:

* `esql.query.result_truncation_default_size`
* `esql.query.result_truncation_max_size`

**Example**

```esql
FROM employees
| SORT emp_no ASC
| LIMIT 5
```

