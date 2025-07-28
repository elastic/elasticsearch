For instance,
```esql
FROM index | WHERE field = "value"
```
is equivalent to:
```esql
FROM index | WHERE field = "value" | LIMIT 1000
```

Queries do not return more than 10,000 rows, regardless of the `LIMIT` command’s value. This is a configurable upper limit.

To overcome this limitation:

* Reduce the result set size by modifying the query to only return relevant data. Use [`WHERE`](/reference/query-languages/esql/commands/processing-commands.md#esql-where) to select a smaller subset of the data.
* Shift any post-query processing to the query itself. You can use the {{esql}} [`STATS`](/reference/query-languages/esql/commands/processing-commands.md#esql-stats-by) command to aggregate data in the query.

The upper limit only applies to the number of rows that are output by the query, not to the number of documents it processes: the query runs on the full data set.

Consider the following two queries:
```esql
FROM index | WHERE field0 == "value" | LIMIT 20000
```
and
```esql
FROM index | STATS AVG(field1) BY field2 | LIMIT 20000
```

In both cases, the filtering by `field0` in the first query or the grouping by `field2` in the second is applied over all the documents present in the `index`, irrespective of their number or indexes size. However, both queries will return at most 10,000 rows, even if there were more rows available to return.

The default and maximum limits can be changed using these dynamic cluster settings:

* `esql.query.result_truncation_default_size`
* `esql.query.result_truncation_max_size`

However, doing so involves trade-offs. A larger result-set involves a higher memory pressure and increased processing times; the internode traffic within and across clusters can also increase.

These limitations are similar to those enforced by the [search API for pagination](/reference/elasticsearch/rest-apis/paginate-search-results.md).

| Functionality                    | Search                  | {{esql}}                                  |
|----------------------------------|-------------------------|-------------------------------------------|
| Results returned by default      | 10                      | 1.000                                     |
| Default upper limit              | 10,000                  | 10,000                                    |
| Specify number of results        | `size`                  | `LIMIT`                                   |
| Change default number of results | n/a                     | esql.query.result_truncation_default_size |
| Change default upper limit       | index-max-result-window | esql.query.result_truncation_max_size     |

