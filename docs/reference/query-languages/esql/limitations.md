---
applies_to:
  stack:
  serverless:
navigation_title: "Limitations"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-limitations.html
---

# {{esql}} limitations [esql-limitations]

## Result set size limit [esql-max-rows]

By default, an {{esql}} query returns up to 1,000 rows. You can increase the number of rows up to 10,000 using the [`LIMIT`](/reference/query-languages/esql/commands/limit.md) command.

:::{include} _snippets/common/result-set-size-limitation.md
:::

## Field types [esql-supported-types]


### Supported types [_supported_types]

{{esql}} currently supports the following [field types](/reference/elasticsearch/mapping-reference/field-data-types.md):

* `alias`
* `boolean`
* `date`
* `date_nanos` (Tech Preview)

    * The following functions don’t yet support date nanos: `bucket`, `date_format`, `date_parse`, `date_diff`, `date_extract`
    * You can use `to_datetime` to cast to millisecond dates to use unsupported functions

* `double` (`float`, `half_float`, `scaled_float` are represented as `double`)
* `dense_vector` {applies_to}`stack: preview 9.2+` {applies_to}`serverless: preview`
* `ip`
* `keyword` [family](/reference/elasticsearch/mapping-reference/keyword.md) including `keyword`, `constant_keyword`, and `wildcard`
* `int` (`short` and `byte` are represented as `int`)
* `long`
* `null`
* `text` [family](/reference/elasticsearch/mapping-reference/text.md) including `text`, `semantic_text` and `match_only_text`
* `unsigned_long` {applies_to}`stack: preview` {applies_to}`serverless: preview`
* `version`
* Spatial types

    * `geo_point`
    * `geo_shape`
    * `point`
    * `shape`
* TSDB metrics {applies_to}`stack: preview 9.2+` {applies_to}`serverless: preview`
   * `counter`
   * `gauge`
   * `aggregate_metric_double`
   * `exponential_histogram` {applies_to}`stack: preview 9.3+` {applies_to}`serverless: preview`
   * `tdigest` {applies_to}`stack: preview 9.3+` {applies_to}`serverless: preview`


### Unsupported types [_unsupported_types]

{{esql}} does not support certain field types. If the limitation only applies to specific product versions, it is indicated in the following list:

* {applies_to}`stack: ga 9.0-9.1` `dense_vector`
* {applies_to}`stack: ga 9.0-9.1` TSDB metrics
   * `counter`
   * `gauge`
   * `aggregate_metric_double`

* Date/time

    * `date_range`

* Other types

    * `binary`
    * `completion`
    * `double_range`
    * `flattened`
    * `float_range`
    * `histogram`
    * `integer_range`
    * `ip_range`
    * `long_range`
    * `nested`
    * `rank_feature`
    * `rank_features`
    * `search_as_you_type`


Querying a column with an unsupported type returns an error. If a column with an unsupported type is not explicitly used in a query, it is returned with `null` values, with the exception of nested fields. Nested fields are not returned at all.


### Limitations on supported types [_limitations_on_supported_types]

Some [field types](/reference/elasticsearch/mapping-reference/field-data-types.md) are not supported in all contexts:

* Spatial types are not supported in the [SORT](/reference/query-languages/esql/commands/sort.md) processing command. Specifying a column of one of these types as a sort parameter will result in an error:

    * `geo_point`
    * `geo_shape`
    * `cartesian_point`
    * `cartesian_shape`


- In addition, when [querying multiple indexes](/reference/query-languages/esql/esql-multi-index.md), it’s possible for the same field to be mapped to multiple types. These fields cannot be directly used in queries or returned in results, unless they’re [explicitly converted to a single type](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types).

#### Spatial precision [esql-limitations-spatial-precision]

The spatial types `geo_point`, `geo_shape`, `cartesian_point` and `cartesian_shape` are maintained at source precision in the original documents,
but indexed at reduced precision by Lucene, for performance reasons.
To ensure this optimization is available in the widest context, all [spatial functions](/reference/query-languages/esql/functions-operators/spatial-functions.md) will produce results
at this reduced precision, aligned with the underlying Lucene index grid.
For `geo_point` and `geo_shape`, this grid is smaller than 1 cm at the equator, which is still very high precision for most use cases.
If the exact, original precision is desired, return the original field in the ES|QL query, which will maintain the original values.
To prioritize performance over precision, simply drop that field.

For example:

```esql
FROM airports
| EVAL geohex = ST_GEOHEX(location, 1)
| KEEP location, geohex
```

This query will perform slowly, due to the need to retrieve the original `location` field from the source document.
However, the following example will perform much faster:

```esql
FROM airports
| EVAL geohex = ST_GEOHEX(location, 1)
| EVAL x = ST_X(location), y = ST_Y(location)
| KEEP x, y, geohex
```

This query will perform much faster, since the original field `location` is not retrieved, and the three spatial functions used will all return values aligned with the Lucene index grid.
Note that if you return both the original `location` and the extracted `x` and `y` you will see very slight differences in the extracted values due to the precision loss.

#### Partial support in 9.2.0

* {applies_to}`stack: preview 9.2.0` The following types are only partially supported on 9.2.0. This is fixed in 9.2.1:
  * `dense_vector`: The [`KNN` function](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-knn) and the [`TO_DENSE_VECTOR` function](/reference/query-languages/esql/functions-operators/type-conversion-functions.md#esql-to_dense_vector) will work and any field data will be retrieved as part of the results. However, the type will appear as `unsupported` when these functions are not used.
  * `aggregate_metric_double`: Using the [`TO_AGGREGATE_METRIC_DOUBLE` function](/reference/query-languages/esql/functions-operators/type-conversion-functions.md#esql-to_aggregate_metric_double) will work and any field data will be retrieved as part of the results. However, the type will appear as `unsupported` if this function is not used.

    :::{note}
    This means that a simple query like `FROM test` will not retrieve `dense_vector` or `aggregate_metric_double` data. However, using the appropriate functions will work:
    * `FROM test WHERE KNN("dense_vector_field", [0, 1, 2, ...])`
    * `FROM test | EVAL agm_data = TO_AGGREGATE_METRIC_DOUBLE(aggregate_metric_double_field)`
    :::

## _source availability [esql-_source-availability]

{{esql}} does not support configurations where the [_source field](/reference/elasticsearch/mapping-reference/mapping-source-field.md) is [disabled](/reference/elasticsearch/mapping-reference/mapping-source-field.md#disable-source-field).

## Full-text search [esql-limitations-full-text-search]

One limitation of [full-text search](/reference/query-languages/esql/functions-operators/search-functions.md) is that it is necessary to use the search function,
like [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match),
in a [`WHERE`](/reference/query-languages/esql/commands/where.md) command directly after the
[`FROM`](/reference/query-languages/esql/commands/from.md) source command, or close enough to it.
Otherwise, the query will fail with a validation error.

For example, this query is valid:

```esql
FROM books
| WHERE MATCH(author, "Faulkner") AND MATCH(author, "Tolkien")
```

But this query will fail due to the [STATS](/reference/query-languages/esql/commands/stats-by.md) command:

```esql
FROM books
| STATS AVG(price) BY author
| WHERE MATCH(author, "Faulkner")
```

Note that any queries on `text` fields that do not explicitly use the full-text functions,
[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match),
[`QSTR`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-qstr) or
[`KQL`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-kql),
will behave as if the fields are actually `keyword` fields: they are case-sensitive and need to match the full string.


## Using {{esql}} to query multiple indices [esql-multi-index-limitations]

As discussed in more detail in [Using {{esql}} to query multiple indices](/reference/query-languages/esql/esql-multi-index.md), {{esql}} can execute a single query across multiple indices, data streams, or aliases. However, there are some limitations to be aware of:

* All underlying indexes and shards must be active. Using admin commands or UI, it is possible to pause an index or shard, for example by disabling a frozen tier instance, but then any {{esql}} query that includes that index or shard will fail, even if the query uses [`WHERE`](/reference/query-languages/esql/commands/where.md) to filter out the results from the paused index. If you see an error of type `search_phase_execution_exception`, with the message `Search rejected due to missing shards`, you likely have an index or shard in `UNASSIGNED` state.
* The same field must have the same type across all indexes. If the same field is mapped to different types it is still possible to query the indexes, but the field must be [explicitly converted to a single type](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types).

## Time series data streams [esql-tsdb]

::::{applies-switch}
:::{applies-item} stack: preview 9.2+
Time series data streams (TSDS) are supported in technical preview.
:::
:::{applies-item} stack: ga 9.0-9.1
{{esql}} does not support querying time series data streams (TSDS).
:::
::::

## Date math limitations [esql-limitations-date-math]

Date math expressions work well when the leftmost expression is a datetime, for example:

```txt
now() + 1 year - 2hour + ...
```

But using parentheses or putting the datetime to the right is not always supported yet. For example, the following expressions fail:

```txt
1year + 2hour + now()
now() + (1year + 2hour)
```

Date math does not allow subtracting two datetimes, for example:

```txt
now() - 2023-10-26
```


## Enrich limitations [esql-limitations-enrich]

While all three enrich policy types are supported, there are some limitations to be aware of:

* The `geo_match` enrich policy type only supports the `intersects` spatial relation.
* It is required that the `match_field` in the `ENRICH` command is of the correct type. For example, if the enrich policy is of type `geo_match`, the `match_field` in the `ENRICH` command must be of type `geo_point` or `geo_shape`. Likewise, a `range` enrich policy requires a `match_field` of type `integer`, `long`, `date`, or `ip`, depending on the type of the range field in the original enrich index.
* However, this constraint is relaxed for `range` policies when the `match_field` is of type `KEYWORD`. In this case the field values will be parsed during query execution, row by row. If any value fails to parse, the output values for that row will be set to `null`, an appropriate warning will be produced and the query will continue to execute.


## Dissect limitations [esql-limitations-dissect]

The `DISSECT` command does not support reference keys.


## Grok limitations [esql-limitations-grok]

The `GROK` command does not support configuring [custom patterns](/reference/enrich-processor/grok-processor.md#custom-patterns), or [multiple patterns](/reference/enrich-processor/grok-processor.md#trace-match). The `GROK` command is not subject to [Grok watchdog settings](/reference/enrich-processor/grok-processor.md#grok-watchdog).


## Multivalue limitations [esql-limitations-mv]

{{esql}} [supports multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md),
but functions return `null` when applied to a multivalued field, unless documented otherwise.
Work around this limitation by converting the field to single value with one of the
[multivalue functions](/reference/query-languages/esql/functions-operators/mv-functions.md).


## Timezone support [esql-limitations-timezone]

{{esql}} only supports the UTC timezone.


## INLINE STATS limitations [esql-limitations-inlinestats]

[`CATEGORIZE`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-categorize) grouping function is not currently supported.

Also, [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md) cannot yet have an unbounded [`SORT`](/reference/query-languages/esql/commands/sort.md) before it. You must either move the SORT after it, or add a [`LIMIT`](/reference/query-languages/esql/commands/limit.md) before the [`SORT`](/reference/query-languages/esql/commands/sort.md).


## Kibana limitations [esql-limitations-kibana]

* The filter bar interface is not enabled when Discover is in {{esql}} mode. To filter data, use [variable controls](docs-content://explore-analyze/discover/try-esql.md#add-variable-control), filter buttons within the table and field list, or write a query that uses the [`WHERE`](/reference/query-languages/esql/commands/where.md) command instead.
* Discover shows no more than 10,000 rows. This limit only applies to the number of rows that are retrieved by the query and displayed in Discover. Queries and aggregations run on the full data set.
* Discover shows no more than 50 columns. If a query returns more than 50 columns, Discover only shows the first 50.
* CSV export from Discover shows no more than 10,000 rows. This limit only applies to the number of rows that are retrieved by the query and displayed in Discover. Queries and aggregations run on the full data set.
* Querying many indices at once without any filters can cause an error in kibana which looks like `[esql] > Unexpected error from Elasticsearch: The content length (536885793) is bigger than the maximum allowed string (536870888)`. The response from {{esql}} is too long. Use [`DROP`](/reference/query-languages/esql/commands/drop.md) or [`KEEP`](/reference/query-languages/esql/commands/keep.md) to limit the number of fields returned.

## Known issues [esql-known-issues]

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for {{esql}}.
