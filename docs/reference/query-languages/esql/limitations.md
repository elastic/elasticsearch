---
navigation_title: "Limitations"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-limitations.html
---

# {{esql}} limitations [esql-limitations]

## Result set size limit [esql-max-rows]

By default, an {{esql}} query returns up to 1,000 rows. You can increase the number of rows up to 10,000 using the [`LIMIT`](/reference/query-languages/esql/commands/processing-commands.md#esql-limit) command.

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
* `ip`
* `keyword` [family](/reference/elasticsearch/mapping-reference/keyword.md) including `keyword`, `constant_keyword`, and `wildcard`
* `int` (`short` and `byte` are represented as `int`)
* `long`
* `null`
* `text` [family](/reference/elasticsearch/mapping-reference/text.md) including `text`, `semantic_text` and `match_only_text`
* [preview] `unsigned_long`
* `version`
* Spatial types

    * `geo_point`
    * `geo_shape`
    * `point`
    * `shape`



### Unsupported types [_unsupported_types]

{{esql}} does not yet support the following field types:

* Date/time

    * `date_range`

* Other types

    * `binary`
    * `completion`
    * `dense_vector`
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

* Spatial types are not supported in the [SORT](/reference/query-languages/esql/commands/processing-commands.md#esql-sort) processing command. Specifying a column of one of these types as a sort parameter will result in an error:

    * `geo_point`
    * `geo_shape`
    * `cartesian_point`
    * `cartesian_shape`


In addition, when [querying multiple indexes](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md), it’s possible for the same field to be mapped to multiple types. These fields cannot be directly used in queries or returned in results, unless they’re [explicitly converted to a single type](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md#esql-multi-index-union-types).


## _source availability [esql-_source-availability]

{{esql}} does not support configurations where the [_source field](/reference/elasticsearch/mapping-reference/mapping-source-field.md) is [disabled](/reference/elasticsearch/mapping-reference/mapping-source-field.md#disable-source-field).

[preview] {{esql}}'s support for [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) is currently experimental.


## Full-text search [esql-limitations-full-text-search]

[preview] {{esql}}'s support for [full-text search](/reference/query-languages/esql/functions-operators/search-functions.md) is currently in Technical Preview.
One limitation of full-text search is that it is necessary to use the search function,
like [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match),
in a [`WHERE`](/reference/query-languages/esql/commands/processing-commands.md#esql-where) command directly after the
[`FROM`](/reference/query-languages/esql/commands/source-commands.md#esql-from) source command, or close enough to it.
Otherwise, the query will fail with a validation error.

For example, this query is valid:

```esql
FROM books
| WHERE MATCH(author, "Faulkner") AND MATCH(author, "Tolkien")
```

But this query will fail due to the [STATS](/reference/query-languages/esql/commands/processing-commands.md#esql-stats-by) command:

```esql
FROM books
| STATS AVG(price) BY author
| WHERE MATCH(author, "Faulkner")
```

Note that, because of [the way {{esql}} treats `text` values](#esql-limitations-text-fields),
any queries on `text` fields that do not explicitly use the full-text functions,
[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match),
[`QSTR`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-qstr) or
[`KQL`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-kql),
will behave as if the fields are actually `keyword` fields: they are case-sensitive and need to match the full string.


## `text` fields behave like `keyword` fields [esql-limitations-text-fields]

While {{esql}} supports [`text`](/reference/elasticsearch/mapping-reference/text.md) fields, {{esql}} does not treat these fields like the Search API does. {{esql}} queries do not query or aggregate the [analyzed string](docs-content://manage-data/data-store/text-analysis.md). Instead, an {{esql}} query will try to get a `text` field’s subfield of the [keyword family type](/reference/elasticsearch/mapping-reference/keyword.md) and query/aggregate that. If it’s not possible to retrieve a `keyword` subfield, {{esql}} will get the string from a document’s `_source`. If the `_source` cannot be retrieved, for example when using synthetic source, `null` is returned.

Once a `text` field is retrieved, if the query touches it in any way, for example passing it into a function, the type will be converted to `keyword`. In fact, functions that operate on both `text` and `keyword` fields will perform as if the `text` field was a `keyword` field all along.

For example, the following query will return a column `greatest` of type `keyword` no matter whether any or all of `field1`, `field2`, and `field3` are of type `text`:

```esql
| FROM index
| EVAL greatest = GREATEST(field1, field2, field3)
```

Note that {{esql}}'s retrieval of `keyword` subfields may have unexpected consequences.
Other than when explicitly using the full-text functions,
[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) and
[`QSTR`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-qstr),
any {{esql}} query on a `text` field is case-sensitive.

For example, after indexing a field of type `text` with the value `Elasticsearch query language`, the following `WHERE` clause does not match because the `LIKE` operator is case-sensitive:

```esql
| WHERE field LIKE "elasticsearch query language"
```

The following `WHERE` clause does not match either, because the `LIKE` operator tries to match the whole string:

```esql
| WHERE field LIKE "Elasticsearch"
```

As a workaround, use wildcards and regular expressions. For example:

```esql
| WHERE field RLIKE "[Ee]lasticsearch.*"
```

Furthermore, a subfield may have been mapped with a [normalizer](/reference/elasticsearch/mapping-reference/normalizer.md), which can transform the original string. Or it may have been mapped with [`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md), which can truncate the string. None of these mapping operations are applied to an {{esql}} query, which may lead to false positives or negatives.

To avoid these issues, a best practice is to be explicit about the field that you query,
and query `keyword` sub-fields instead of `text` fields.
Or consider using one of the [full-text search](/reference/query-languages/esql/functions-operators/search-functions.md) functions.


## Using {{esql}} to query multiple indices [esql-multi-index-limitations]

As discussed in more detail in [Using {{esql}} to query multiple indices](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md), {{esql}} can execute a single query across multiple indices, data streams, or aliases. However, there are some limitations to be aware of:

* All underlying indexes and shards must be active. Using admin commands or UI, it is possible to pause an index or shard, for example by disabling a frozen tier instance, but then any {{esql}} query that includes that index or shard will fail, even if the query uses [`WHERE`](/reference/query-languages/esql/commands/processing-commands.md#esql-where) to filter out the results from the paused index. If you see an error of type `search_phase_execution_exception`, with the message `Search rejected due to missing shards`, you likely have an index or shard in `UNASSIGNED` state.
* The same field must have the same type across all indexes. If the same field is mapped to different types it is still possible to query the indexes, but the field must be [explicitly converted to a single type](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md#esql-multi-index-union-types).


## Time series data streams

{{esql}} supports time-series data streams (TSDS) with limitations:

* Counter fields such as `counter_long`, `counter_integer`, and `counter_double` are not supported except
  through explicit casting.
* aggregate_metric_double` types are supported in max, min, sum, avg, and count aggregations;
  however, this functionality is experimental and available in snapshot builds or requires enabling via its feature flag.

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


## Kibana limitations [esql-limitations-kibana]

* The user interface to filter data is not enabled when Discover is in {{esql}} mode. To filter data, write a query that uses the [`WHERE`](/reference/query-languages/esql/commands/processing-commands.md#esql-where) command instead.
* Discover shows no more than 10,000 rows. This limit only applies to the number of rows that are retrieved by the query and displayed in Discover. Queries and aggregations run on the full data set.
* Discover shows no more than 50 columns. If a query returns more than 50 columns, Discover only shows the first 50.
* CSV export from Discover shows no more than 10,000 rows. This limit only applies to the number of rows that are retrieved by the query and displayed in Discover. Queries and aggregations run on the full data set.
* Querying many indices at once without any filters can cause an error in kibana which looks like `[esql] > Unexpected error from Elasticsearch: The content length (536885793) is bigger than the maximum allowed string (536870888)`. The response from {{esql}} is too long. Use [`DROP`](/reference/query-languages/esql/commands/processing-commands.md#esql-drop) or [`KEEP`](/reference/query-languages/esql/commands/processing-commands.md#esql-keep) to limit the number of fields returned.

