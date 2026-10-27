---
applies_to:
  stack: preview =9.5, ga 9.6+
  serverless: ga
navigation_title: "Runtime search"
description: How ES|QL runs MATCH and MATCH_PHRASE on expressions and other values that aren't backed by an inverted index.
---

# Search expressions with runtime search [esql-runtime-search]

[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md), the
[match operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) `:`, and
[`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md) normally run
as Lucene queries against the inverted index of the field they search. When the value being searched has no index
behind it, for example a column computed by [`EVAL`](/reference/query-languages/esql/commands/eval.md), they run as a
**runtime search** instead: {{esql}} analyzes and matches the values row by row as the query runs.

```esql
FROM books
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien")
```

A runtime search keeps the semantics of the same search on an indexed field: the values are analyzed, and options
such as `operator` and `fuzziness` behave the same way. However, the search is usually slower, since every row that
reaches it is analyzed instead of looked up in an index. And the relevance score is computed differently, since there
are no index statistics to compute BM25 from.

Runtime search is useful to:

* search [federated datasets](/reference/query-languages/esql/esql-data-federation-querying.md#use-search-functions), which have no index
* search a column that combines several fields, like `content` above
* search after a command that changes the rows, such as [`STATS`](/reference/query-languages/esql/commands/stats-by.md) or [`LIMIT`](/reference/query-languages/esql/commands/limit.md)
* search a field that is not mapped, or not mapped the same way, in every index the query reads

Other search functions, such as [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md)
and [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md), need an indexed field.

:::{dropdown} Runtime search in 9.5
:applies_to: stack: preview =9.5

* Only `MATCH` and the `:` operator support it, not `MATCH_PHRASE`.
* It is subject to the same [position restriction](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search-position)
  as a search on an indexed field.
* It does not support options, and always analyzes `text` with the `standard` analyzer.
* It does not contribute to `_score`.
:::

## When a search runs at runtime [esql-runtime-search-when]

{{esql}} decides whether a search runs at runtime from what it searches. There is no option to choose, and no
warning when it happens. A search runs at runtime when its target is:

* A column of a [federated dataset](/reference/query-languages/esql/esql-data-federation-querying.md#use-search-functions).
* An expression rather than a field, either a column created by a command such as `EVAL` or `STATS`, or a function
  call written directly as the target, like `MATCH(TO_TEXT(CONCAT(title, " ", description)), "Tolkien")`. This includes
  [`FIELD_EXTRACT`](/reference/query-languages/esql/esql-flattened-fields.md#esql-flattened-fields-fulltext) on a
  `flattened` field. A `STATS` grouping key is not an expression: it keeps the identity of the field it groups by, so
  searching it after `STATS` is still a search on an indexed field.
* A field converted to `text` or `keyword` when it is not already of that type, for example `TO_TEXT` on a `keyword`
  field. A conversion that does not change the type, like `TO_TEXT` on a `text` field, still uses the index.
* A column expanded by [`MV_EXPAND`](/reference/query-languages/esql/commands/mv_expand.md).
* A field that is [mapped to different types](/reference/query-languages/esql/esql-multi-index.md#esql-multi-index-union-types)
  across the indices the query reads.
* A field that is unmapped in some or all of the indices the query reads, and that
  [`SET unmapped_fields="load"`](/reference/query-languages/esql/esql-unmapped-fields.md) loads from `_source`.
* A column output by [`FORK`](/reference/query-languages/esql/commands/fork.md) when a branch contains a pipeline
  breaker such as `LIMIT` or `STATS`. When no branch does, the search is pushed into the branches and uses the index.

## Where a runtime search can appear [esql-runtime-search-position]

A runtime search can appear anywhere in the query, including after `STATS`, `LIMIT` and `FORK`. The
[position restriction](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search-position)
on searches of indexed fields does not apply to it:

```esql
FROM books
| SORT book_no
| LIMIT 10
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien")
```

The restriction is lifted for each search, not for the whole `WHERE` command. A search on an indexed field in the
same `WHERE` command is still rejected.

## Supported types [esql-runtime-search-types]

`MATCH` and the `:` operator can search expressions of these types:

* `text`: the values and the query are analyzed, as for an indexed `text` field.
* `keyword`: the query must equal a whole value exactly, as for an indexed `keyword` field.
* `boolean`, `date`, `date_nanos`, `double`, `integer`, `long`, `unsigned_long`, `ip` and `version`: the query must
  equal a value. The query must be convertible to the type of the expression.

`MATCH_PHRASE` can search `text` and `keyword` expressions. On a `keyword` expression the whole query must equal a
value exactly, as for an indexed `keyword` field.

:::{tip}
String functions such as [`CONCAT`](/reference/query-languages/esql/functions-operators/string-functions/concat.md)
return `keyword`, so searching their output requires an exact match of the whole value. To search it as full text,
convert it with [`TO_TEXT`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_text.md),
as in the examples on this page.
:::

## Multivalued columns [esql-runtime-search-multivalued]

For a [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) `text` column, query terms can match
across different values in the same row. Phrase matching uses a position gap between values, so a phrase does not
cross value boundaries with the default `slop`. A sufficiently large `slop` can allow a cross-value match.

## Analyzers [esql-runtime-search-analyzers]

A runtime search on a `text` expression uses two analyzers, like a search on an indexed `text` field:

* The **values analyzer** analyzes the values being searched. It plays the role of the `analyzer` in the mapping of an
  indexed field. Declare it with the `analyzer` option of
  [`TO_TEXT`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_text.md) where the
  column is created. It defaults to `standard`.
* The **query analyzer** analyzes the query string. Set it with the `analyzer` option of `MATCH` or `MATCH_PHRASE`.
  It defaults to the values analyzer.

For example, with the `english` analyzer declared on `content`, a search for `ring` also matches books whose title or
description contains `rings`, since both stem to `ring`:

```esql
FROM books
| EVAL content = TO_TEXT(CONCAT(title, " ", description), {"analyzer": "english"})
| WHERE MATCH(content, "ring")
```

Both options take the name of a built-in analyzer or one provided by a plugin. Custom analyzers defined in an
index's settings are not available, since the expression does not belong to an index.

`TO_TEXT` rejects the `analyzer` option on a field mapped in the index, since declaring it would force a search of
that field to run at runtime instead of using the index. Search the field directly to use the analyzer from its
mapping.

### Indexed `text` fields searched at runtime [esql-runtime-search-analyzers-indexed]

An indexed `text` field is sometimes searched at runtime, such as when `MV_EXPAND` expands it, or when it is not mapped the
same way across every index the query reads. Its values are then analyzed with the values analyzer of the column, not
the analyzer its mapping declares (`standard`, unless the `analyzer` option is passed to `TO_TEXT` to declare a different 
one). In contrast, if a search against a field can use its index, it uses the analyzer from its mapping.

:::{warning}
This substitution is silent. A field whose mapping declares an analyzer other than `standard` is searched differently
depending on where in the query it is searched, and the query succeeds while matching a different set of rows.

After `MV_EXPAND`, either search the field before the command, or declare the analyzer again with `TO_TEXT`
afterward, which is allowed once the column is no longer an indexed field:

```esql
FROM books
| MV_EXPAND author
| EVAL a = TO_TEXT(author, {"analyzer": "whitespace"})
| WHERE MATCH(a, "Tolkien")
```

A field that is mapped differently across indices has no such remedy: `TO_TEXT` rejects the `analyzer` option on
it, because it is still a field from an index mapping. Search it only where `standard` gives the results you need,
or map it the same way in every index.
:::

Which column `MV_EXPAND` expanded decides how a field is searched. Both of these queries expand `author`, which a book
can have several of, but only the first searches the expanded column, and so runs at runtime:

```esql
FROM books
| MV_EXPAND author
| WHERE MATCH(author, "Tolkien")
```

The second searches an indexed field instead, which is still subject to the
[position restriction](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search-position)
and is rejected after `MV_EXPAND`:

```esql
FROM books
| MV_EXPAND author
| WHERE MATCH(title, "Tolkien")
```

A column output by `FORK` is not index-backed either, but there a mapped `text` field is rejected rather than searched
with a substituted analyzer. Declare the analyzer on the merged column to search it, which is allowed because the
merged column is no longer an indexed field, or search the field inside the `FORK` branches instead:

```esql
FROM books
| FORK (SORT book_no | LIMIT 5)
       (SORT book_no DESC | LIMIT 5)
| EVAL t = TO_TEXT(title, {"analyzer": "whitespace"})
| WHERE MATCH(t, "Tolkien")
```

`FORK` branches that declare different values analyzers for the same column are rejected too, since the merged column
can only have one.

## Options [esql-runtime-search-options]

On `text` expressions, `MATCH` and `MATCH_PHRASE` support the same
[function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params) as on an indexed
field. On other types, `MATCH` supports only `lenient`, and `MATCH_PHRASE` supports none. When the query can't be
converted to the type of the expression, `lenient` makes the search match no rows instead of failing.

## Scoring [esql-runtime-search-scoring]

When the query requests [`METADATA _score`](/reference/query-languages/esql/esql-metadata-fields.md), a runtime search
contributes to the relevance score. There are no index statistics for an expression, so the score is not BM25:

* `MATCH` on a `text` expression scores the `boost` option (1.0 by default) for each query term the row matches. A
  term repeated in the query counts once for each repetition.
* `MATCH_PHRASE` on a `text` expression scores the `boost` option for a matching row.
* A search on any other type scores 1.0 for a matching row.

These scores are added to `_score` like any other search's, so in a query that also searches indexed fields they add
to BM25 scores, which are on a different scale.

A runtime search after `FORK` or a `FROM` with subqueries filters rows but does not contribute to `_score` unless
it can be pushed into the branches. Refer to [#161746](https://github.com/elastic/elasticsearch/issues/161746).

A runtime search contributes to `_score` only after it runs. Filtering on `_score` after the search sees its score:

```esql
FROM books METADATA _score
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien Rings")
| WHERE _score > 1.5
```

Within one `WHERE` command, a condition on `_score` combined with `AND` sees the score from every search in that
command, wherever it appears. Here `_score > 1.5` sees the score from both `MATCH` functions:

```esql
FROM books METADATA _score
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien Rings") AND _score > 1.5 AND MATCH(content, "English")
```

A filter, a sort, or a value computed from `_score` before the search runs sees the score from before it, which is
0.0 if no earlier search has scored. A plain copy of `_score`, such as `EVAL s = _score`, is the exception: as with a
search on an indexed field, it sees the search's score. For example, a `WHERE` on `_score` before the expression is
computed keeps no books here:

```esql
FROM books METADATA _score
| WHERE _score > 1.5
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien Rings")
```

A value computed from `_score` before or alongside the expression is also 0.0, so `s` is 0.0 here:

```esql
FROM books METADATA _score
| EVAL s = _score + 0.0, content = TO_TEXT(CONCAT(title, " ", description))
| WHERE MATCH(content, "Tolkien Rings")
```

And a `SORT` and `LIMIT` before the search pick 10 books before any of them have been scored:

```esql
FROM books METADATA _score
| EVAL content = TO_TEXT(CONCAT(title, " ", description))
| SORT _score DESC
| LIMIT 10
| WHERE MATCH(content, "Tolkien Rings")
```

## Performance [esql-runtime-search-performance]

A runtime search analyzes every row that reaches it, so the time it takes grows with the number of rows rather than the
number of matches. To keep it fast:

* Combine it with filters on indexed fields. Those run in the index first, so fewer rows reach the runtime search.
* Prefer faster options. Options such as `fuzziness` take longer to evaluate for each row.
* If searching a computed value is a frequent access pattern, index that value instead, for example with
  [`copy_to`](/reference/elasticsearch/mapping-reference/copy-to.md) or a
  [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md), and search the indexed field.

## Related pages [esql-runtime-search-related]

* [Search functions](/reference/query-languages/esql/functions-operators/search-functions.md): the full-text search functions in {{esql}}.
* [Full-text search limitations](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search): what runtime search and searches on indexed fields don't support.
* [Search and filter with {{esql}}](/reference/query-languages/esql/esql-search-tutorial.md): a hands-on tutorial, including searching computed values.
