---
navigation_title: "Wildcard"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
---

# Wildcard query [query-dsl-wildcard-query]


Returns documents that contain terms matching a wildcard pattern.

A wildcard operator is a placeholder that matches one or more characters. For example, the `*` wildcard operator matches zero or more characters. You can combine wildcard operators with other characters to create a wildcard pattern.

## Example request [wildcard-query-ex-request]

The following search returns documents where the `user.id` field contains a term that begins with `ki` and ends with `y`. These matching terms can include `kiy`, `kity`, or `kimchy`.

```console
GET /_search
{
  "query": {
    "wildcard": {
      "user.id": {
        "value": "ki*y",
        "boost": 1.0,
        "rewrite": "constant_score_blended"
      }
    }
  }
}
```


## Top-level parameters for `wildcard` [wildcard-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [wildcard-query-field-params]

`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of a query. Defaults to `1.0`.

    You can use the `boost` parameter to adjust relevance scores for searches containing two or more queries.

    Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.


`case_insensitive`
:   :::{admonition} Added in 7.10.0
    This parameter was added in 7.10.0.
    :::

    (Optional, Boolean) Allows case insensitive matching of the pattern with the indexed field values when set to true. Default is false which means the case sensitivity of matching depends on the underlying field’s mapping.

`rewrite`
:   (Optional, string) Method used to rewrite the query. For valid values and more information, see the [`rewrite` parameter](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md).

`value`
:   (Required, string) Wildcard pattern for terms you wish to find in the provided `<field>`.

    This parameter supports two wildcard operators:

    * `?`, which matches any single character
    * `*`, which can match zero or more characters, including an empty one

    ::::{warning}
    Avoid beginning patterns with `*` or `?`. This can increase the iterations needed to find matching terms and slow search performance.
    ::::


`wildcard`
:   (Required, string) An alias for the `value` parameter. If you specify both `value` and `wildcard`, the query uses the last one in the request body.


## Notes [wildcard-query-notes]

Wildcard queries using `*` can be resource-intensive, particularly with leading wildcards. To improve performance, minimize their use and consider alternatives like the [n-gram tokenizer](/reference/text-analysis/analysis-ngram-tokenizer.md). While this allows for more efficient searching, it may increase index size. For better performance and accuracy, combine wildcard queries with other query types like [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md) or [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to first narrow down results.

### Allow expensive queries [_allow_expensive_queries_7]

Wildcard queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.



