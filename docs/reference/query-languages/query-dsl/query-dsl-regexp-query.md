---
navigation_title: "Regexp"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
---

# Regexp query [query-dsl-regexp-query]


Returns documents that contain terms matching a [regular expression](https://en.wikipedia.org/wiki/Regular_expression).

A regular expression is a way to match patterns in data using placeholder characters, called operators. For a list of operators supported by the `regexp` query, see [Regular expression syntax](/reference/query-languages/query-dsl/regexp-syntax.md).

## Example request [regexp-query-ex-request]

The following search returns documents where the `user.id` field contains any term that begins with `k` and ends with `y`. The `.*` operators match any characters of any length, including no characters. Matching terms can include `ky`, `kay`, and `kimchy`.

```console
GET /_search
{
  "query": {
    "regexp": {
      "user.id": {
        "value": "k.*y",
        "flags": "ALL",
        "case_insensitive": true,
        "max_determinized_states": 10000,
        "rewrite": "constant_score_blended"
      }
    }
  }
}
```


## Top-level parameters for `regexp` [regexp-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [regexp-query-field-params]

`value`
:   (Required, string) Regular expression for terms you wish to find in the provided `<field>`. For a list of supported operators, see [Regular expression syntax](/reference/query-languages/query-dsl/regexp-syntax.md).

    By default, regular expressions are limited to 1,000 characters. You can change this limit using the [`index.max_regex_length`](/reference/elasticsearch/index-settings/index-modules.md#index-max-regex-length) setting.

    ::::{warning}
    The performance of the `regexp` query can vary based on the regular expression provided. To improve performance, avoid using wildcard patterns, such as `.*` or `.*?+`, without a prefix or suffix.

    ::::


`flags`
:   (Optional, string) Enables optional operators for the regular expression. For valid values and more information, see [Regular expression syntax](/reference/query-languages/query-dsl/regexp-syntax.md#regexp-optional-operators).

`case_insensitive`
:   :::{admonition} Added in 7.10.0
    This parameter was added in 7.10.0.
    :::

    (Optional, Boolean) Allows case insensitive matching of the regular expression value with the indexed field values when set to true. Default is false which means the case sensitivity of matching depends on the underlying field’s mapping.

`max_determinized_states`
:   (Optional, integer) Maximum number of [automaton states](https://en.wikipedia.org/wiki/Deterministic_finite_automaton) required for the query. Default is `10000`.

{{es}} uses [Apache Lucene](https://lucene.apache.org/core/) internally to parse regular expressions. Lucene converts each regular expression to a finite automaton containing a number of determinized states.

You can use this parameter to prevent that conversion from unintentionally consuming too many resources. You may need to increase this limit to run complex regular expressions.


`rewrite`
:   (Optional, string) Method used to rewrite the query. For valid values and more information, see the [`rewrite` parameter](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md).


## Notes [regexp-query-notes]

### Allow expensive queries [_allow_expensive_queries_6]

Regexp queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.



