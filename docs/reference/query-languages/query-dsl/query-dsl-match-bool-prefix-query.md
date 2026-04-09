---
navigation_title: "Match boolean prefix"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-bool-prefix-query.html
---

# Match boolean prefix query [query-dsl-match-bool-prefix-query]


A `match_bool_prefix` query analyzes its input and constructs a [`bool` query](/reference/query-languages/query-dsl/query-dsl-bool-query.md) from the terms. Each term except the last is used in a `term` query. The last term is used in a `prefix` query. A `match_bool_prefix` query such as

```console
GET /_search
{
  "query": {
    "match_bool_prefix" : {
      "message" : "quick brown f"
    }
  }
}
```

where analysis produces the terms `quick`, `brown`, and `f` is similar to the following `bool` query

```console
GET /_search
{
  "query": {
    "bool" : {
      "should": [
        { "term": { "message": "quick" }},
        { "term": { "message": "brown" }},
        { "prefix": { "message": "f"}}
      ]
    }
  }
}
```

An important difference between the `match_bool_prefix` query and [`match_phrase_prefix`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase-prefix.md) is that the `match_phrase_prefix` query matches its terms as a phrase, but the `match_bool_prefix` query can match its terms in any position. The example `match_bool_prefix` query above could match a field containing `quick brown fox`, but it could also match `brown fox quick`. It could also match a field containing the term `quick`, the term `brown` and a term starting with `f`, appearing in any position.

## Parameters [_parameters]

By default, `match_bool_prefix` queries' input text will be analyzed using the analyzer from the queried field’s mapping. A different search analyzer can be configured with the `analyzer` parameter

```console
GET /_search
{
  "query": {
    "match_bool_prefix": {
      "message": {
        "query": "quick brown f",
        "analyzer": "keyword"
      }
    }
  }
}
```

`match_bool_prefix` queries support the [`minimum_should_match`](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) and `operator` parameters as described for the [`match` query](/reference/query-languages/query-dsl/query-dsl-match-query.md#query-dsl-match-query-boolean), applying the setting to the constructed `bool` query. The number of clauses in the constructed `bool` query will in most cases be the number of terms produced by analysis of the query text.

The [`fuzziness`](/reference/query-languages/query-dsl/query-dsl-match-query.md#query-dsl-match-query-fuzziness), `prefix_length`, `max_expansions`, `fuzzy_transpositions`, and `fuzzy_rewrite` parameters can be applied to the `term` subqueries constructed for all terms but the final term. They do not have any effect on the prefix query constructed for the final term.


