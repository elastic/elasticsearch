---
navigation_title: "Multi-match"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
---

# Multi-match query [query-dsl-multi-match-query]


The `multi_match` query builds on the [`match` query](/reference/query-languages/query-dsl/query-dsl-match-query.md) to allow multi-field queries:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":    "this is a test", <1>
      "fields": [ "subject", "message" ] <2>
    }
  }
}
```

1. The query string.
2. The fields to be queried.



### `fields` and per-field boosting [field-boost]

Fields can be specified with wildcards, eg:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":    "Will Smith",
      "fields": [ "title", "*_name" ] <1>
    }
  }
}
```

1. Query the `title`, `first_name` and `last_name` fields.


Individual fields can be boosted with the caret (`^`) notation:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query" : "this is a test",
      "fields" : [ "subject^3", "message" ] <1>
    }
  }
}
```

1. The query multiplies the `subject` field’s score by three but leaves the `message` field’s score unchanged.


If no `fields` are provided, the `multi_match` query defaults to the `index.query.default_field` index settings, which in turn defaults to `*`. `*` extracts all fields in the mapping that are eligible to term queries and filters the metadata fields. All extracted fields are then combined to build a query.

::::{admonition} Field number limit
:class: warning

By default, there is a limit to the number of clauses a query can contain. This limit is defined by the [`indices.query.bool.max_clause_count`](/reference/elasticsearch/configuration-reference/search-settings.md#indices-query-bool-max-clause-count) setting, which defaults to `4096`. For multi-match queries, the number of clauses is calculated as the number of fields multiplied by the number of terms.

::::



### Types of `multi_match` query: [multi-match-types]

The way the `multi_match` query is executed internally depends on the `type` parameter, which can be set to:

`best_fields`
:   (**default**) Finds documents which match any field, but uses the  `_score` from the best field. See [`best_fields`](#type-best-fields).

`most_fields`
:   Finds documents which match any field and combines the `_score` from each field. See [`most_fields`](#type-most-fields).

`cross_fields`
:   Treats fields with the same `analyzer` as though they were one big field. Looks for each word in **any** field. See [`cross_fields`](#type-cross-fields).

`phrase`
:   Runs a `match_phrase` query on each field and uses the `_score` from the best field. See [`phrase` and `phrase_prefix`](#type-phrase).

`phrase_prefix`
:   Runs a `match_phrase_prefix` query on each field and uses the `_score` from the best field. See [`phrase` and `phrase_prefix`](#type-phrase).

`bool_prefix`
:   Creates a `match_bool_prefix` query on each field and combines the `_score` from each field. See [`bool_prefix`](#type-bool-prefix).

## `best_fields` [type-best-fields]

The `best_fields` type is most useful when you are searching for multiple words best found in the same field. For instance brown fox in a single field is more meaningful than brown in one field and fox in the other.

The `best_fields` type generates a [`match` query](/reference/query-languages/query-dsl/query-dsl-match-query.md) for each field and wraps them in a [`dis_max`](/reference/query-languages/query-dsl/query-dsl-dis-max-query.md) query, to find the single best matching field. For instance, this query:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "brown fox",
      "type":       "best_fields",
      "fields":     [ "subject", "message" ],
      "tie_breaker": 0.3
    }
  }
}
```

would be executed as:

```console
GET /_search
{
  "query": {
    "dis_max": {
      "queries": [
        { "match": { "subject": "brown fox" }},
        { "match": { "message": "brown fox" }}
      ],
      "tie_breaker": 0.3
    }
  }
}
```

Normally the `best_fields` type uses the score of the **single** best matching field, but if `tie_breaker` is specified, then it calculates the score as follows:

* the score from the best matching field
* plus `tie_breaker * _score` for all other matching fields

Also, accepts `analyzer`, `boost`, `operator`, `minimum_should_match`, `fuzziness`, `lenient`, `prefix_length`, `max_expansions`, `fuzzy_rewrite`, `zero_terms_query`, `auto_generate_synonyms_phrase_query` and `fuzzy_transpositions`, as explained in [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md).

::::{admonition} operator and minimum_should_match
:class: important

:name: operator-min

The `best_fields` and `most_fields` types are *field-centric* — they generate a `match` query **per field**. This means that the `operator` and `minimum_should_match` parameters are applied to each field individually, which is probably not what you want.

Take this query for example:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "Will Smith",
      "type":       "best_fields",
      "fields":     [ "first_name", "last_name" ],
      "operator":   "and" <1>
    }
  }
}
```

1. All terms must be present.


This query is executed as:

```
  (+first_name:will +first_name:smith)
| (+last_name:will  +last_name:smith)
```
In other words, **all terms** must be present **in a single field** for a document to match.

The [`combined_fields`](/reference/query-languages/query-dsl/query-dsl-combined-fields-query.md) query offers a term-centric approach that handles `operator` and `minimum_should_match` on a per-term basis. The other multi-match mode [`cross_fields`](#type-cross-fields) also addresses this issue.

::::



## `most_fields` [type-most-fields]

The `most_fields` type is most useful when querying multiple fields that contain the same text analyzed in different ways. For instance, the main field may contain synonyms, stemming and terms without diacritics. A second field may contain the original terms, and a third field might contain shingles. By combining scores from all three fields we can match as many documents as possible with the main field, but use the second and third fields to push the most similar results to the top of the list.

This query:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "quick brown fox",
      "type":       "most_fields",
      "fields":     [ "title", "title.original", "title.shingles" ]
    }
  }
}
```

would be executed as:

```console
GET /_search
{
  "query": {
    "bool": {
      "should": [
        { "match": { "title":          "quick brown fox" }},
        { "match": { "title.original": "quick brown fox" }},
        { "match": { "title.shingles": "quick brown fox" }}
      ]
    }
  }
}
```

The score from each `match` clause is added together, just like a `bool` query.

Also, accepts `analyzer`, `boost`, `operator`, `minimum_should_match`, `fuzziness`, `lenient`, `prefix_length`, `max_expansions`, `fuzzy_rewrite`, and `zero_terms_query`.


## `phrase` and `phrase_prefix` [type-phrase]

The `phrase` and `phrase_prefix` types behave just like [`best_fields`](#type-best-fields), but they use a `match_phrase` or `match_phrase_prefix` query instead of a `match` query.

This query:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "quick brown f",
      "type":       "phrase_prefix",
      "fields":     [ "subject", "message" ]
    }
  }
}
```

would be executed as:

```console
GET /_search
{
  "query": {
    "dis_max": {
      "queries": [
        { "match_phrase_prefix": { "subject": "quick brown f" }},
        { "match_phrase_prefix": { "message": "quick brown f" }}
      ]
    }
  }
}
```

Also, accepts `analyzer`, `boost`, `lenient` and `zero_terms_query` as explained in [Match](/reference/query-languages/query-dsl/query-dsl-match-query.md), as well as `slop` which is explained in [Match phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md). Type `phrase_prefix` additionally accepts `max_expansions`.

::::{admonition} phrase, phrase_prefix and fuzziness
:class: important

:name: phrase-fuzziness

The `fuzziness` parameter cannot be used with the `phrase` or `phrase_prefix` type.

::::



## `cross_fields` [type-cross-fields]

The `cross_fields` type is particularly useful with structured documents where multiple fields **should** match. For instance, when querying the `first_name` and `last_name` fields for Will Smith, the best match is likely to have Will in one field and Smith in the other.

::::{admonition}
This sounds like a job for [`most_fields`](#type-most-fields) but there are two problems with that approach. The first problem is that `operator` and `minimum_should_match` are applied per-field, instead of per-term (see [explanation above](#operator-min)).

The second problem is to do with relevance: the different term frequencies in the `first_name` and `last_name` fields can produce unexpected results.

For instance, imagine we have two people: Will Smith and Smith Jones. Smith as a last name is very common (and so is of low importance) but Smith as a first name is very uncommon (and so is of great importance).

If we do a search for Will Smith, the Smith Jones document will probably appear above the better matching Will Smith because the score of `first_name:smith` has trumped the combined scores of `first_name:will` plus `last_name:smith`.

::::


One way of dealing with these types of queries is simply to index the `first_name` and `last_name` fields into a single `full_name` field. Of course, this can only be done at index time.

The `cross_field` type tries to solve these problems at query time by taking a *term-centric* approach. It first analyzes the query string into individual terms, then looks for each term in any of the fields, as though they were one big field.

A query like:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "Will Smith",
      "type":       "cross_fields",
      "fields":     [ "first_name", "last_name" ],
      "operator":   "and"
    }
  }
}
```

is executed as:

```
+(first_name:will last_name:will)
+(first_name:smith last_name:smith)
```
In other words, **all terms** must be present **in at least one field** for a document to match.  (Compare this to [the logic used for `best_fields` and `most_fields`](#operator-min).)

That solves one of the two problems. The problem of differing term frequencies is solved by *blending* the term frequencies for all fields in order to even out the differences.

In practice, `first_name:smith` will be treated as though it has the same frequencies as `last_name:smith`, plus one. This will make matches on `first_name` and `last_name` have comparable scores, with a tiny advantage for `last_name` since it is the most likely field that contains `smith`.

Note that `cross_fields` is usually only useful on short string fields that all have a `boost` of `1`. Otherwise boosts, term freqs and length normalization contribute to the score in such a way that the blending of term statistics is not meaningful anymore.

If you run the above query through the [Validate](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-validate-query), it returns this explanation:

```
+blended("will",  fields: [first_name, last_name])
+blended("smith", fields: [first_name, last_name])
```
Also, accepts `analyzer`, `boost`, `operator`, `minimum_should_match`, `lenient` and `zero_terms_query`.

::::{warning}
The `cross_fields` type blends field statistics in a complex way that can be hard to interpret. The score combination can even be incorrect, in particular when some documents contain some of the search fields, but not all of them. You should consider the [`combined_fields`](/reference/query-languages/query-dsl/query-dsl-combined-fields-query.md) query as an alternative, which is also term-centric but combines field statistics in a more robust way.
::::


### `cross_field` and analysis [cross-field-analysis]

The `cross_field` type can only work in term-centric mode on fields that have the same analyzer. Fields with the same analyzer are grouped together as in the example above. If there are multiple groups, the query will use the best score from any group.

For instance, if we have a `first` and `last` field which have the same analyzer, plus a `first.edge` and `last.edge` which both use an `edge_ngram` analyzer, this query:

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "Jon",
      "type":       "cross_fields",
      "fields":     [
        "first", "first.edge",
        "last",  "last.edge"
      ]
    }
  }
}
```

would be executed as:

```
    blended("jon", fields: [first, last])
| (
    blended("j",   fields: [first.edge, last.edge])
    blended("jo",  fields: [first.edge, last.edge])
    blended("jon", fields: [first.edge, last.edge])
)
```
In other words, `first` and `last` would be grouped together and treated as a single field, and `first.edge` and `last.edge` would be grouped together and treated as a single field.

Having multiple groups is fine, but when combined with `operator` or `minimum_should_match`, it can suffer from the [same problem](#operator-min) as `most_fields` or `best_fields`.

You can easily rewrite this query yourself as two separate `cross_fields` queries combined with a `dis_max` query, and apply the `minimum_should_match` parameter to just one of them:

```console
GET /_search
{
  "query": {
    "dis_max": {
      "queries": [
        {
          "multi_match" : {
            "query":      "Will Smith",
            "type":       "cross_fields",
            "fields":     [ "first", "last" ],
            "minimum_should_match": "50%" <1>
          }
        },
        {
          "multi_match" : {
            "query":      "Will Smith",
            "type":       "cross_fields",
            "fields":     [ "*.edge" ]
          }
        }
      ]
    }
  }
}
```

1. Either `will` or `smith` must be present in either of the `first` or `last` fields


You can force all fields into the same group by specifying the `analyzer` parameter in the query.

```console
GET /_search
{
  "query": {
   "multi_match" : {
      "query":      "Jon",
      "type":       "cross_fields",
      "analyzer":   "standard", <1>
      "fields":     [ "first", "last", "*.edge" ]
    }
  }
}
```

1. Use the `standard` analyzer for all fields.


which will be executed as:

```
blended("will",  fields: [first, first.edge, last.edge, last])
blended("smith", fields: [first, first.edge, last.edge, last])
```

### `tie_breaker` [tie-breaker]

By default, each per-term `blended` query will use the best score returned by any field in a group. Then when combining scores across groups, the query uses the best score from any group. The `tie_breaker` parameter can change the behavior for both of these steps:

`0.0`
:   Take the single best score out of (eg) `first_name:will` and `last_name:will` (default)

`1.0`
:   Add together the scores for (eg) `first_name:will` and `last_name:will`

`0.0 < n < 1.0`
:   Take the single best score plus `tie_breaker` multiplied by each of the scores from other matching fields/ groups

::::{admonition} cross_fields and fuzziness
:class: important

:name: crossfields-fuzziness

The `fuzziness` parameter cannot be used with the `cross_fields` type.

::::




## `bool_prefix` [type-bool-prefix]

The `bool_prefix` type’s scoring behaves like [`most_fields`](#type-most-fields), but using a [`match_bool_prefix` query](/reference/query-languages/query-dsl/query-dsl-match-bool-prefix-query.md) instead of a `match` query.

```console
GET /_search
{
  "query": {
    "multi_match" : {
      "query":      "quick brown f",
      "type":       "bool_prefix",
      "fields":     [ "subject", "message" ]
    }
  }
}
```

The `analyzer`, `boost`, `operator`, `minimum_should_match`, `lenient`, `zero_terms_query`, and `auto_generate_synonyms_phrase_query` parameters as explained in [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) are supported. The `fuzziness`, `prefix_length`, `max_expansions`, `fuzzy_rewrite`, and `fuzzy_transpositions` parameters are supported for the terms that are used to construct term queries, but do not have an effect on the prefix query constructed from the final term.

The `slop` parameter is not supported by this query type.


