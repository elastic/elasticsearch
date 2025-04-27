---
navigation_title: "Search-as-you-type"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-as-you-type.html
---

# Search-as-you-type field type [search-as-you-type]


The `search_as_you_type` field type is a text-like field that is optimized to provide out-of-the-box support for queries that serve an as-you-type completion use case. It creates a series of subfields that are analyzed to index terms that can be efficiently matched by a query that partially matches the entire indexed text value. Both prefix completion (i.e matching terms starting at the beginning of the input) and infix completion (i.e. matching terms at any position within the input) are supported.

When adding a field of this type to a mapping

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "search_as_you_type"
      }
    }
  }
}
```

This creates the following fields

`my_field`
:   Analyzed as configured in the mapping. If an analyzer is not configured, the default analyzer for the index is used

`my_field._2gram`
:   Wraps the analyzer of `my_field` with a shingle token filter of shingle size 2

`my_field._3gram`
:   Wraps the analyzer of `my_field` with a shingle token filter of shingle size 3

`my_field._index_prefix`
:   Wraps the analyzer of `my_field._3gram` with an edge ngram token filter

The size of shingles in subfields can be configured with the `max_shingle_size` mapping parameter. The default is 3, and valid values for this parameter are integer values 2 - 4 inclusive. Shingle subfields will be created for each shingle size from 2 up to and including the `max_shingle_size`. The `my_field._index_prefix` subfield will always use the analyzer from the shingle subfield with the `max_shingle_size` when constructing its own analyzer.

Increasing the `max_shingle_size` will improve matches for queries with more consecutive terms, at the cost of larger index size. The default `max_shingle_size` should usually be sufficient.

The same input text is indexed into each of these fields automatically, with their differing analysis chains, when an indexed document has a value for the root field `my_field`.

```console
PUT my-index-000001/_doc/1?refresh
{
  "my_field": "quick brown fox jump lazy dog"
}
```

The most efficient way of querying to serve a search-as-you-type use case is usually a [`multi_match`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) query of type [`bool_prefix`](/reference/query-languages/query-dsl/query-dsl-match-bool-prefix-query.md) that targets the root `search_as_you_type` field and its shingle subfields. This can match the query terms in any order, but will score documents higher if they contain the terms in order in a shingle subfield.

```console
GET my-index-000001/_search
{
  "query": {
    "multi_match": {
      "query": "brown f",
      "type": "bool_prefix",
      "fields": [
        "my_field",
        "my_field._2gram",
        "my_field._3gram"
      ]
    }
  },
  "highlight": {
    "fields": {
      "my_field": {
        "matched_fields": ["my_field._index_prefix"] <1>
      }
    }
  }
}
```

1. Adding "my_field._index_prefix" to the `matched_fields` allows to highlight "my_field" also based on matches from "my_field._index_prefix" field.


```console-result
{
  "took" : 44,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 0.8630463,
    "hits" : [
      {
        "_index" : "my-index-000001",
        "_id" : "1",
        "_score" : 0.8630463,
        "_source" : {
          "my_field" : "quick brown fox jump lazy dog"
        },
        "highlight": {
          "my_field": [
            "quick <em>brown fox jump lazy</em> dog"
          ]
        }
      }
    ]
  }
}
```

To search for documents that strictly match the query terms in order, or to search using other properties of phrase queries, use a [`match_phrase_prefix` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase-prefix.md) on the root field. A [`match_phrase` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) can also be used if the last term should be matched exactly, and not as a prefix. Using phrase queries may be less efficient than using the `match_bool_prefix` query.

```console
GET my-index-000001/_search
{
  "query": {
    "match_phrase_prefix": {
      "my_field": "brown f"
    }
  }
}
```

## Parameters specific to the `search_as_you_type` field [specific-params]

The following parameters are accepted in a mapping for the `search_as_you_type` field and are specific to this field type

`max_shingle_size`
:   (Optional, integer) Largest shingle size to create. Valid values are `2` (inclusive) to `4` (inclusive). Defaults to `3`.

A subfield is created for each integer between `2` and this value. For example, a value of `3` creates two subfields: `my_field._2gram` and `my_field._3gram`

More subfields enables more specific queries but increases index size.



## Parameters of the field type as a text field [general-params]

The following parameters are accepted in a mapping for the `search_as_you_type` field due to its nature as a text-like field, and behave similarly to their behavior when configuring a field of the [`text`](/reference/elasticsearch/mapping-reference/text.md) data type. Unless otherwise noted, these options configure the root fields subfields in the same way.

[`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md)
:   The [analyzer](docs-content://manage-data/data-store/text-analysis.md) which should be used for `text` fields, both at index-time and at search-time (unless overridden by the [`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)). Defaults to the default index analyzer, or the [`standard` analyzer](/reference/text-analysis/analysis-standard-analyzer.md).

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be searchable? Accepts `true` (default) or `false`.

[`index_options`](/reference/elasticsearch/mapping-reference/index-options.md)
:   What information should be stored in the index, for search and highlighting purposes. Defaults to `positions`.

[`norms`](/reference/elasticsearch/mapping-reference/norms.md)
:   Whether field-length should be taken into account when scoring queries. Accepts `true` or `false`. This option configures the root field and shingle subfields, where its default is `true`. It does not configure the prefix subfield, where it is `false`.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default). This option only configures the root field, and does not configure any subfields.

[`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)
:   The [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) that should be used at search time on [`text`](/reference/elasticsearch/mapping-reference/text.md) fields. Defaults to the `analyzer` setting.

[`search_quote_analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md#search-quote-analyzer)
:   The [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) that should be used at search time when a phrase is encountered. Defaults to the `search_analyzer` setting.

[`similarity`](/reference/elasticsearch/mapping-reference/similarity.md)
:   Which scoring algorithm or *similarity* should be used. Defaults to `BM25`.

[`term_vector`](/reference/elasticsearch/mapping-reference/term-vector.md)
:   Whether term vectors should be stored for the field. Defaults to `no`. This option configures the root field and shingle subfields, but not the prefix subfield.


## Optimization of prefix queries [prefix-queries]

When making a [`prefix`](/reference/query-languages/query-dsl/query-dsl-prefix-query.md) query to the root field or any of its subfields, the query will be rewritten to a [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) query on the `._index_prefix` subfield. This matches more efficiently than is typical of `prefix` queries on text fields, as prefixes up to a certain length of each shingle are indexed directly as terms in the `._index_prefix` subfield.

The analyzer of the `._index_prefix` subfield slightly modifies the shingle-building behavior to also index prefixes of the terms at the end of the field’s value that normally would not be produced as shingles. For example, if the value `quick brown fox` is indexed into a `search_as_you_type` field with `max_shingle_size` of 3, prefixes for `brown fox` and `fox` are also indexed into the `._index_prefix` subfield even though they do not appear as terms in the `._3gram` subfield. This allows for completion of all the terms in the field’s input.

### Synthetic `_source` [search-as-you-type-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


`search_as_you_type` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.



