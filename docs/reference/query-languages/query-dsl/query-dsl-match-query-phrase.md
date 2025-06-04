---
navigation_title: "Match phrase"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html
---

# Match phrase query [query-dsl-match-query-phrase]


The `match_phrase` query analyzes the text and creates a `phrase` query out of the analyzed text. For example:

```console
GET /_search
{
  "query": {
    "match_phrase": {
      "message": "this is a test"
    }
  }
}
```

## Parameters for `<field>` [match-phrase-field-params]

`query`
:   (Required) Text, number, boolean value or date you wish to find in the provided `<field>`.


`analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert the text in the `query` value into tokens. Defaults to the [index-time analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-analyzer) mapped for the `<field>`. If no analyzer is mapped, the index’s default analyzer is used.

`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of the query. Defaults to `1.0`.
Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.

`slop`
:   (Optional, integer) Maximum number of positions allowed between matching tokens. Defaults to `0`. Transposed terms have a slop of `2`.

`zero_terms_query`
:   (Optional, string) Indicates whether no documents are returned if the `analyzer` removes all tokens, such as when using a `stop` filter. Valid values are:
  - `none` (Default)
  No documents are returned if the `analyzer` removes all tokens.
  - `all`
  Returns all documents, similar to a [`match_all`](/reference/query-languages/query-dsl/query-dsl-match-all-query.md) query.

A phrase query matches terms up to a configurable `slop` (which defaults to 0) in any order. Transposed terms have a slop of 2.

### Analyzer in the match phrase query [query-dsl-match-query-phrase-analyzer]

The `analyzer` can be set to control which analyzer will perform the analysis process on the text. It defaults to the field explicit mapping definition, or the default search analyzer, for example:

```console
GET /_search
{
  "query": {
    "match_phrase": {
      "message": {
        "query": "this is a test",
        "analyzer": "my_analyzer"
      }
    }
  }
}
```



