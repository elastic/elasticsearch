---
navigation_title: "Match phrase prefix"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html
---

# Match phrase prefix query [query-dsl-match-query-phrase-prefix]


Returns documents that contain the words of a provided text, in the **same order** as provided. The last term of the provided text is treated as a [prefix](/reference/query-languages/query-dsl/query-dsl-prefix-query.md), matching any words that begin with that term.

## Example request [match-phrase-prefix-query-ex-request]

The following search returns documents that contain phrases beginning with `quick brown f` in the `message` field.

This search would match a `message` value of `quick brown fox` or `two quick brown ferrets` but not `the fox is quick and brown`.

```console
GET /_search
{
  "query": {
    "match_phrase_prefix": {
      "message": {
        "query": "quick brown f"
      }
    }
  }
}
```


## Top-level parameters for `match_phrase_prefix` [match-phrase-prefix-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [match-phrase-prefix-field-params]

`query`
:   (Required, string) Text you wish to find in the provided `<field>`.

The `match_phrase_prefix` query [analyzes](docs-content://manage-data/data-store/text-analysis.md) any provided text into tokens before performing a search. The last term of this text is treated as a [prefix](/reference/query-languages/query-dsl/query-dsl-prefix-query.md), matching any words that begin with that term.


`analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert text in the `query` value into tokens. Defaults to the [index-time analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-analyzer) mapped for the `<field>`. If no analyzer is mapped, the index’s default analyzer is used.

`max_expansions`
:   (Optional, integer) Maximum number of terms to which the last provided term of the `query` value will expand. Defaults to `50`.

`slop`
:   (Optional, integer) Maximum number of positions allowed between matching tokens. Defaults to `0`. Transposed terms have a slop of `2`.

`zero_terms_query`
:   (Optional, string) Indicates whether no documents are returned if the `analyzer` removes all tokens, such as when using a `stop` filter. Valid values are:

`none` (Default)
:   No documents are returned if the `analyzer` removes all tokens.

`all`
:   Returns all documents, similar to a [`match_all`](/reference/query-languages/query-dsl/query-dsl-match-all-query.md) query.



## Notes [match-phrase-prefix-query-notes]

### Using the match phrase prefix query for search autocompletion [match-phrase-prefix-autocomplete]

While easy to set up, using the `match_phrase_prefix` query for search autocompletion can sometimes produce confusing results.

For example, consider the query string `quick brown f`. This query works by creating a phrase query out of `quick` and `brown` (i.e. the term `quick` must exist and must be followed by the term `brown`). Then it looks at the sorted term dictionary to find the first 50 terms that begin with `f`, and adds these terms to the phrase query.

The problem is that the first 50 terms may not include the term `fox` so the phrase `quick brown fox` will not be found. This usually isn’t a problem as the user will continue to type more letters until the word they are looking for appears.

For better solutions for *search-as-you-type* check out the [completion suggester](/reference/elasticsearch/rest-apis/search-suggesters.md#completion-suggester) and the [`search_as_you_type` field type](/reference/elasticsearch/mapping-reference/search-as-you-type.md).
