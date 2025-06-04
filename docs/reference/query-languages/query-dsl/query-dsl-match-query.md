---
navigation_title: "Match"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
---

# Match query [query-dsl-match-query]


Returns documents that match a provided text, number, date or boolean value. The provided text is analyzed before matching.

The `match` query is the standard query for performing a full-text search, including options for fuzzy matching.

`Match` will also work against [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md) fields. 
As `semantic_text` does not support lexical text search,`match` queries against `semantic_text` fields will automatically perform the correct semantic search.
Because of this, options that specifically target lexical search such as `fuzziness` or `analyzer` will be ignored.

## Example request [match-query-ex-request]

```console
GET /_search
{
  "query": {
    "match": {
      "message": {
        "query": "this is a test"
      }
    }
  }
}
```


## Top-level parameters for `match` [match-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [match-field-params]

`query`
:   (Required) Text, number, boolean value or date you wish to find in the provided `<field>`.

The `match` query [analyzes](docs-content://manage-data/data-store/text-analysis.md) any provided text before performing a search. This means the `match` query can search [`text`](/reference/elasticsearch/mapping-reference/text.md) fields for analyzed tokens rather than an exact term.


`analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert the text in the `query` value into tokens. Defaults to the [index-time analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-analyzer) mapped for the `<field>`. If no analyzer is mapped, the index’s default analyzer is used.

`auto_generate_synonyms_phrase_query`
:   (Optional, Boolean) If `true`, [match phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) queries are automatically created for multi-term synonyms. Defaults to `true`.

See [Use synonyms with match query](#query-dsl-match-query-synonyms) for an example.


`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of the query. Defaults to `1.0`.

Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.


`fuzziness`
:   (Optional, string) Maximum edit distance allowed for matching. See [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness) for valid values and more information. See [Fuzziness in the match query](#query-dsl-match-query-fuzziness) for an example.

`max_expansions`
:   (Optional, integer) Maximum number of terms to which the query will expand. Defaults to `50`.

`prefix_length`
:   (Optional, integer) Number of beginning characters left unchanged for fuzzy matching. Defaults to `0`.

`fuzzy_transpositions`
:   (Optional, Boolean) If `true`, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to `true`.

`fuzzy_rewrite`
:   (Optional, string) Method used to rewrite the query. See the [`rewrite` parameter](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md) for valid values and more information.

If the `fuzziness` parameter is not `0`, the `match` query uses a `fuzzy_rewrite` method of `top_terms_blended_freqs_${max_expansions}` by default.


`lenient`
:   (Optional, Boolean) If `true`, format-based errors, such as providing a text `query` value for a [numeric](/reference/elasticsearch/mapping-reference/number.md) field, are ignored. Defaults to `false`.

`operator`
:   (Optional, string) Boolean logic used to interpret text in the `query` value. Valid values are:

`OR` (Default)
:   For example, a `query` value of `capital of Hungary` is interpreted as `capital OR of OR Hungary`.

`AND`
:   For example, a `query` value of `capital of Hungary` is interpreted as `capital AND of AND Hungary`.


`minimum_should_match`
:   (Optional, string) Minimum number of clauses that must match for a document to be returned. See the [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) for valid values and more information.


`zero_terms_query`
:   (Optional, string) Indicates whether no documents are returned if the `analyzer` removes all tokens, such as when using a `stop` filter. Valid values are:

`none` (Default)
:   No documents are returned if the `analyzer` removes all tokens.

`all`
:   Returns all documents, similar to a [`match_all`](/reference/query-languages/query-dsl/query-dsl-match-all-query.md) query.

See [Zero terms query](#query-dsl-match-query-zero) for an example.



## Notes [match-query-notes]

### Short request example [query-dsl-match-query-short-ex]

You can simplify the match query syntax by combining the `<field>` and `query` parameters. For example:

```console
GET /_search
{
  "query": {
    "match": {
      "message": "this is a test"
    }
  }
}
```


### How the match query works [query-dsl-match-query-boolean]

The `match` query is of type `boolean`. It means that the text provided is analyzed and the analysis process constructs a boolean query from the provided text. The `operator` parameter can be set to `or` or `and` to control the boolean clauses (defaults to `or`). The minimum number of optional `should` clauses to match can be set using the [`minimum_should_match`](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) parameter.

Here is an example with the `operator` parameter:

```console
GET /_search
{
  "query": {
    "match": {
      "message": {
        "query": "this is a test",
        "operator": "and"
      }
    }
  }
}
```

The `analyzer` can be set to control which analyzer will perform the analysis process on the text. It defaults to the field explicit mapping definition, or the default search analyzer.

The `lenient` parameter can be set to `true` to ignore exceptions caused by data-type mismatches,  such as trying to query a numeric field with a text query string. Defaults to `false`.


### Fuzziness in the match query [query-dsl-match-query-fuzziness]

`fuzziness` allows *fuzzy matching* based on the type of field being queried. See [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness) for allowed settings.

The `prefix_length` and `max_expansions` can be set in this case to control the fuzzy process. If the fuzzy option is set the query will use `top_terms_blended_freqs_${max_expansions}` as its [rewrite method](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md) the `fuzzy_rewrite` parameter allows to control how the query will get rewritten.

Fuzzy transpositions (`ab` → `ba`) are allowed by default but can be disabled by setting `fuzzy_transpositions` to `false`.

::::{note}
Fuzzy matching is not applied to terms with synonyms or in cases where the analysis process produces multiple tokens at the same position. Under the hood these terms are expanded to a special synonym query that blends term frequencies, which does not support fuzzy expansion.
::::


```console
GET /_search
{
  "query": {
    "match": {
      "message": {
        "query": "this is a testt",
        "fuzziness": "AUTO"
      }
    }
  }
}
```


### Zero terms query [query-dsl-match-query-zero]

If the analyzer used removes all tokens in a query like a `stop` filter does, the default behavior is to match no documents at all. In order to change that the `zero_terms_query` option can be used, which accepts `none` (default) and `all` which corresponds to a `match_all` query.

```console
GET /_search
{
  "query": {
    "match": {
      "message": {
        "query": "to be or not to be",
        "operator": "and",
        "zero_terms_query": "all"
      }
    }
  }
}
```


### Synonyms [query-dsl-match-query-synonyms]

The `match` query supports multi-terms synonym expansion with the [synonym_graph](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) token filter. When this filter is used, the parser creates a phrase query for each multi-terms synonyms. For example, the following synonym: `"ny, new york"` would produce:

`(ny OR ("new york"))`

It is also possible to match multi terms synonyms with conjunctions instead:

```console
GET /_search
{
   "query": {
       "match" : {
           "message": {
               "query" : "ny city",
               "auto_generate_synonyms_phrase_query" : false
           }
       }
   }
}
```

The example above creates a boolean query:

`(ny OR (new AND york)) city`

that matches documents with the term `ny` or the conjunction `new AND york`. By default the parameter `auto_generate_synonyms_phrase_query` is set to `true`.



