---
navigation_title: "Combined fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-combined-fields-query.html
---

# Combined fields [query-dsl-combined-fields-query]


The `combined_fields` query supports searching multiple text fields as if their contents had been indexed into one combined field. The query takes a term-centric view of the input string: first it analyzes the query string into individual terms, then looks for each term in any of the fields. This query is particularly useful when a match could span multiple text fields, for example the `title`, `abstract`, and `body` of an article:

```console
GET /_search
{
  "query": {
    "combined_fields" : {
      "query":      "database systems",
      "fields":     [ "title", "abstract", "body"],
      "operator":   "and"
    }
  }
}
```

The `combined_fields` query takes a principled approach to scoring based on the simple BM25F formula described in [The Probabilistic Relevance Framework: BM25 and Beyond](http://www.staff.city.ac.uk/~sb317/papers/foundations_bm25_review.pdf). When scoring matches, the query combines term and collection statistics across fields to score each match as if the specified fields had been indexed into a single, combined field. This scoring is a best attempt; `combined_fields` makes some approximations and scores will not obey the BM25F model perfectly.

::::{admonition} Field number limit
:class: warning

By default, there is a limit to the number of clauses a query can contain. This limit is defined by the [`indices.query.bool.max_clause_count`](/reference/elasticsearch/configuration-reference/search-settings.md#indices-query-bool-max-clause-count) setting, which defaults to `4096`. For combined fields queries, the number of clauses is calculated as the number of fields multiplied by the number of terms.

::::


## Per-field boosting [_per_field_boosting]

Field boosts are interpreted according to the combined field model. For example, if the `title` field has a boost of 2, the score is calculated as if each term in the title appeared twice in the synthetic combined field.

```console
GET /_search
{
  "query": {
    "combined_fields" : {
      "query" : "distributed consensus",
      "fields" : [ "title^2", "body" ] <1>
    }
  }
}
```

1. Individual fields can be boosted with the caret (`^`) notation.


::::{note}
The `combined_fields` query requires that field boosts are greater than or equal to 1.0. Field boosts are allowed to be fractional.
::::



## Top-level parameters for `combined_fields` [combined-field-top-level-params]

`fields`
:   (Required, array of strings) List of fields to search. Field wildcard patterns are allowed. Only [`text`](/reference/elasticsearch/mapping-reference/text.md) fields are supported, and they must all have the same search [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md).

`query`
:   (Required, string) Text to search for in the provided `<fields>`.

The `combined_fields` query [analyzes](docs-content://manage-data/data-store/text-analysis.md) the provided text before performing a search.


`auto_generate_synonyms_phrase_query`
:   (Optional, Boolean) If `true`, [match phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) queries are automatically created for multi-term synonyms. Defaults to `true`.

See [Use synonyms with match query](/reference/query-languages/query-dsl/query-dsl-match-query.md#query-dsl-match-query-synonyms) for an example.


`operator`
:   (Optional, string) Boolean logic used to interpret text in the `query` value. Valid values are:

`or` (Default)
:   For example, a `query` value of `database systems` is interpreted as `database OR systems`.

`and`
:   For example, a `query` value of `database systems` is interpreted as `database AND systems`.


`minimum_should_match`
:   (Optional, string) Minimum number of clauses that must match for a document to be returned. See the [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) for valid values and more information.


`zero_terms_query`
:   (Optional, string) Indicates whether no documents are returned if the `analyzer` removes all tokens, such as when using a `stop` filter. Valid values are:

`none` (Default)
:   No documents are returned if the `analyzer` removes all tokens.

`all`
:   Returns all documents, similar to a [`match_all`](/reference/query-languages/query-dsl/query-dsl-match-all-query.md) query.

See [Zero terms query](/reference/query-languages/query-dsl/query-dsl-match-query.md#query-dsl-match-query-zero) for an example.


### Comparison to `multi_match` query [_comparison_to_multi_match_query]

The `combined_fields` query provides a principled way of matching and scoring across multiple [`text`](/reference/elasticsearch/mapping-reference/text.md) fields. To support this, it requires that all fields have the same search [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md).

If you want a single query that handles fields of different types like keywords or numbers, then the [`multi_match`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) query may be a better fit. It supports both text and non-text fields, and accepts text fields that do not share the same analyzer.

The main `multi_match` modes `best_fields` and `most_fields` take a field-centric view of the query. In contrast, `combined_fields` is term-centric: `operator` and `minimum_should_match` are applied per-term, instead of per-field. Concretely, a query like

```console
GET /_search
{
  "query": {
    "combined_fields" : {
      "query":      "database systems",
      "fields":     [ "title", "abstract"],
      "operator":   "and"
    }
  }
}
```

is executed as:

```txt
+(combined("database", fields:["title" "abstract"]))
+(combined("systems", fields:["title", "abstract"]))
```

In other words, each term must be present in at least one field for a document to match.

The `cross_fields` `multi_match` mode also takes a term-centric approach and applies `operator` and `minimum_should_match per-term`. The main advantage of `combined_fields` over `cross_fields` is its robust and interpretable approach to scoring based on the BM25F algorithm.

::::{admonition} Custom similarities
:class: note

The `combined_fields` query currently only supports the BM25 similarity, which is the default unless a [custom similarity](/reference/elasticsearch/index-settings/similarity.md) is configured. [Per-field similarities](/reference/elasticsearch/mapping-reference/similarity.md) are also not allowed. Using `combined_fields` in either of these cases will result in an error.

::::




