---
navigation_title: "Simple query string"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html
---

# Simple query string query [query-dsl-simple-query-string-query]


Returns documents based on a provided query string, using a parser with a limited but fault-tolerant syntax.

This query uses a [simple syntax](#simple-query-string-syntax) to parse and split the provided query string into terms based on special operators. The query then [analyzes](docs-content://manage-data/data-store/text-analysis.md) each term independently before returning matching documents.

While its syntax is more limited than the [`query_string` query](/reference/query-languages/query-dsl/query-dsl-query-string-query.md), the `simple_query_string` query does not return errors for invalid syntax. Instead, it ignores any invalid parts of the query string.

## Example request [simple-query-string-query-ex-request]

```console
GET /_search
{
  "query": {
    "simple_query_string" : {
        "query": "\"fried eggs\" +(eggplant | potato) -frittata",
        "fields": ["title^5", "body"],
        "default_operator": "and"
    }
  }
}
```


## Top-level parameters for `simple_query_string` [simple-query-string-top-level-params]

`query`
:   (Required, string) Query string you wish to parse and use for search. See [Simple query string syntax](#simple-query-string-syntax).

`fields`
:   (Optional, array of strings) Array of fields you wish to search.

This field accepts wildcard expressions. You also can boost relevance scores for matches to particular fields using a caret (`^`) notation. See [Wildcards and per-field boosts in the `fields` parameter](#simple-query-string-boost) for examples.

Defaults to the `index.query.default_field` index setting, which has a default value of `*`. The `*` value extracts all fields that are eligible to term queries and filters the metadata fields. All extracted fields are then combined to build a query if no `prefix` is specified.

::::{warning}
There is a limit on the number of fields that can be queried at once. It is defined by the `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md), which defaults to `1024`.
::::



`default_operator`
:   (Optional, string) Default boolean logic used to interpret text in the query string if no operators are specified. Valid values are:

`OR` (Default)
:   For example, a query string of `capital of Hungary` is interpreted as `capital OR of OR Hungary`.

`AND`
:   For example, a query string of `capital of Hungary` is interpreted as `capital AND of AND Hungary`.


`analyze_wildcard`
:   (Optional, Boolean) If `true`, the query attempts to analyze wildcard terms in the query string. Defaults to `false`. Note that, in case of  `true`, only queries that end with a `*`
are fully analyzed. Queries that start with `*` or have it in the middle
are only [normalized](/reference/text-analysis/normalizers.md).

`analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert text in the query string into tokens. Defaults to the [index-time analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-analyzer) mapped for the `default_field`. If no analyzer is mapped, the index’s default analyzer is used.

`auto_generate_synonyms_phrase_query`
:   (Optional, Boolean) If `true`, the parser creates a [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query for each [multi-position token](docs-content://manage-data/data-store/text-analysis/token-graphs.md#token-graphs-multi-position-tokens). Defaults to `true`. For examples, see [Multi-position tokens](#simple-query-string-synonyms).

`flags`
:   (Optional, string) List of enabled operators for the [simple query string syntax](#simple-query-string-syntax). Defaults to `ALL` (all operators). See [Limit operators](#supported-flags) for valid values.

`fuzzy_max_expansions`
:   (Optional, integer) Maximum number of terms to which the query expands for fuzzy matching. Defaults to `50`.

`fuzzy_prefix_length`
:   (Optional, integer) Number of beginning characters left unchanged for fuzzy matching. Defaults to `0`.

`fuzzy_transpositions`
:   (Optional, Boolean) If `true`, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to `true`.

`lenient`
:   (Optional, Boolean) If `true`, format-based errors, such as providing a text value for a [numeric](/reference/elasticsearch/mapping-reference/number.md) field, are ignored. Defaults to `false`.

`minimum_should_match`
:   (Optional, string) Minimum number of clauses that must match for a document to be returned. See the [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) for valid values and more information.

`quote_field_suffix`
:   (Optional, string) Suffix appended to quoted text in the query string.

You can use this suffix to use a different analysis method for exact matches. See [Mixing exact search with stemming](docs-content://solutions/search/full-text/search-relevance/mixing-exact-search-with-stemming.md).



## Notes [simple-query-string-query-notes]

### Simple query string syntax [simple-query-string-syntax]

The `simple_query_string` query supports the following operators:

* `+` signifies AND operation
* `|` signifies OR operation
* `-` negates a single token
* `"` wraps a number of tokens to signify a phrase for searching
* `*` at the end of a term signifies a prefix query
* `(` and `)` signify precedence
* `~N` after a word signifies edit distance (fuzziness)
* `~N` after a phrase signifies slop amount

To use one of these characters literally, escape it with a preceding backslash (`\`).

The behavior of these operators may differ depending on the `default_operator` value. For example:

```console
GET /_search
{
  "query": {
    "simple_query_string": {
      "fields": [ "content" ],
      "query": "foo bar -baz"
    }
  }
}
```

This search is intended to only return documents containing `foo` or `bar` that also do **not** contain `baz`. However because of a `default_operator` of `OR`, this search actually returns documents that contain `foo` or `bar` and any documents that don’t contain `baz`. To return documents as intended, change the query string to `foo bar +-baz`.


### Limit operators [supported-flags]

You can use the `flags` parameter to limit the supported operators for the simple query string syntax.

To explicitly enable only specific operators, use a `|` separator. For example, a `flags` value of `OR|AND|PREFIX` disables all operators except `OR`, `AND`, and `PREFIX`.

```console
GET /_search
{
  "query": {
    "simple_query_string": {
      "query": "foo | bar + baz*",
      "flags": "OR|AND|PREFIX"
    }
  }
}
```

#### Valid values [supported-flags-values]

The available flags are:

`ALL` (Default)
:   Enables all optional operators.

`AND`
:   Enables the `+` AND operator.

`ESCAPE`
:   Enables `\` as an escape character.

`FUZZY`
:   Enables the `~N` operator after a word, where `N` is an integer denoting the allowed edit distance for matching. See [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness).

`NEAR`
:   Enables the `~N` operator, after a phrase where `N` is the maximum number of positions allowed between matching tokens. Synonymous to `SLOP`.

`NONE`
:   Disables all operators.

`NOT`
:   Enables the `-` NOT operator.

`OR`
:   Enables the `\|` OR operator.

`PHRASE`
:   Enables the `"` quotes operator used to search for phrases.

`PRECEDENCE`
:   Enables the `(` and `)` operators to control operator precedence.

`PREFIX`
:   Enables the `*` prefix operator.

`SLOP`
:   Enables the `~N` operator, after a phrase where `N` is maximum number of positions allowed between matching tokens. Synonymous to `NEAR`.

`WHITESPACE`
:   Enables whitespace as split characters.



### Wildcards and per-field boosts in the `fields` parameter [simple-query-string-boost]

Fields can be specified with wildcards, eg:

```console
GET /_search
{
  "query": {
    "simple_query_string" : {
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
    "simple_query_string" : {
      "query" : "this is a test",
      "fields" : [ "subject^3", "message" ] <1>
    }
  }
}
```

1. The `subject` field is three times as important as the `message` field.



### Multi-position tokens [simple-query-string-synonyms]

By default, the `simple_query_string` query parser creates a [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query for each [multi-position token](docs-content://manage-data/data-store/text-analysis/token-graphs.md#token-graphs-multi-position-tokens) in the query string. For example, the parser creates a `match_phrase` query for the multi-word synonym `ny, new york`:

`(ny OR ("new york"))`

To match multi-position tokens with an `AND` conjunction instead, set `auto_generate_synonyms_phrase_query` to `false`:

```console
GET /_search
{
  "query": {
    "simple_query_string": {
      "query": "ny city",
      "auto_generate_synonyms_phrase_query": false
    }
  }
}
```

For the above example, the parser creates the following [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) query:

`(ny OR (new AND york)) city)`

This `bool` query matches documents with the term `ny` or the conjunction `new AND york`.
