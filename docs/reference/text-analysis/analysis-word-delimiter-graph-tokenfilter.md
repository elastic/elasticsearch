---
navigation_title: "Word delimiter graph"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-word-delimiter-graph-tokenfilter.html
---

# Word delimiter graph token filter [analysis-word-delimiter-graph-tokenfilter]


Splits tokens at non-alphanumeric characters. The `word_delimiter_graph` filter also performs optional token normalization based on a set of rules. By default, the filter uses the following rules:

* Split tokens at non-alphanumeric characters. The filter uses these characters as delimiters. For example: `Super-Duper` → `Super`, `Duper`
* Remove leading or trailing delimiters from each token. For example: `XL---42+'Autocoder'` → `XL`, `42`, `Autocoder`
* Split tokens at letter case transitions. For example: `PowerShot` → `Power`, `Shot`
* Split tokens at letter-number transitions. For example: `XL500` → `XL`, `500`
* Remove the English possessive (`'s`) from the end of each token. For example: `Neil's` → `Neil`

The `word_delimiter_graph` filter uses Lucene’s [WordDelimiterGraphFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/WordDelimiterGraphFilter.md).

::::{tip}
The `word_delimiter_graph` filter was designed to remove punctuation from complex identifiers, such as product IDs or part numbers. For these use cases, we recommend using the `word_delimiter_graph` filter with the [`keyword`](/reference/text-analysis/analysis-keyword-tokenizer.md) tokenizer.

Avoid using the `word_delimiter_graph` filter to split hyphenated words, such as `wi-fi`. Because users often search for these words both with and without hyphens, we recommend using the [`synonym_graph`](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) filter instead.

::::


## Example [analysis-word-delimiter-graph-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `word_delimiter_graph` filter to split `Neil's-Super-Duper-XL500--42+AutoCoder` into normalized tokens using the filter’s default rules:

```console
GET /_analyze
{
  "tokenizer": "keyword",
  "filter": [ "word_delimiter_graph" ],
  "text": "Neil's-Super-Duper-XL500--42+AutoCoder"
}
```

The filter produces the following tokens:

```txt
[ Neil, Super, Duper, XL, 500, 42, Auto, Coder ]
```


## Add to an analyzer [analysis-word-delimiter-graph-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `word_delimiter_graph` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "filter": [ "word_delimiter_graph" ]
        }
      }
    }
  }
}
```

::::{warning}
Avoid using the `word_delimiter_graph` filter with tokenizers that remove punctuation, such as the [`standard`](/reference/text-analysis/analysis-standard-tokenizer.md) tokenizer. This could prevent the `word_delimiter_graph` filter from splitting tokens correctly. It can also interfere with the filter’s configurable parameters, such as [`catenate_all`](#word-delimiter-graph-tokenfilter-catenate-all) or [`preserve_original`](#word-delimiter-graph-tokenfilter-preserve-original). We recommend using the [`keyword`](/reference/text-analysis/analysis-keyword-tokenizer.md) or [`whitespace`](/reference/text-analysis/analysis-whitespace-tokenizer.md) tokenizer instead.

::::



## Configurable parameters [word-delimiter-graph-tokenfilter-configure-parms]

$$$word-delimiter-graph-tokenfilter-adjust-offsets$$$

`adjust_offsets`
:   (Optional, Boolean) If `true`, the filter adjusts the offsets of split or catenated tokens to better reflect their actual position in the token stream. Defaults to `true`.

::::{warning}
Set `adjust_offsets` to `false` if your analyzer uses filters, such as the [`trim`](/reference/text-analysis/analysis-trim-tokenfilter.md) filter, that change the length of tokens without changing their offsets. Otherwise, the `word_delimiter_graph` filter could produce tokens with illegal offsets.

::::



$$$word-delimiter-graph-tokenfilter-catenate-all$$$

`catenate_all`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of alphanumeric characters separated by non-alphabetic delimiters. For example: `super-duper-xl-500` → [ **`superduperxl500`**, `super`, `duper`, `xl`, `500` ]. Defaults to `false`.

::::{warning}
Setting this parameter to `true` produces multi-position tokens, which are not supported by indexing.

If this parameter is `true`, avoid using this filter in an index analyzer or use the [`flatten_graph`](/reference/text-analysis/analysis-flatten-graph-tokenfilter.md) filter after this filter to make the token stream suitable for indexing.

When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



$$$word-delimiter-graph-tokenfilter-catenate-numbers$$$

`catenate_numbers`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of numeric characters separated by non-alphabetic delimiters. For example: `01-02-03` → [ **`010203`**, `01`, `02`, `03` ]. Defaults to `false`.

::::{warning}
Setting this parameter to `true` produces multi-position tokens, which are not supported by indexing.

If this parameter is `true`, avoid using this filter in an index analyzer or use the [`flatten_graph`](/reference/text-analysis/analysis-flatten-graph-tokenfilter.md) filter after this filter to make the token stream suitable for indexing.

When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



$$$word-delimiter-graph-tokenfilter-catenate-words$$$

`catenate_words`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of alphabetical characters separated by non-alphabetic delimiters. For example: `super-duper-xl` → [ **`superduperxl`**, `super`, `duper`, `xl` ]. Defaults to `false`.

::::{warning}
Setting this parameter to `true` produces multi-position tokens, which are not supported by indexing.

If this parameter is `true`, avoid using this filter in an index analyzer or use the [`flatten_graph`](/reference/text-analysis/analysis-flatten-graph-tokenfilter.md) filter after this filter to make the token stream suitable for indexing.

When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



`generate_number_parts`
:   (Optional, Boolean) If `true`, the filter includes tokens consisting of only numeric characters in the output. If `false`, the filter excludes these tokens from the output. Defaults to `true`.

`generate_word_parts`
:   (Optional, Boolean) If `true`, the filter includes tokens consisting of only alphabetical characters in the output. If `false`, the filter excludes these tokens from the output. Defaults to `true`.

`ignore_keywords`
:   (Optional, Boolean) If `true`, the filter skips tokens with a `keyword` attribute of `true`. Defaults to `false`.

$$$word-delimiter-graph-tokenfilter-preserve-original$$$

`preserve_original`
:   (Optional, Boolean) If `true`, the filter includes the original version of any split tokens in the output. This original version includes non-alphanumeric delimiters. For example: `super-duper-xl-500` → [ **`super-duper-xl-500`**, `super`, `duper`, `xl`, `500` ]. Defaults to `false`.

::::{warning}
Setting this parameter to `true` produces multi-position tokens, which are not supported by indexing.

If this parameter is `true`, avoid using this filter in an index analyzer or use the [`flatten_graph`](/reference/text-analysis/analysis-flatten-graph-tokenfilter.md) filter after this filter to make the token stream suitable for indexing.

::::



`protected_words`
:   (Optional, array of strings) Array of tokens the filter won’t split.

`protected_words_path`
:   (Optional, string) Path to a file that contains a list of tokens the filter won’t split.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each token in the file must be separated by a line break.


`split_on_case_change`
:   (Optional, Boolean) If `true`, the filter splits tokens at letter case transitions. For example: `camelCase` → [ `camel`, `Case` ]. Defaults to `true`.

`split_on_numerics`
:   (Optional, Boolean) If `true`, the filter splits tokens at letter-number transitions. For example: `j2se` → [ `j`, `2`, `se` ]. Defaults to `true`.

`stem_english_possessive`
:   (Optional, Boolean) If `true`, the filter removes the English possessive (`'s`) from the end of each token. For example: `O'Neil's` → [ `O`, `Neil` ]. Defaults to `true`.

`type_table`
:   (Optional, array of strings) Array of custom type mappings for characters. This allows you to map non-alphanumeric characters as numeric or alphanumeric to avoid splitting on those characters.

For example, the following array maps the plus (`+`) and hyphen (`-`) characters as alphanumeric, which means they won’t be treated as delimiters:

`[ "+ => ALPHA", "- => ALPHA" ]`

Supported types include:

* `ALPHA` (Alphabetical)
* `ALPHANUM` (Alphanumeric)
* `DIGIT` (Numeric)
* `LOWER` (Lowercase alphabetical)
* `SUBWORD_DELIM` (Non-alphanumeric delimiter)
* `UPPER` (Uppercase alphabetical)


`type_table_path`
:   (Optional, string) Path to a file that contains custom type mappings for characters. This allows you to map non-alphanumeric characters as numeric or alphanumeric to avoid splitting on those characters.

For example, the contents of this file may contain the following:

```txt
# Map the $, %, '.', and ',' characters to DIGIT
# This might be useful for financial data.
$ => DIGIT
% => DIGIT
. => DIGIT
\\u002C => DIGIT

# in some cases you might not want to split on ZWJ
# this also tests the case where we need a bigger byte[]
# see https://en.wikipedia.org/wiki/Zero-width_joiner
\\u200D => ALPHANUM
```

Supported types include:

* `ALPHA` (Alphabetical)
* `ALPHANUM` (Alphanumeric)
* `DIGIT` (Numeric)
* `LOWER` (Lowercase alphabetical)
* `SUBWORD_DELIM` (Non-alphanumeric delimiter)
* `UPPER` (Uppercase alphabetical)

This file path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each mapping in the file must be separated by a line break.



## Customize [analysis-word-delimiter-graph-tokenfilter-customize]

To customize the `word_delimiter_graph` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a `word_delimiter_graph` filter that uses the following rules:

* Split tokens at non-alphanumeric characters, *except* the hyphen (`-`) character.
* Remove leading or trailing delimiters from each token.
* Do *not* split tokens at letter case transitions.
* Do *not* split tokens at letter-number transitions.
* Remove the English possessive (`'s`) from the end of each token.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "filter": [ "my_custom_word_delimiter_graph_filter" ]
        }
      },
      "filter": {
        "my_custom_word_delimiter_graph_filter": {
          "type": "word_delimiter_graph",
          "type_table": [ "- => ALPHA" ],
          "split_on_case_change": false,
          "split_on_numerics": false,
          "stem_english_possessive": true
        }
      }
    }
  }
}
```


## Differences between `word_delimiter_graph` and `word_delimiter` [analysis-word-delimiter-graph-differences]

Both the `word_delimiter_graph` and [`word_delimiter`](/reference/text-analysis/analysis-word-delimiter-tokenfilter.md) filters produce tokens that span multiple positions when any of the following parameters are `true`:

* [`catenate_all`](#word-delimiter-graph-tokenfilter-catenate-all)
* [`catenate_numbers`](#word-delimiter-graph-tokenfilter-catenate-numbers)
* [`catenate_words`](#word-delimiter-graph-tokenfilter-catenate-words)
* [`preserve_original`](#word-delimiter-graph-tokenfilter-preserve-original)

However, only the `word_delimiter_graph` filter assigns multi-position tokens a `positionLength` attribute, which indicates the number of positions a token spans. This ensures the `word_delimiter_graph` filter always produces valid [token graphs](docs-content://manage-data/data-store/text-analysis/token-graphs.md).

The `word_delimiter` filter does not assign multi-position tokens a `positionLength` attribute. This means it produces invalid graphs for streams including these tokens.

While indexing does not support token graphs containing multi-position tokens, queries, such as the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query, can use these graphs to generate multiple sub-queries from a single query string.

To see how token graphs produced by the `word_delimiter` and `word_delimiter_graph` filters differ, check out the following example.

:::::{dropdown} Example
$$$analysis-word-delimiter-graph-basic-token-graph$$$
**Basic token graph**

Both the `word_delimiter` and `word_delimiter_graph` produce the following token graph for `PowerShot2000` when the following parameters are `false`:

* [`catenate_all`](#word-delimiter-graph-tokenfilter-catenate-all)
* [`catenate_numbers`](#word-delimiter-graph-tokenfilter-catenate-numbers)
* [`catenate_words`](#word-delimiter-graph-tokenfilter-catenate-words)
* [`preserve_original`](#word-delimiter-graph-tokenfilter-preserve-original)

This graph does not contain multi-position tokens. All tokens span only one position.

:::{image} images/token-graph-basic.svg
:alt: token graph basic
:::

$$$analysis-word-delimiter-graph-wdg-token-graph$$$
**`word_delimiter_graph` graph with a multi-position token**

The `word_delimiter_graph` filter produces the following token graph for `PowerShot2000` when `catenate_words` is `true`.

This graph correctly indicates the catenated `PowerShot` token spans two positions.

:::{image} images/token-graph-wdg.svg
:alt: token graph wdg
:::

$$$analysis-word-delimiter-graph-wd-token-graph$$$
**`word_delimiter` graph with a multi-position token**

When `catenate_words` is `true`, the `word_delimiter` filter produces the following token graph for `PowerShot2000`.

Note that the catenated `PowerShot` token should span two positions but only spans one in the token graph, making it invalid.

:::{image} images/token-graph-wd.svg
:alt: token graph wd
:::

:::::
