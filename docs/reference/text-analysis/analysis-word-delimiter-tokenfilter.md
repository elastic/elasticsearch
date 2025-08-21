---
navigation_title: "Word delimiter"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-word-delimiter-tokenfilter.html
---

# Word delimiter token filter [analysis-word-delimiter-tokenfilter]


::::{warning}
We recommend using the [`word_delimiter_graph`](/reference/text-analysis/analysis-word-delimiter-graph-tokenfilter.md) instead of the `word_delimiter` filter.

The `word_delimiter` filter can produce invalid token graphs. See [Differences between `word_delimiter_graph` and `word_delimiter`](/reference/text-analysis/analysis-word-delimiter-graph-tokenfilter.md#analysis-word-delimiter-graph-differences).

The `word_delimiter` filter also uses Lucene’s [WordDelimiterFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/WordDelimiterFilter.md), which is marked as deprecated.

::::


Splits tokens at non-alphanumeric characters. The `word_delimiter` filter also performs optional token normalization based on a set of rules. By default, the filter uses the following rules:

* Split tokens at non-alphanumeric characters. The filter uses these characters as delimiters. For example: `Super-Duper` → `Super`, `Duper`
* Remove leading or trailing delimiters from each token. For example: `XL---42+'Autocoder'` → `XL`, `42`, `Autocoder`
* Split tokens at letter case transitions. For example: `PowerShot` → `Power`, `Shot`
* Split tokens at letter-number transitions. For example: `XL500` → `XL`, `500`
* Remove the English possessive (`'s`) from the end of each token. For example: `Neil's` → `Neil`

::::{tip}
The `word_delimiter` filter was designed to remove punctuation from complex identifiers, such as product IDs or part numbers. For these use cases, we recommend using the `word_delimiter` filter with the [`keyword`](/reference/text-analysis/analysis-keyword-tokenizer.md) tokenizer.

Avoid using the `word_delimiter` filter to split hyphenated words, such as `wi-fi`. Because users often search for these words both with and without hyphens, we recommend using the [`synonym_graph`](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) filter instead.

::::


## Example [analysis-word-delimiter-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `word_delimiter` filter to split `Neil's-Super-Duper-XL500--42+AutoCoder` into normalized tokens using the filter’s default rules:

```console
GET /_analyze
{
  "tokenizer": "keyword",
  "filter": [ "word_delimiter" ],
  "text": "Neil's-Super-Duper-XL500--42+AutoCoder"
}
```

The filter produces the following tokens:

```txt
[ Neil, Super, Duper, XL, 500, 42, Auto, Coder ]
```


## Add to an analyzer [_add_to_an_analyzer]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `word_delimiter` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "filter": [ "word_delimiter" ]
        }
      }
    }
  }
}
```

::::{warning}
Avoid using the `word_delimiter` filter with tokenizers that remove punctuation, such as the [`standard`](/reference/text-analysis/analysis-standard-tokenizer.md) tokenizer. This could prevent the `word_delimiter` filter from splitting tokens correctly. It can also interfere with the filter’s configurable parameters, such as `catenate_all` or `preserve_original`. We recommend using the [`keyword`](/reference/text-analysis/analysis-keyword-tokenizer.md) or [`whitespace`](/reference/text-analysis/analysis-whitespace-tokenizer.md) tokenizer instead.

::::



## Configurable parameters [word-delimiter-tokenfilter-configure-parms]

`catenate_all`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of alphanumeric characters separated by non-alphabetic delimiters. For example: `super-duper-xl-500` → [ `super`, **`superduperxl500`**, `duper`, `xl`, `500` ]. Defaults to `false`.

::::{warning}
When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



`catenate_numbers`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of numeric characters separated by non-alphabetic delimiters. For example: `01-02-03` → [ `01`, **`010203`**, `02`, `03` ]. Defaults to `false`.

::::{warning}
When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



`catenate_words`
:   (Optional, Boolean) If `true`, the filter produces catenated tokens for chains of alphabetical characters separated by non-alphabetic delimiters. For example: `super-duper-xl` → [ `super`, **`superduperxl`**, `duper`, `xl` ]. Defaults to `false`.

::::{warning}
When used for search analysis, catenated tokens can cause problems for the [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query and other queries that rely on token position for matching. Avoid setting this parameter to `true` if you plan to use these queries.

::::



`generate_number_parts`
:   (Optional, Boolean) If `true`, the filter includes tokens consisting of only numeric characters in the output. If `false`, the filter excludes these tokens from the output. Defaults to `true`.

`generate_word_parts`
:   (Optional, Boolean) If `true`, the filter includes tokens consisting of only alphabetical characters in the output. If `false`, the filter excludes these tokens from the output. Defaults to `true`.

`preserve_original`
:   (Optional, Boolean) If `true`, the filter includes the original version of any split tokens in the output. This original version includes non-alphanumeric delimiters. For example: `super-duper-xl-500` → [ **`super-duper-xl-500`**, `super`, `duper`, `xl`, `500` ]. Defaults to `false`.

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



## Customize [analysis-word-delimiter-tokenfilter-customize]

To customize the `word_delimiter` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a `word_delimiter` filter that uses the following rules:

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
          "filter": [ "my_custom_word_delimiter_filter" ]
        }
      },
      "filter": {
        "my_custom_word_delimiter_filter": {
          "type": "word_delimiter",
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
