---
navigation_title: "Edge n-gram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-edgengram-tokenfilter.html
---

# Edge n-gram token filter [analysis-edgengram-tokenfilter]


Forms an [n-gram](https://en.wikipedia.org/wiki/N-gram) of a specified length from the beginning of a token.

For example, you can use the `edge_ngram` token filter to change `quick` to `qu`.

When not customized, the filter creates 1-character edge n-grams by default.

This filter uses Lucene’s [EdgeNGramTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/ngram/EdgeNGramTokenFilter.md).

::::{note}
The `edge_ngram` filter is similar to the [`ngram` token filter](/reference/text-analysis/analysis-ngram-tokenizer.md). However, the `edge_ngram` only outputs n-grams that start at the beginning of a token. These edge n-grams are useful for [search-as-you-type](/reference/elasticsearch/mapping-reference/search-as-you-type.md) queries.

::::


## Example [analysis-edgengram-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `edge_ngram` filter to convert `the quick brown fox jumps` to 1-character and 2-character edge n-grams:

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [
    { "type": "edge_ngram",
      "min_gram": 1,
      "max_gram": 2
    }
  ],
  "text": "the quick brown fox jumps"
}
```

The filter produces the following tokens:

```text
[ t, th, q, qu, b, br, f, fo, j, ju ]
```


## Add to an analyzer [analysis-edgengram-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `edge_ngram` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT edge_ngram_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_edge_ngram": {
          "tokenizer": "standard",
          "filter": [ "edge_ngram" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-edgengram-tokenfilter-configure-parms]

`max_gram`
:   (Optional, integer) Maximum character length of a gram. For custom token filters, defaults to `2`. For the built-in `edge_ngram` filter, defaults to `1`.

See [Limitations of the `max_gram` parameter](#analysis-edgengram-tokenfilter-max-gram-limits).


`min_gram`
:   (Optional, integer) Minimum character length of a gram. Defaults to `1`.

`preserve_original`
:   (Optional, Boolean) Emits original token when set to `true`. Defaults to `false`.

`side`
:   :::{admonition} Deprecated in 8.16.0
    This setting was deprecated in 8.16.0.
    :::

    (Optional, string) Indicates whether to truncate tokens from the `front` or `back`. Defaults to `front`.

## Customize [analysis-edgengram-tokenfilter-customize]

To customize the `edge_ngram` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `edge_ngram` filter that forms n-grams between 3-5 characters.

```console
PUT edge_ngram_custom_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "3_5_edgegrams" ]
        }
      },
      "filter": {
        "3_5_edgegrams": {
          "type": "edge_ngram",
          "min_gram": 3,
          "max_gram": 5
        }
      }
    }
  }
}
```


## Limitations of the `max_gram` parameter [analysis-edgengram-tokenfilter-max-gram-limits]

The `edge_ngram` filter’s `max_gram` value limits the character length of tokens. When the `edge_ngram` filter is used with an index analyzer, this means search terms longer than the `max_gram` length may not match any indexed terms.

For example, if the `max_gram` is `3`, searches for `apple` won’t match the indexed term `app`.

To account for this, you can use the [`truncate`](/reference/text-analysis/analysis-truncate-tokenfilter.md) filter with a search analyzer to shorten search terms to the `max_gram` character length. However, this could return irrelevant results.

For example, if the `max_gram` is `3` and search terms are truncated to three characters, the search term `apple` is shortened to `app`. This means searches for `apple` return any indexed terms matching `app`, such as `apply`, `snapped`, and `apple`.

We recommend testing both approaches to see which best fits your use case and desired search experience.


