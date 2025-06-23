---
navigation_title: "N-gram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-ngram-tokenfilter.html
---

# N-gram token filter [analysis-ngram-tokenfilter]


Forms [n-grams](https://en.wikipedia.org/wiki/N-gram) of specified lengths from a token.

For example, you can use the `ngram` token filter to change `fox` to `[ f, fo, o, ox, x ]`.

This filter uses Lucene’s [NGramTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/ngram/NGramTokenFilter.md).

::::{note}
The `ngram` filter is similar to the [`edge_ngram` token filter](/reference/text-analysis/analysis-edgengram-tokenfilter.md). However, the `edge_ngram` only outputs n-grams that start at the beginning of a token.

::::


## Example [analysis-ngram-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `ngram` filter to convert `Quick fox` to 1-character and 2-character n-grams:

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [ "ngram" ],
  "text": "Quick fox"
}
```

The filter produces the following tokens:

```text
[ Q, Qu, u, ui, i, ic, c, ck, k, f, fo, o, ox, x ]
```


## Add to an analyzer [analysis-ngram-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `ngram` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT ngram_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_ngram": {
          "tokenizer": "standard",
          "filter": [ "ngram" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-ngram-tokenfilter-configure-parms]

`max_gram`
:   (Optional, integer) Maximum length of characters in a gram. Defaults to `2`.

`min_gram`
:   (Optional, integer) Minimum length of characters in a gram. Defaults to `1`.

`preserve_original`
:   (Optional, Boolean) Emits original token when set to `true`. Defaults to `false`.

You can use the [`index.max_ngram_diff`](/reference/elasticsearch/index-settings/index-modules.md#index-max-ngram-diff) index-level setting to control the maximum allowed difference between the `max_gram` and `min_gram` values.


## Customize [analysis-ngram-tokenfilter-customize]

To customize the `ngram` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `ngram` filter that forms n-grams between 3-5 characters. The request also increases the `index.max_ngram_diff` setting to `2`.

```console
PUT ngram_custom_example
{
  "settings": {
    "index": {
      "max_ngram_diff": 2
    },
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "3_5_grams" ]
        }
      },
      "filter": {
        "3_5_grams": {
          "type": "ngram",
          "min_gram": 3,
          "max_gram": 5
        }
      }
    }
  }
}
```


