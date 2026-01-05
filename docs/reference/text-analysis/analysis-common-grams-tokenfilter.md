---
navigation_title: "Common grams"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-common-grams-tokenfilter.html
---

# Common grams token filter [analysis-common-grams-tokenfilter]


Generates [bigrams](https://en.wikipedia.org/wiki/Bigram) for a specified set of common words.

For example, you can specify `is` and `the` as common words. This filter then converts the tokens `[the, quick, fox, is, brown]` to `[the, the_quick, quick, fox, fox_is, is, is_brown, brown]`.

You can use the `common_grams` filter in place of the [stop token filter](/reference/text-analysis/analysis-stop-tokenfilter.md) when you don’t want to completely ignore common words.

This filter uses Lucene’s [CommonGramsFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/commongrams/CommonGramsFilter.md).

## Example [analysis-common-grams-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request creates bigrams for `is` and `the`:

```console
GET /_analyze
{
  "tokenizer" : "whitespace",
  "filter" : [
    {
      "type": "common_grams",
      "common_words": ["is", "the"]
    }
  ],
  "text" : "the quick fox is brown"
}
```

The filter produces the following tokens:

```text
[ the, the_quick, quick, fox, fox_is, is, is_brown, brown ]
```


## Add to an analyzer [analysis-common-grams-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `common_grams` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md):

```console
PUT /common_grams_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "index_grams": {
          "tokenizer": "whitespace",
          "filter": [ "common_grams" ]
        }
      },
      "filter": {
        "common_grams": {
          "type": "common_grams",
          "common_words": [ "a", "is", "the" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-common-grams-tokenfilter-configure-parms]

`common_words`
:   (Required*, array of strings) A list of tokens. The filter generates bigrams for these tokens.

Either this or the `common_words_path` parameter is required.


`common_words_path`
:   (Required*, string) Path to a file containing a list of tokens. The filter generates bigrams for these tokens.

This path must be absolute or relative to the `config` location. The file must be UTF-8 encoded. Each token in the file must be separated by a line break.

Either this or the `common_words` parameter is required.


`ignore_case`
:   (Optional, Boolean) If `true`, matches for common words matching are case-insensitive. Defaults to `false`.

`query_mode`
:   (Optional, Boolean) If `true`, the filter excludes the following tokens from the output:

* Unigrams for common words
* Unigrams for terms followed by common words

Defaults to `false`. We recommend enabling this parameter for [search analyzers](/reference/elasticsearch/mapping-reference/search-analyzer.md).

For example, you can enable this parameter and specify `is` and `the` as common words. This filter converts the tokens `[the, quick, fox, is, brown]` to `[the_quick, quick, fox_is, is_brown,]`.



## Customize [analysis-common-grams-tokenfilter-customize]

To customize the `common_grams` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `common_grams` filter with `ignore_case` and `query_mode` set to `true`:

```console
PUT /common_grams_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "index_grams": {
          "tokenizer": "whitespace",
          "filter": [ "common_grams_query" ]
        }
      },
      "filter": {
        "common_grams_query": {
          "type": "common_grams",
          "common_words": [ "a", "is", "the" ],
          "ignore_case": true,
          "query_mode": true
        }
      }
    }
  }
}
```


