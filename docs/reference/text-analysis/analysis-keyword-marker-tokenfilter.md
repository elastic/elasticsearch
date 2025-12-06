---
navigation_title: "Keyword marker"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-keyword-marker-tokenfilter.html
---

# Keyword marker token filter [analysis-keyword-marker-tokenfilter]


Marks specified tokens as keywords, which are not stemmed.

The `keyword_marker` filter assigns specified tokens a `keyword` attribute of `true`. Stemmer token filters, such as [`stemmer`](/reference/text-analysis/analysis-stemmer-tokenfilter.md) or [`porter_stem`](/reference/text-analysis/analysis-porterstem-tokenfilter.md), skip tokens with a `keyword` attribute of `true`.

::::{important}
To work properly, the `keyword_marker` filter must be listed before any stemmer token filters in the [analyzer configuration](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

::::


The `keyword_marker` filter uses Lucene’s [KeywordMarkerFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/KeywordMarkerFilter.md).

## Example [analysis-keyword-marker-tokenfilter-analyze-ex]

To see how the `keyword_marker` filter works, you first need to produce a token stream containing stemmed tokens.

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the [`stemmer`](/reference/text-analysis/analysis-stemmer-tokenfilter.md) filter to create stemmed tokens for `fox running and jumping`.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [ "stemmer" ],
  "text": "fox running and jumping"
}
```

The request produces the following tokens. Note that `running` was stemmed to `run` and `jumping` was stemmed to `jump`.

```text
[ fox, run, and, jump ]
```

To prevent `jumping` from being stemmed, add the `keyword_marker` filter before the `stemmer` filter in the previous analyze API request. Specify `jumping` in the `keywords` parameter of the `keyword_marker` filter.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "keyword_marker",
      "keywords": [ "jumping" ]
    },
    "stemmer"
  ],
  "text": "fox running and jumping"
}
```

The request produces the following tokens. `running` is still stemmed to `run`, but `jumping` is not stemmed.

```text
[ fox, run, and, jumping ]
```

To see the `keyword` attribute for these tokens, add the following arguments to the analyze API request:

* `explain`: `true`
* `attributes`: `keyword`

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "keyword_marker",
      "keywords": [ "jumping" ]
    },
    "stemmer"
  ],
  "text": "fox running and jumping",
  "explain": true,
  "attributes": "keyword"
}
```

The API returns the following response. Note the `jumping` token has a `keyword` attribute of `true`.

```console-result
{
  "detail": {
    "custom_analyzer": true,
    "charfilters": [],
    "tokenizer": {
      "name": "whitespace",
      "tokens": [
        {
          "token": "fox",
          "start_offset": 0,
          "end_offset": 3,
          "type": "word",
          "position": 0
        },
        {
          "token": "running",
          "start_offset": 4,
          "end_offset": 11,
          "type": "word",
          "position": 1
        },
        {
          "token": "and",
          "start_offset": 12,
          "end_offset": 15,
          "type": "word",
          "position": 2
        },
        {
          "token": "jumping",
          "start_offset": 16,
          "end_offset": 23,
          "type": "word",
          "position": 3
        }
      ]
    },
    "tokenfilters": [
      {
        "name": "__anonymous__keyword_marker",
        "tokens": [
          {
            "token": "fox",
            "start_offset": 0,
            "end_offset": 3,
            "type": "word",
            "position": 0,
            "keyword": false
          },
          {
            "token": "running",
            "start_offset": 4,
            "end_offset": 11,
            "type": "word",
            "position": 1,
            "keyword": false
          },
          {
            "token": "and",
            "start_offset": 12,
            "end_offset": 15,
            "type": "word",
            "position": 2,
            "keyword": false
          },
          {
            "token": "jumping",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": true
          }
        ]
      },
      {
        "name": "stemmer",
        "tokens": [
          {
            "token": "fox",
            "start_offset": 0,
            "end_offset": 3,
            "type": "word",
            "position": 0,
            "keyword": false
          },
          {
            "token": "run",
            "start_offset": 4,
            "end_offset": 11,
            "type": "word",
            "position": 1,
            "keyword": false
          },
          {
            "token": "and",
            "start_offset": 12,
            "end_offset": 15,
            "type": "word",
            "position": 2,
            "keyword": false
          },
          {
            "token": "jumping",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": true
          }
        ]
      }
    ]
  }
}
```


## Configurable parameters [analysis-keyword-marker-tokenfilter-configure-parms]

`ignore_case`
:   (Optional, Boolean) If `true`, matching for the `keywords` and `keywords_path` parameters ignores letter case. Defaults to `false`.

`keywords`
:   (Required*, array of strings) Array of keywords. Tokens that match these keywords are not stemmed.

    This parameter, `keywords_path`, or `keywords_pattern` must be specified. You cannot specify this parameter and `keywords_pattern`.


`keywords_path`
:   (Required*, string) Path to a file that contains a list of keywords. Tokens that match these keywords are not stemmed.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each word in the file must be separated by a line break.

This parameter, `keywords`, or `keywords_pattern` must be specified. You cannot specify this parameter and `keywords_pattern`.


`keywords_pattern`
:   (Required*, string) [Java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md) used to match tokens. Tokens that match this expression are marked as keywords and not stemmed.

This parameter, `keywords`, or `keywords_path` must be specified. You cannot specify this parameter and `keywords` or `keywords_pattern`.

::::{warning}
Poorly written regular expressions can cause {{es}} to run slowly or result in stack overflow errors, causing the running node to suddenly exit.

::::




## Customize and add to an analyzer [analysis-keyword-marker-tokenfilter-customize]

To customize the `keyword_marker` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `keyword_marker` filter and the `porter_stem` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

The custom `keyword_marker` filter marks tokens specified in the `analysis/example_word_list.txt` file as keywords. The `porter_stem` filter does not stem these tokens.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_custom_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "my_custom_keyword_marker_filter",
            "porter_stem"
          ]
        }
      },
      "filter": {
        "my_custom_keyword_marker_filter": {
          "type": "keyword_marker",
          "keywords_path": "analysis/example_word_list.txt"
        }
      }
    }
  }
}
```


