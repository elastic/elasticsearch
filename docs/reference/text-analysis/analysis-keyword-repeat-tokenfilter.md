---
navigation_title: "Keyword repeat"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-keyword-repeat-tokenfilter.html
---

# Keyword repeat token filter [analysis-keyword-repeat-tokenfilter]


Outputs a keyword version of each token in a stream. These keyword tokens are not stemmed.

The `keyword_repeat` filter assigns keyword tokens a `keyword` attribute of `true`. Stemmer token filters, such as [`stemmer`](/reference/text-analysis/analysis-stemmer-tokenfilter.md) or [`porter_stem`](/reference/text-analysis/analysis-porterstem-tokenfilter.md), skip tokens with a `keyword` attribute of `true`.

You can use the `keyword_repeat` filter with a stemmer token filter to output a stemmed and unstemmed version of each token in a stream.

::::{important}
To work properly, the `keyword_repeat` filter must be listed before any stemmer token filters in the [analyzer configuration](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

Stemming does not affect all tokens. This means streams could contain duplicate tokens in the same position, even after stemming.

To remove these duplicate tokens, add the [`remove_duplicates`](/reference/text-analysis/analysis-remove-duplicates-tokenfilter.md) filter after the stemmer filter in the analyzer configuration.

::::


The `keyword_repeat` filter uses Lucene’s [KeywordRepeatFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/KeywordRepeatFilter.md).

## Example [analysis-keyword-repeat-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `keyword_repeat` filter to output a keyword and non-keyword version of each token in `fox running and jumping`.

To return the `keyword` attribute for these tokens, the analyze API request also includes the following arguments:

* `explain`:  `true`
* `attributes`: `keyword`

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    "keyword_repeat"
  ],
  "text": "fox running and jumping",
  "explain": true,
  "attributes": "keyword"
}
```

The API returns the following response. Note that one version of each token has a `keyword` attribute of `true`.

::::{dropdown} Response
```console-result
{
  "detail": {
    "custom_analyzer": true,
    "charfilters": [],
    "tokenizer": ...,
    "tokenfilters": [
      {
        "name": "keyword_repeat",
        "tokens": [
          {
            "token": "fox",
            "start_offset": 0,
            "end_offset": 3,
            "type": "word",
            "position": 0,
            "keyword": true
          },
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
            "keyword": true
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
            "keyword": true
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
          },
          {
            "token": "jumping",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": false
          }
        ]
      }
    ]
  }
}
```

::::


To stem the non-keyword tokens, add the `stemmer` filter after the `keyword_repeat` filter in the previous analyze API request.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    "keyword_repeat",
    "stemmer"
  ],
  "text": "fox running and jumping",
  "explain": true,
  "attributes": "keyword"
}
```

The API returns the following response. Note the following changes:

* The non-keyword version of `running` was stemmed to `run`.
* The non-keyword version of `jumping` was stemmed to `jump`.

::::{dropdown} Response
```console-result
{
  "detail": {
    "custom_analyzer": true,
    "charfilters": [],
    "tokenizer": ...,
    "tokenfilters": [
      {
        "name": "keyword_repeat",
        "tokens": ...
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
            "keyword": true
          },
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
            "keyword": true
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
            "keyword": true
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
          },
          {
            "token": "jump",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": false
          }
        ]
      }
    ]
  }
}
```

::::


However, the keyword and non-keyword versions of `fox` and `and` are identical and in the same respective positions.

To remove these duplicate tokens, add the `remove_duplicates` filter after `stemmer` in the analyze API request.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    "keyword_repeat",
    "stemmer",
    "remove_duplicates"
  ],
  "text": "fox running and jumping",
  "explain": true,
  "attributes": "keyword"
}
```

The API returns the following response. Note that the duplicate tokens for `fox` and `and` have been removed.

::::{dropdown} Response
```console-result
{
  "detail": {
    "custom_analyzer": true,
    "charfilters": [],
    "tokenizer": ...,
    "tokenfilters": [
      {
        "name": "keyword_repeat",
        "tokens": ...
      },
      {
        "name": "stemmer",
        "tokens": ...
      },
      {
        "name": "remove_duplicates",
        "tokens": [
          {
            "token": "fox",
            "start_offset": 0,
            "end_offset": 3,
            "type": "word",
            "position": 0,
            "keyword": true
          },
          {
            "token": "running",
            "start_offset": 4,
            "end_offset": 11,
            "type": "word",
            "position": 1,
            "keyword": true
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
            "keyword": true
          },
          {
            "token": "jumping",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": true
          },
          {
            "token": "jump",
            "start_offset": 16,
            "end_offset": 23,
            "type": "word",
            "position": 3,
            "keyword": false
          }
        ]
      }
    ]
  }
}
```

::::



## Add to an analyzer [analysis-keyword-repeat-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `keyword_repeat` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

This custom analyzer uses the `keyword_repeat` and `porter_stem` filters to create a stemmed and unstemmed version of each token in a stream. The `remove_duplicates` filter then removes any duplicate tokens from the stream.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_custom_analyzer": {
          "tokenizer": "standard",
          "filter": [
            "keyword_repeat",
            "porter_stem",
            "remove_duplicates"
          ]
        }
      }
    }
  }
}
```
