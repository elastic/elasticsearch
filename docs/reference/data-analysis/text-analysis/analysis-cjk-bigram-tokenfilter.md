---
navigation_title: "CJK bigram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-cjk-bigram-tokenfilter.html
---

# CJK bigram token filter [analysis-cjk-bigram-tokenfilter]


Forms [bigrams](https://en.wikipedia.org/wiki/Bigram) out of CJK (Chinese, Japanese, and Korean) tokens.

This filter is included in {{es}}'s built-in [CJK language analyzer](/reference/data-analysis/text-analysis/analysis-lang-analyzer.md#cjk-analyzer). It uses Lucene’s [CJKBigramFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/cjk/CJKBigramFilter.md).

## Example [analysis-cjk-bigram-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request demonstrates how the CJK bigram token filter works.

```console
GET /_analyze
{
  "tokenizer" : "standard",
  "filter" : ["cjk_bigram"],
  "text" : "東京都は、日本の首都であり"
}
```

The filter produces the following tokens:

```text
[ 東京, 京都, 都は, 日本, 本の, の首, 首都, 都で, であ, あり ]
```

% [source,console-result]
% --------------------------------------------------
% {
%   "tokens" : [
%     {
%       "token" : "東京",
%       "start_offset" : 0,
%       "end_offset" : 2,
%       "type" : "<DOUBLE>",
%       "position" : 0
%     },
%     {
%       "token" : "京都",
%       "start_offset" : 1,
%       "end_offset" : 3,
%       "type" : "<DOUBLE>",
%       "position" : 1
%     },
%     {
%       "token" : "都は",
%       "start_offset" : 2,
%       "end_offset" : 4,
%       "type" : "<DOUBLE>",
%       "position" : 2
%     },
%     {
%       "token" : "日本",
%       "start_offset" : 5,
%       "end_offset" : 7,
%       "type" : "<DOUBLE>",
%       "position" : 3
%     },
%     {
%       "token" : "本の",
%       "start_offset" : 6,
%       "end_offset" : 8,
%       "type" : "<DOUBLE>",
%       "position" : 4
%     },
%     {
%       "token" : "の首",
%       "start_offset" : 7,
%       "end_offset" : 9,
%       "type" : "<DOUBLE>",
%       "position" : 5
%     },
%     {
%       "token" : "首都",
%       "start_offset" : 8,
%       "end_offset" : 10,
%       "type" : "<DOUBLE>",
%       "position" : 6
%     },
%     {
%       "token" : "都で",
%       "start_offset" : 9,
%       "end_offset" : 11,
%       "type" : "<DOUBLE>",
%       "position" : 7
%     },
%     {
%       "token" : "であ",
%       "start_offset" : 10,
%       "end_offset" : 12,
%       "type" : "<DOUBLE>",
%       "position" : 8
%     },
%     {
%       "token" : "あり",
%       "start_offset" : 11,
%       "end_offset" : 13,
%       "type" : "<DOUBLE>",
%       "position" : 9
%     }
%   ]
% }
% --------------------------------------------------


## Add to an analyzer [analysis-cjk-bigram-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the CJK bigram token filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /cjk_bigram_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_cjk_bigram": {
          "tokenizer": "standard",
          "filter": [ "cjk_bigram" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-cjk-bigram-tokenfilter-configure-parms]

`ignored_scripts`
:   (Optional, array of character scripts) Array of character scripts for which to disable bigrams. Possible values:

* `han`
* `hangul`
* `hiragana`
* `katakana`

All non-CJK input is passed through unmodified.


`output_unigrams`
:   (Optional, Boolean) If `true`, emit tokens in both bigram and [unigram](https://en.wikipedia.org/wiki/N-gram) form. If `false`, a CJK character is output in unigram form when it has no adjacent characters. Defaults to `false`.


## Customize [analysis-cjk-bigram-tokenfilter-customize]

To customize the CJK bigram token filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

```console
PUT /cjk_bigram_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "han_bigrams": {
          "tokenizer": "standard",
          "filter": [ "han_bigrams_filter" ]
        }
      },
      "filter": {
        "han_bigrams_filter": {
          "type": "cjk_bigram",
          "ignored_scripts": [
            "hangul",
            "hiragana",
            "katakana"
          ],
          "output_unigrams": true
        }
      }
    }
  }
}
```


