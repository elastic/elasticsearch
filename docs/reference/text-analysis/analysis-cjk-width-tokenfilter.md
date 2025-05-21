---
navigation_title: "CJK width"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-cjk-width-tokenfilter.html
---

# CJK width token filter [analysis-cjk-width-tokenfilter]


Normalizes width differences in CJK (Chinese, Japanese, and Korean) characters as follows:

* Folds full-width ASCII character variants into the equivalent basic Latin characters
* Folds half-width Katakana character variants into the equivalent Kana characters

This filter is included in {{es}}'s built-in [CJK language analyzer](/reference/text-analysis/analysis-lang-analyzer.md#cjk-analyzer). It uses Lucene’s [CJKWidthFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/cjk/CJKWidthFilter.md).

::::{note}
This token filter can be viewed as a subset of NFKC/NFKD Unicode normalization. See the [`analysis-icu` plugin](/reference/elasticsearch-plugins/analysis-icu-normalization-charfilter.md) for full normalization support.
::::


## Example [analysis-cjk-width-tokenfilter-analyze-ex]

```console
GET /_analyze
{
  "tokenizer" : "standard",
  "filter" : ["cjk_width"],
  "text" : "ｼｰｻｲﾄﾞﾗｲﾅｰ"
}
```

The filter produces the following token:

```text
シーサイドライナー
```


## Add to an analyzer [analysis-cjk-width-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the CJK width token filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /cjk_width_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_cjk_width": {
          "tokenizer": "standard",
          "filter": [ "cjk_width" ]
        }
      }
    }
  }
}
```


