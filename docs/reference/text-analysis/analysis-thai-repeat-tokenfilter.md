---
navigation_title: "Thai repeat"
---

# Thai repeat token filter [analysis-thai-repeat-tokenfilter]


Expands Thai Maiyamok (`ๆ`) reduplication into repeated tokens. Standalone or attached `ๆ` after a word is replaced with a copy of that word, so `"เร็วๆ"` and `"เร็ว ๆ"` both produce `[เร็ว, เร็ว]`.

This filter is included in {{es}}'s built-in [Thai language analyzer](/reference/text-analysis/analysis-lang-analyzer.md#thai-analyzer). It uses Lucene’s [ThaiRepeatFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/th/ThaiRepeatFilter.html).

The `thai_repeat` token filter is not configurable.

## Example [analysis-thai-repeat-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request demonstrates how the Thai repeat token filter works.

```console
GET /_analyze
{
  "tokenizer": "keyword",
  "filter": ["thai_repeat"],
  "text": "เร็วๆ"
}
```

The filter produces the following tokens:

```text
[ เร็ว, เร็ว ]
```


## Add to an analyzer [analysis-thai-repeat-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the Thai repeat token filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /thai_repeat_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "thai_repeat_analyzer": {
          "tokenizer": "thai",
          "filter": [ "thai_repeat" ]
        }
      }
    }
  }
}
```
