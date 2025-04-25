---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-stop.html
---

# ja_stop token filter [analysis-kuromoji-stop]

The `ja_stop` token filter filters out Japanese stopwords (`_japanese_`), and any other custom stopwords specified by the user. This filter only supports the predefined `_japanese_` stopwords list. If you want to use a different predefined list, then use the [`stop` token filter](/reference/text-analysis/analysis-stop-tokenfilter.md) instead.

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "analyzer_with_ja_stop": {
            "tokenizer": "kuromoji_tokenizer",
            "filter": [
              "ja_stop"
            ]
          }
        },
        "filter": {
          "ja_stop": {
            "type": "ja_stop",
            "stopwords": [
              "_japanese_",
              "ストップ"
            ]
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "analyzer_with_ja_stop",
  "text": "ストップは消える"
}
```

The above request returns:

```console-result
{
  "tokens" : [ {
    "token" : "消える",
    "start_offset" : 5,
    "end_offset" : 8,
    "type" : "word",
    "position" : 2
  } ]
}
```

