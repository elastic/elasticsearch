---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-katakana-uppercase.html
---

# katakana_uppercase token filter [analysis-kuromoji-katakana-uppercase]

The `katakana_uppercase` token filter normalizes small letters (捨て仮名) in katakana into standard letters. This filter is useful if you want to search against old style Japanese text such as patents, legal documents, contract policies, etc.

For example:

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_analyzer": {
            "tokenizer": "kuromoji_tokenizer",
            "filter": [
              "katakana_uppercase"
            ]
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "ストップウォッチ"
}
```

Which results in:

```console-result
{
  "tokens": [
    {
      "token": "ストツプウオツチ",
      "start_offset": 0,
      "end_offset": 8,
      "type": "word",
      "position": 0
    }
  ]
}
```

