---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-number.html
---

# kuromoji_number token filter [analysis-kuromoji-number]

The `kuromoji_number` token filter normalizes Japanese numbers (kansūji) to regular Arabic decimal numbers in half-width characters. For example:

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
              "kuromoji_number"
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
  "text": "一〇〇〇"
}
```

Which results in:

```console-result
{
  "tokens" : [ {
    "token" : "1000",
    "start_offset" : 0,
    "end_offset" : 4,
    "type" : "word",
    "position" : 0
  } ]
}
```

