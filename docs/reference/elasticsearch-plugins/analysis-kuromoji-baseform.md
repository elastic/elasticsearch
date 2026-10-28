---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-baseform.html
---

# kuromoji_baseform token filter [analysis-kuromoji-baseform]

The `kuromoji_baseform` token filter replaces terms with their BaseFormAttribute. This acts as a lemmatizer for verbs and adjectives. Example:

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
              "kuromoji_baseform"
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
  "text": "飲み"
}
```

which responds with:

```console-result
{
  "tokens" : [ {
    "token" : "飲む",
    "start_offset" : 0,
    "end_offset" : 2,
    "type" : "word",
    "position" : 0
  } ]
}
```

