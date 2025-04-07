---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-speech.html
---

# kuromoji_part_of_speech token filter [analysis-kuromoji-speech]

The `kuromoji_part_of_speech` token filter removes tokens that match a set of part-of-speech tags. It accepts the following setting:

`stoptags`
:   An array of part-of-speech tags that should be removed. It defaults to the `stoptags.txt` file embedded in the `lucene-analyzer-kuromoji.jar`.

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
              "my_posfilter"
            ]
          }
        },
        "filter": {
          "my_posfilter": {
            "type": "kuromoji_part_of_speech",
            "stoptags": [
              "助詞-格助詞-一般",
              "助詞-終助詞"
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
  "text": "寿司がおいしいね"
}
```

Which responds with:

```console-result
{
  "tokens" : [ {
    "token" : "寿司",
    "start_offset" : 0,
    "end_offset" : 2,
    "type" : "word",
    "position" : 0
  }, {
    "token" : "おいしい",
    "start_offset" : 3,
    "end_offset" : 7,
    "type" : "word",
    "position" : 2
  } ]
}
```

