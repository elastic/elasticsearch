---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori-readingform.html
---

# nori_readingform token filter [analysis-nori-readingform]

The `nori_readingform` token filter rewrites tokens written in Hanja to their Hangul form.

```console
PUT nori_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_analyzer": {
            "tokenizer": "nori_tokenizer",
            "filter": [ "nori_readingform" ]
          }
        }
      }
    }
  }
}

GET nori_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "鄕歌"      <1>
}
```

1. A token written in Hanja: Hyangga


Which responds with:

```console-result
{
  "tokens" : [ {
    "token" : "향가",     <1>
    "start_offset" : 0,
    "end_offset" : 2,
    "type" : "word",
    "position" : 0
  }]
}
```

1. The Hanja form is replaced by the Hangul translation.


