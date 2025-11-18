---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori-speech.html
---

# nori_part_of_speech token filter [analysis-nori-speech]

The `nori_part_of_speech` token filter removes tokens that match a set of part-of-speech tags. The list of supported tags and their meanings can be found here: [Part of speech tags](https://lucene.apache.org/core/10_1_0/core/../analysis/nori/org/apache/lucene/analysis/ko/POS.Tag.html)

It accepts the following setting:

`stoptags`
:   An array of part-of-speech tags that should be removed.

and defaults to:

```js
"stoptags": [
    "E",
    "IC",
    "J",
    "MAG", "MAJ", "MM",
    "SP", "SSC", "SSO", "SC", "SE",
    "XPN", "XSA", "XSN", "XSV",
    "UNA", "NA", "VSV"
]
```

For example:

```console
PUT nori_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_analyzer": {
            "tokenizer": "nori_tokenizer",
            "filter": [
              "my_posfilter"
            ]
          }
        },
        "filter": {
          "my_posfilter": {
            "type": "nori_part_of_speech",
            "stoptags": [
              "NR"   <1>
            ]
          }
        }
      }
    }
  }
}

GET nori_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "여섯 용이"  <2>
}
```

1. Korean numerals should be removed (`NR`)
2. Six dragons


Which responds with:

```console-result
{
  "tokens" : [ {
    "token" : "용",
    "start_offset" : 3,
    "end_offset" : 4,
    "type" : "word",
    "position" : 1
  }, {
    "token" : "이",
    "start_offset" : 4,
    "end_offset" : 5,
    "type" : "word",
    "position" : 2
  } ]
}
```

