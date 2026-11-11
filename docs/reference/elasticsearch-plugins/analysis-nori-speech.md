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
    "IC",
    "MAG", "MAJ", "MM",
    "SP", "SSC", "SSO", "SC", "SE",
    "XPN", "XSA", "XSN", "XSV",
    "UNA", "NA", "VSV"
]
```

::::{warning}
The default `stoptags` include `XPN` (prefix). For Korean terms where a meaning-carrying prefix is emitted as `XPN`, the default `nori` analyzer removes the prefix. For example, `비급여` is analyzed as `급여`:

```console
GET _analyze
{
  "analyzer": "nori",
  "text": "비급여"
}
```

Which responds with:

```console-result
{
  "tokens" : [ {
    "token" : "급여",
    "start_offset" : 1,
    "end_offset" : 3,
    "type" : "word",
    "position" : 1
  } ]
}
```

To preserve the full term, add entries such as `비급여` with `user_dictionary_rules` so they are emitted as a single noun token. To preserve prefix tokens globally, define a custom `stoptags` list that omits `XPN`; this preserves prefixes such as `비` but can add prefix noise.
::::

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

