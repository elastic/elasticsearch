---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-readingform.html
---

# kuromoji_readingform token filter [analysis-kuromoji-readingform]

The `kuromoji_readingform` token filter replaces the token with its reading form in either katakana or romaji. It accepts the following setting:

`use_romaji`
:   Whether romaji reading form should be output instead of katakana. Defaults to `false`.

When using the pre-defined `kuromoji_readingform` filter, `use_romaji` is set to `true`. The default when defining a custom `kuromoji_readingform`, however, is `false`. The only reason to use the custom form is if you need the katakana reading form:

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "romaji_analyzer": {
            "tokenizer": "kuromoji_tokenizer",
            "filter": [ "romaji_readingform" ]
          },
          "katakana_analyzer": {
            "tokenizer": "kuromoji_tokenizer",
            "filter": [ "katakana_readingform" ]
          }
        },
        "filter": {
          "romaji_readingform": {
            "type": "kuromoji_readingform",
            "use_romaji": true
          },
          "katakana_readingform": {
            "type": "kuromoji_readingform",
            "use_romaji": false
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "katakana_analyzer",
  "text": "寿司" <1>
}

GET kuromoji_sample/_analyze
{
  "analyzer": "romaji_analyzer",
  "text": "寿司" <2>
}
```

1. Returns `スシ`.
2. Returns `sushi`.


