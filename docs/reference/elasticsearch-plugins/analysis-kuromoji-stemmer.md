---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-stemmer.html
---

# kuromoji_stemmer token filter [analysis-kuromoji-stemmer]

The `kuromoji_stemmer` token filter normalizes common katakana spelling variations ending in a long sound character by removing this character (U+30FC). Only full-width katakana characters are supported.

This token filter accepts the following setting:

`minimum_length`
:   Katakana words shorter than the `minimum length` are not stemmed (default is `4`).

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
              "my_katakana_stemmer"
            ]
          }
        },
        "filter": {
          "my_katakana_stemmer": {
            "type": "kuromoji_stemmer",
            "minimum_length": 4
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "コピー" <1>
}

GET kuromoji_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "サーバー" <2>
}
```

1. Returns `コピー`.
2. Return `サーバ`.


