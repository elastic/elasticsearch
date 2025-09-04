---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-transform.html
---

# ICU transform token filter [analysis-icu-transform]

Transforms are used to process Unicode text in many different ways, such as case mapping, normalization, transliteration and bidirectional text handling.

You can define which transformation you want to apply with the `id` parameter (defaults to `Null`), and specify text direction with the `dir` parameter which accepts `forward` (default) for LTR and `reverse` for RTL. Custom rulesets are not yet supported.

For example:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "latin": {
            "tokenizer": "keyword",
            "filter": [
              "myLatinTransform"
            ]
          }
        },
        "filter": {
          "myLatinTransform": {
            "type": "icu_transform",
            "id": "Any-Latin; NFD; [:Nonspacing Mark:] Remove; NFC" <1>
          }
        }
      }
    }
  }
}

GET icu_sample/_analyze
{
  "analyzer": "latin",
  "text": "你好" <2>
}

GET icu_sample/_analyze
{
  "analyzer": "latin",
  "text": "здравствуйте" <3>
}

GET icu_sample/_analyze
{
  "analyzer": "latin",
  "text": "こんにちは" <4>
}
```

1. This transforms transliterates characters to Latin, and separates accents from their base characters, removes the accents, and then puts the remaining text into an unaccented form.
2. Returns `ni hao`.
3. Returns `zdravstvujte`.
4. Returns `kon'nichiha`.


For more documentation, Please see the [user guide of ICU Transform](https://unicode-org.github.io/icu/userguide/transforms/).

