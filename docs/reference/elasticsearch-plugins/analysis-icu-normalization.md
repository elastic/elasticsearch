---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-normalization.html
---

# ICU normalization token filter [analysis-icu-normalization]

Normalizes characters as explained [here](https://unicode-org.github.io/icu/userguide/transforms/normalization/). It registers itself as the `icu_normalizer` token filter, which is available to all indices without any further configuration. The type of normalization can be specified with the `name` parameter, which accepts `nfc`, `nfkc`, and `nfkc_cf` (default).

Which letters are normalized can be controlled by specifying the `unicode_set_filter` parameter, which accepts a [UnicodeSet](https://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.md).

You should probably prefer the [Normalization character filter](/reference/elasticsearch-plugins/analysis-icu-normalization-charfilter.md).

Here are two examples, the default usage and a customised token filter:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "nfkc_cf_normalized": { <1>
            "tokenizer": "icu_tokenizer",
            "filter": [
              "icu_normalizer"
            ]
          },
          "nfc_normalized": { <2>
            "tokenizer": "icu_tokenizer",
            "filter": [
              "nfc_normalizer"
            ]
          }
        },
        "filter": {
          "nfc_normalizer": {
            "type": "icu_normalizer",
            "name": "nfc"
          }
        }
      }
    }
  }
}
```

1. Uses the default `nfkc_cf` normalization.
2. Uses the customized `nfc_normalizer` token filter, which is set to use `nfc` normalization.


