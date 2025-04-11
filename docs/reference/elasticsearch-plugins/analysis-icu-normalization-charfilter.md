---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-normalization-charfilter.html
---

# ICU normalization character filter [analysis-icu-normalization-charfilter]

Normalizes characters as explained [here](https://unicode-org.github.io/icu/userguide/transforms/normalization/). It registers itself as the `icu_normalizer` character filter, which is available to all indices without any further configuration. The type of normalization can be specified with the `name` parameter, which accepts `nfc`, `nfkc`, and `nfkc_cf` (default). Set the `mode` parameter to `decompose` to convert `nfc` to `nfd` or `nfkc` to `nfkd` respectively:

Which letters are normalized can be controlled by specifying the `unicode_set_filter` parameter, which accepts a [UnicodeSet](https://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.md).

Here are two examples, the default usage and a customised character filter:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "nfkc_cf_normalized": { <1>
            "tokenizer": "icu_tokenizer",
            "char_filter": [
              "icu_normalizer"
            ]
          },
          "nfd_normalized": { <2>
            "tokenizer": "icu_tokenizer",
            "char_filter": [
              "nfd_normalizer"
            ]
          }
        },
        "char_filter": {
          "nfd_normalizer": {
            "type": "icu_normalizer",
            "name": "nfc",
            "mode": "decompose"
          }
        }
      }
    }
  }
}
```

1. Uses the default `nfkc_cf` normalization.
2. Uses the customized `nfd_normalizer` token filter, which is set to use `nfc` normalization with decomposition.


