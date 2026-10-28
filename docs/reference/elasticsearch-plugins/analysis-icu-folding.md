---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-folding.html
---

# ICU folding token filter [analysis-icu-folding]

Case folding of Unicode characters based on `UTR#30`, like the [ASCII-folding token filter](/reference/text-analysis/analysis-asciifolding-tokenfilter.md) on steroids. It registers itself as the `icu_folding` token filter and is available to all indices:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "folded": {
            "tokenizer": "icu_tokenizer",
            "filter": [
              "icu_folding"
            ]
          }
        }
      }
    }
  }
}
```

The ICU folding token filter already does Unicode normalization, so there is no need to use Normalize character or token filter as well.

Which letters are folded can be controlled by specifying the `unicode_set_filter` parameter, which accepts a [UnicodeSet](https://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.md).

The following example exempts Swedish characters from folding. It is important to note that both upper and lowercase forms should be specified, and that these filtered character are not lowercased which is why we add the `lowercase` filter as well:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "swedish_analyzer": {
            "tokenizer": "icu_tokenizer",
            "filter": [
              "swedish_folding",
              "lowercase"
            ]
          }
        },
        "filter": {
          "swedish_folding": {
            "type": "icu_folding",
            "unicode_set_filter": "[^åäöÅÄÖ]"
          }
        }
      }
    }
  }
}
```

