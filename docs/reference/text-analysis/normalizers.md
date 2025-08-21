---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-normalizers.html
---

# Normalizers [analysis-normalizers]

Normalizers are similar to analyzers except that they may only emit a single token. As a consequence, they do not have a tokenizer and only accept a subset of the available char filters and token filters. Only the filters that work on a per-character basis are allowed. For instance a lowercasing filter would be allowed, but not a stemming filter, which needs to look at the keyword as a whole. The current list of filters that can be used in a normalizer definition are: `arabic_normalization`, `asciifolding`, `bengali_normalization`, `cjk_width`, `decimal_digit`, `elision`, `german_normalization`, `hindi_normalization`, `indic_normalization`, `lowercase`, `pattern_replace`, `persian_normalization`, `scandinavian_folding`, `serbian_normalization`, `sorani_normalization`, `trim`, `uppercase`.

Elasticsearch ships with a `lowercase` built-in normalizer. For other forms of normalization, a custom configuration is required.


## Custom normalizers [_custom_normalizers]

Custom normalizers take a list of [character filters](/reference/text-analysis/character-filter-reference.md) and a list of [token filters](/reference/text-analysis/token-filter-reference.md).

```console
PUT index
{
  "settings": {
    "analysis": {
      "char_filter": {
        "quote": {
          "type": "mapping",
          "mappings": [
            "« => \"",
            "» => \""
          ]
        }
      },
      "normalizer": {
        "my_normalizer": {
          "type": "custom",
          "char_filter": ["quote"],
          "filter": ["lowercase", "asciifolding"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "foo": {
        "type": "keyword",
        "normalizer": "my_normalizer"
      }
    }
  }
}
```

