---
navigation_title: "Stemmer override"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stemmer-override-tokenfilter.html
---

# Stemmer override token filter [analysis-stemmer-override-tokenfilter]


Overrides stemming algorithms, by applying a custom mapping, then protecting these terms from being modified by stemmers. Must be placed before any stemming filters.

Rules are mappings in the form of `token1[, ..., tokenN] => override`.

| Setting | Description |
| --- | --- |
| `rules` | A list of mapping rules to use. |
| `rules_path` | A path (either relative to `config` location, orabsolute) to a list of mappings. |

Here is an example:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [ "lowercase", "custom_stems", "porter_stem" ]
        }
      },
      "filter": {
        "custom_stems": {
          "type": "stemmer_override",
          "rules_path": "analysis/stemmer_override.txt"
        }
      }
    }
  }
}
```

Where the file looks like:

```text
running, runs => run

stemmer => stemmer
```

You can also define the overrides rules inline:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [ "lowercase", "custom_stems", "porter_stem" ]
        }
      },
      "filter": {
        "custom_stems": {
          "type": "stemmer_override",
          "rules": [
            "running, runs => run",
            "stemmer => stemmer"
          ]
        }
      }
    }
  }
}
```

