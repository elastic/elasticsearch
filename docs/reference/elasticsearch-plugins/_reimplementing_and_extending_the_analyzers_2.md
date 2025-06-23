---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/_reimplementing_and_extending_the_analyzers_2.html
---

# Reimplementing and extending the analyzers [_reimplementing_and_extending_the_analyzers_2]

The `polish` analyzer could be reimplemented as a `custom` analyzer that can then be extended and configured differently as follows:

```console
PUT /stempel_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_stempel": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "polish_stop",
            "polish_stem"
          ]
        }
      }
    }
  }
}
```

