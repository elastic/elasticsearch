---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/_reimplementing_and_extending_the_analyzers.html
---

# Reimplementing and extending the analyzers [_reimplementing_and_extending_the_analyzers]

The `smartcn` analyzer could be reimplemented as a `custom` analyzer that can then be extended and configured as follows:

```console
PUT smartcn_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_smartcn": {
          "tokenizer":  "smartcn_tokenizer",
          "filter": [
            "porter_stem",
            "smartcn_stop"
          ]
        }
      }
    }
  }
}
```

