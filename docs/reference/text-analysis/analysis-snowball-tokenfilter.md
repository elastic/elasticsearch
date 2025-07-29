---
navigation_title: "Snowball"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-snowball-tokenfilter.html
applies_to:
 stack: all 
---

# Snowball token filter [analysis-snowball-tokenfilter]


A filter that stems words using a Snowball-generated stemmer. The `language` parameter controls the stemmer with the following available values: `Arabic`, `Armenian`, `Basque`, `Catalan`, `Danish`, `Dutch`, `English`, `Estonian`, `Finnish`, `French`, `German`, `German2`, `Hungarian`, `Italian`, `Irish`, `Kp`, `Lithuanian`, `Lovins`, `Norwegian`, `Porter`, `Portuguese`, `Romanian`, `Russian`, `Serbian`, `Spanish`, `Swedish`, `Turkish`.

:::{note}
The `Kp` and `Lovins` values are deprecated in 8.16.0 and will be removed in a future version.
:::

For example:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [ "lowercase", "my_snow" ]
        }
      },
      "filter": {
        "my_snow": {
          "type": "snowball",
          "language": "English"
        }
      }
    }
  }
}
```

