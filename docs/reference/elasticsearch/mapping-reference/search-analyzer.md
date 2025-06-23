---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-analyzer.html
---

# search_analyzer [search-analyzer]

Usually, the same [analyzer](/reference/elasticsearch/mapping-reference/analyzer.md) should be applied at index time and at search time, to ensure that the terms in the query are in the same format as the terms in the inverted index.

Sometimes, though, it can make sense to use a different analyzer at search time, such as when using the  [`edge_ngram`](/reference/text-analysis/analysis-edgengram-tokenizer.md) tokenizer for autocomplete or when using search-time synonyms.

By default, queries will use the `analyzer` defined in the field mapping, but this can be overridden with the `search_analyzer` setting:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "filter": {
        "autocomplete_filter": {
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 20
        }
      },
      "analyzer": {
        "autocomplete": { <1>
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "autocomplete_filter"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "analyzer": "autocomplete", <2>
        "search_analyzer": "standard" <2>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "text": "Quick Brown Fox" <3>
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "text": {
        "query": "Quick Br", <4>
        "operator": "and"
      }
    }
  }
}
```

1. Analysis settings to define the custom `autocomplete` analyzer.
2. The `text` field uses the `autocomplete` analyzer at index time, but the `standard` analyzer at search time.
3. This field is indexed as the terms: [ `q`, `qu`, `qui`, `quic`, `quick`, `b`, `br`, `bro`, `brow`, `brown`, `f`, `fo`, `fox` ]
4. The query searches for both of these terms: [ `quick`, `br` ]


See [Index time search-as-you- type](https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_index_time_search_as_you_type.html) for a full explanation of this example.

::::{tip}
The `search_analyzer` setting can be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping). Note, that in order to do so, any existing "analyzer" setting and "type" need to be repeated in the updated field definition.
::::


