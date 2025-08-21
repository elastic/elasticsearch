---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-murmur3-usage.html
---

# Using the murmur3 field [mapper-murmur3-usage]

The `murmur3` is typically used within a multi-field, so that both the original value and its hash are stored in the index:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "keyword",
        "fields": {
          "hash": {
            "type": "murmur3"
          }
        }
      }
    }
  }
}
```

Such a mapping would allow to refer to `my_field.hash` in order to get hashes of the values of the `my_field` field. This is only useful in order to run `cardinality` aggregations:

```console
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "This is a document"
}

PUT my-index-000001/_doc/2
{
  "my_field": "This is another document"
}

GET my-index-000001/_search
{
  "aggs": {
    "my_field_cardinality": {
      "cardinality": {
        "field": "my_field.hash" <1>
      }
    }
  }
}
```

1. Counting unique values on the `my_field.hash` field


Running a `cardinality` aggregation on the `my_field` field directly would yield the same result, however using `my_field.hash` instead might result in a speed-up if the field has a high-cardinality. On the other hand, it is discouraged to use the `murmur3` field on numeric fields and string fields that are not almost unique as the use of a `murmur3` field is unlikely to bring significant speed-ups, while increasing the amount of disk space required to store the index.

