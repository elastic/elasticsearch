---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/null-value.html
---

# null_value [null-value]

A `null` value cannot be indexed or searched. When a field is set to `null`, (or an empty array or an array of `null` values)  it is treated as though that field has no values.

The `null_value` parameter allows you to replace explicit `null` values with the specified value so that it can be indexed and searched. For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": {
        "type":       "keyword",
        "null_value": "NULL" <1>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "status_code": null
}

PUT my-index-000001/_doc/2
{
  "status_code": [] <2>
}

GET my-index-000001/_search
{
  "query": {
    "term": {
      "status_code": "NULL" <3>
    }
  }
}
```

1. Replace explicit `null` values with the term `NULL`.
2. An empty array does not contain an explicit `null`, and so won’t be replaced with the `null_value`.
3. A query for `NULL` returns document 1, but not document 2.


::::{important}
The `null_value` needs to be the same data type as the field. For instance, a `long` field cannot have a string `null_value`.
::::


::::{note}
The `null_value` only influences how data is indexed, it doesn’t modify the `_source` document.
::::


