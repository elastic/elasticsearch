---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html
---

# store [mapping-store]

By default, field values are [indexed](/reference/elasticsearch/mapping-reference/mapping-index.md) to make them searchable, but they are not *stored*. This means that the field can be queried, but the original field value cannot be retrieved.

Usually this doesn’t matter. The field value is already part of the [`_source` field](/reference/elasticsearch/mapping-reference/mapping-source-field.md), which is stored by default. If you only want to retrieve the value of a single field or of a few fields, instead of the whole `_source`, then this can be achieved with [source filtering](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering).

In certain situations it can make sense to `store` a field. For instance, if you have a document with a `title`, a `date`, and a very large `content` field, you may want to retrieve just the `title` and the `date` without having to extract those fields from a large `_source` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "store": true <1>
      },
      "date": {
        "type": "date",
        "store": true <1>
      },
      "content": {
        "type": "text"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "title":   "Some short title",
  "date":    "2015-01-01",
  "content": "A very long content field..."
}

GET my-index-000001/_search
{
  "stored_fields": [ "title", "date" ] <2>
}
```

1. The `title` and `date` fields are stored.
2. This request will retrieve the values of the `title` and `date` fields.


::::{admonition} Stored fields returned as arrays
:class: note

For consistency, stored fields are always returned as an *array* because there is no way of knowing if the original field value was a single value, multiple values, or an empty array.

If you need the original value, you should retrieve it from the `_source` field instead.

::::


