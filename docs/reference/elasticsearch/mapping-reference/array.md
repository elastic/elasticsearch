---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html
---

# Arrays [array]

In Elasticsearch, there is no dedicated `array` data type. Any field can contain zero or more values by default, however, all values in the array must be of the same data type. For instance:

* an array of strings: [ `"one"`, `"two"` ]
* an array of integers: [ `1`, `2` ]
* an array of arrays: [ `1`, [ `2`, `3` ]] which is the equivalent of [ `1`, `2`, `3` ]
* an array of objects: [ `{ "name": "Mary", "age": 12 }`, `{ "name": "John", "age": 10 }`]

::::{admonition} Arrays with object field type vs nested type
:class: note

Arrays of objects in Elasticsearch do not behave as you would expect: queries may match fields across different objects in the array, leading to unexpected results. By default, arrays of objects are [flattened](/reference/elasticsearch/mapping-reference/nested.md#nested-arrays-flattening-objects) during indexing. To ensure queries match values within the same object, use the [`nested`](/reference/elasticsearch/mapping-reference/nested.md) data type instead of the [`object`](/reference/elasticsearch/mapping-reference/object.md) data type.

This behavior is explained in more detail in [`nested`](/reference/elasticsearch/mapping-reference/nested.md#nested-arrays-flattening-objects).

::::


When adding a field dynamically, the first value in the array determines the field `type`. All subsequent values must be of the same data type or it must at least be possible to [coerce](/reference/elasticsearch/mapping-reference/coerce.md) subsequent values to the same data type.

Arrays with a mixture of data types are *not* supported: [ `10`, `"some string"` ]

An array may contain `null` values, which are either replaced by the configured [`null_value`](/reference/elasticsearch/mapping-reference/null-value.md) or skipped entirely. An empty array `[]` is treated as a missing field — a field with no values.

Nothing needs to be pre-configured in order to use arrays in documents, they are supported out of the box:

```console
PUT my-index-000001/_doc/1
{
  "message": "some arrays in this document...",
  "tags":  [ "elasticsearch", "wow" ], <1>
  "lists": [ <2>
    {
      "name": "prog_list",
      "description": "programming list"
    },
    {
      "name": "cool_list",
      "description": "cool stuff list"
    }
  ]
}

PUT my-index-000001/_doc/2 <3>
{
  "message": "no arrays in this document...",
  "tags":  "elasticsearch",
  "lists": {
    "name": "prog_list",
    "description": "programming list"
  }
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "tags": "elasticsearch" <4>
    }
  }
}
```

1. The `tags` field is dynamically added as a `string` field.
2. The `lists` field is dynamically added as an `object` field.
3. The second document contains no arrays, but can be indexed into the same fields.
4. The query looks for `elasticsearch` in the `tags` field, and matches both documents.


::::{tip}
You can modify arrays using the [update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update).

::::


