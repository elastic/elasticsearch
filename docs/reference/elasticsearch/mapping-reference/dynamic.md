---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html
---

# dynamic [dynamic]

When you index a document containing a new field, {{es}} [adds the field dynamically](docs-content://manage-data/data-store/mapping/dynamic-mapping.md) to a document or to inner objects within a document. The following document adds the string field `username`, the object field `name`, and two string fields under the `name` object:

```console
PUT my-index-000001/_doc/1
{
  "username": "johnsmith",
  "name": { <1>
    "first": "John",
    "last": "Smith"
  }
}

GET my-index-000001/_mapping <2>
```

1. Refer to fields under the `name` object as `name.first` and `name.last`.
2. Check the mapping to view changes.


The following document adds two string fields: `email` and `name.middle`:

```console
PUT my-index-000001/_doc/2
{
  "username": "marywhite",
  "email": "mary@white.com",
  "name": {
    "first": "Mary",
    "middle": "Alice",
    "last": "White"
  }
}

GET my-index-000001/_mapping
```

## Setting `dynamic` on inner objects [dynamic-inner-objects]

[Inner objects](/reference/elasticsearch/mapping-reference/object.md) inherit the `dynamic` setting from their parent object. In the following example, dynamic mapping is disabled at the type level, so no new top-level fields will be added dynamically.

However, the `user.social_networks` object enables dynamic mapping, so you can add fields to this inner object.

```console
PUT my-index-000001
{
  "mappings": {
    "dynamic": false, <1>
    "properties": {
      "user": { <2>
        "properties": {
          "name": {
            "type": "text"
          },
          "social_networks": {
            "dynamic": true, <3>
            "properties": {}
          }
        }
      }
    }
  }
}
```

1. Disables dynamic mapping at the type level.
2. The `user` object inherits the type-level setting.
3. Enables dynamic mapping for this inner object.



## Parameters for `dynamic` [dynamic-parameters]

The `dynamic` parameter controls whether new fields are added dynamically, and accepts the following parameters:

`true`
:   New fields are added to the mapping (default).

`runtime`
:   New fields are added to the mapping as [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md). These fields are not indexed, and are loaded from `_source` at query time.

`false`
:   New fields are ignored. These fields will not be indexed or searchable, but will still appear in the `_source` field of returned hits. These fields will not be added to the mapping, and new fields must be added explicitly.

`strict`
:   If new fields are detected, an exception is thrown and the document is rejected. New fields must be explicitly added to the mapping.


## Behavior when reaching the field limit [dynamic-field-limit]

Setting `dynamic` to either `true` or `runtime` will only add dynamic fields until `index.mapping.total_fields.limit` is reached. By default, index requests for documents that would exceed the field limit will fail, unless `index.mapping.total_fields.ignore_dynamic_beyond_limit` is set to `true`. In that case, ignored fields are added to the [`_ignored` metadata field](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md). For more index setting details, refer to [](/reference/elasticsearch/index-settings/mapping-limit.md).


