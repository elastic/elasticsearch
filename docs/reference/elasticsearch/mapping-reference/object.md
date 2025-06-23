---
navigation_title: "Object"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html
---

# Object field type [object]


JSON documents are hierarchical in nature: the document may contain inner objects which, in turn, may contain inner objects themselves:

```console
PUT my-index-000001/_doc/1
{ // <1>
  "region": "US",
  "manager": { <2>
    "age":     30,
    "name": { <3>
      "first": "John",
      "last":  "Smith"
    }
  }
}
```

1. The outer document is also a JSON object.
2. It contains an inner object called `manager`.
3. Which in turn contains an inner object called `name`.


Internally, this document is indexed as a simple, flat list of key-value pairs, something like this:

```js
{
  "region":             "US",
  "manager.age":        30,
  "manager.name.first": "John",
  "manager.name.last":  "Smith"
}
```

An explicit mapping for the above document could look like this:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": { <1>
      "region": {
        "type": "keyword"
      },
      "manager": { <2>
        "properties": {
          "age":  { "type": "integer" },
          "name": { <3>
            "properties": {
              "first": { "type": "text" },
              "last":  { "type": "text" }
            }
          }
        }
      }
    }
  }
}
```

1. Properties in the top-level mappings definition.
2. The `manager` field is an inner `object` field.
3. The `manager.name` field is an inner `object` field within the `manager` field.


You are not required to set the field `type` to `object` explicitly, as this is the default value.

## Parameters for `object` fields [object-params]

The following parameters are accepted by `object` fields:

[`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md)
:   Whether or not new `properties` should be added dynamically to an existing object. Accepts `true` (default), `runtime`, `false` and `strict`.

[`enabled`](/reference/elasticsearch/mapping-reference/enabled.md)
:   Whether the JSON value given for the object field should be parsed and indexed (`true`, default) or completely ignored (`false`).

[`subobjects`](/reference/elasticsearch/mapping-reference/subobjects.md)
:   Whether the object can hold subobjects (`true`, default) or not (`false`). If not, sub-fields with dots in their names will be treated as leaves instead, otherwise their field names would be expanded to their corresponding object structure.

[`properties`](/reference/elasticsearch/mapping-reference/properties.md)
:   The fields within the object, which can be of any [data type](/reference/elasticsearch/mapping-reference/field-data-types.md), including `object`. New properties may be added to an existing object.

::::{important}
If you need to index arrays of objects instead of single objects, read [Nested](/reference/elasticsearch/mapping-reference/nested.md) first.
::::



