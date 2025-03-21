---
navigation_title: "Nested"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html
---

# Nested field type [nested]


The `nested` type is a specialised version of the [`object`](/reference/elasticsearch/mapping-reference/object.md) data type that allows arrays of objects to be indexed in a way that they can be queried independently of each other.

::::{tip}
When ingesting key-value pairs with a large, arbitrary set of keys, you might consider modeling each key-value pair as its own nested document with `key` and `value` fields. Instead, consider using the [flattened](/reference/elasticsearch/mapping-reference/flattened.md) data type, which maps an entire object as a single field and allows for simple searches over its contents. Nested documents and queries are typically expensive, so using the `flattened` data type for this use case is a better option.
::::


::::{warning}
Nested fields have incomplete support in Kibana. While they are visible and searchable in Discover, they cannot be used to build visualizations in Lens.
::::


## How arrays of objects are flattened [nested-arrays-flattening-objects]

Elasticsearch has no concept of inner objects. Therefore, it flattens object hierarchies into a simple list of field names and values. For instance, consider the following document:

```console
PUT my-index-000001/_doc/1
{
  "group" : "fans",
  "user" : [ <1>
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ]
}
```

1. The `user` field is dynamically added as a field of type `object`.


The previous document would be transformed internally into a document that looks more like this:

```js
{
  "group" :        "fans",
  "user.first" : [ "alice", "john" ],
  "user.last" :  [ "smith", "white" ]
}
```

The `user.first` and `user.last` fields are flattened into multi-value fields, and the association between `alice` and `white` is lost. This document would incorrectly match a query for `alice AND smith`:

```console
GET my-index-000001/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "user.first": "Alice" }},
        { "match": { "user.last":  "Smith" }}
      ]
    }
  }
}
```


## Using `nested` fields for arrays of objects [nested-fields-array-objects]

If you need to index arrays of objects and to maintain the independence of each object in the array, use the `nested` data type instead of the [`object`](/reference/elasticsearch/mapping-reference/object.md) data type.

Internally, nested objects index each object in the array as a separate hidden document, meaning that each nested object can be queried independently of the others with the [`nested` query](/reference/query-languages/query-dsl/query-dsl-nested-query.md):

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "user": {
        "type": "nested" <1>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "group" : "fans",
  "user" : [
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ]
}

GET my-index-000001/_search
{
  "query": {
    "nested": {
      "path": "user",
      "query": {
        "bool": {
          "must": [
            { "match": { "user.first": "Alice" }},
            { "match": { "user.last":  "Smith" }} <2>
          ]
        }
      }
    }
  }
}

GET my-index-000001/_search
{
  "query": {
    "nested": {
      "path": "user",
      "query": {
        "bool": {
          "must": [
            { "match": { "user.first": "Alice" }},
            { "match": { "user.last":  "White" }} <3>
          ]
        }
      },
      "inner_hits": { <4>
        "highlight": {
          "fields": {
            "user.first": {}
          }
        }
      }
    }
  }
}
```

1. The `user` field is mapped as type `nested` instead of type `object`.
2. This query doesn’t match because `Alice` and `Smith` are not in the same nested object.
3. This query matches because `Alice` and `White` are in the same nested object.
4. `inner_hits` allow us to highlight the matching nested documents.



## Interacting with `nested` documents [nested-accessing-documents]

Nested documents can be:

* queried with the [`nested`](/reference/query-languages/query-dsl/query-dsl-nested-query.md) query.
* analyzed with the [`nested`](/reference/aggregations/search-aggregations-bucket-nested-aggregation.md) and [`reverse_nested`](/reference/aggregations/search-aggregations-bucket-reverse-nested-aggregation.md) aggregations.
* sorted with [nested sorting](/reference/elasticsearch/rest-apis/sort-search-results.md#nested-sorting).
* retrieved and highlighted with [nested inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md#nested-inner-hits).

::::{important}
Because nested documents are indexed as separate documents, they can only be accessed within the scope of the `nested` query, the `nested`/`reverse_nested` aggregations, or [nested inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md#nested-inner-hits).

For instance, if a string field within a nested document has [`index_options`](/reference/elasticsearch/mapping-reference/index-options.md) set to `offsets` to allow use of the postings during the highlighting, these offsets will not be available during the main highlighting phase. Instead, highlighting needs to be performed via [nested inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md#nested-inner-hits). The same consideration applies when loading fields during a search through [`docvalue_fields`](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#docvalue-fields) or [`stored_fields`](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#stored-fields).

::::



## Parameters for `nested` fields [nested-params]

The following parameters are accepted by `nested` fields:

[`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md)
:   (Optional, string) Whether or not new `properties` should be added dynamically to an existing nested object. Accepts `true` (default), `false` and `strict`.

[`properties`](/reference/elasticsearch/mapping-reference/properties.md)
:   (Optional, object) The fields within the nested object, which can be of any [data type](/reference/elasticsearch/mapping-reference/field-data-types.md), including `nested`. New properties may be added to an existing nested object.

`include_in_parent`
:   (Optional, Boolean) If `true`, all fields in the nested object are also added to the parent document as standard (flat) fields. Defaults to `false`.

`include_in_root`
:   (Optional, Boolean) If `true`, all fields in the nested object are also added to the root document as standard (flat) fields. Defaults to `false`.


## Limits on `nested` mappings and objects [_limits_on_nested_mappings_and_objects]

As described earlier, each nested object is indexed as a separate Lucene document. Continuing with the previous example, if we indexed a single document containing 100 `user` objects, then 101 Lucene documents would be created: one for the parent document, and one for each nested object. Because of the expense associated with `nested` mappings, Elasticsearch puts settings in place to guard against performance problems:

`index.mapping.nested_fields.limit`
:   The maximum number of distinct `nested` mappings in an index. The `nested` type should only be used in special cases, when arrays of objects need to be queried independently of each other. To safeguard against poorly designed mappings, this setting limits the number of unique `nested` types per index. Default is `50`.

In the previous example, the `user` mapping would count as only 1 towards this limit.

`index.mapping.nested_objects.limit`
:   The maximum number of nested JSON objects that a single document can contain across all `nested` types. This limit helps to prevent out of memory errors when a document contains too many nested objects. Default is `10000`.

To illustrate how this setting works, consider adding another `nested` type called `comments` to the previous example mapping. For each document, the combined number of `user` and `comment` objects it contains must be below the limit.

See [Prevent mapping explosions](docs-content://manage-data/data-store/mapping.md#mapping-limit-settings) regarding additional settings for preventing mappings explosion.


