---
navigation_title: "Retrieve selected fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
applies_to:
  stack: all
---

# Retrieve selected fields from a search [search-fields]


By default, each hit in the search response includes the document [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md), which is the entire JSON object that was provided when indexing the document. There are two recommended methods to retrieve selected fields from a search query:

* Use the [`fields` option](#search-fields-param) to extract the values of fields present in the index mapping
* Use the [`_source` option](#source-filtering) if you need to access the original data that was passed at index time

You can use both of these methods, though the `fields` option is preferred because it consults both the document data and index mappings. In some instances, you might want to use [other methods](#field-retrieval-methods) of retrieving data.


### The `fields` option [search-fields-param]

To retrieve specific fields in the search response, use the `fields` parameter. Because it consults the index mappings, the `fields` parameter provides several advantages over referencing the `_source` directly. Specifically, the `fields` parameter:

* Returns each value in a standardized way that matches its mapping type
* Accepts [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md) and [field aliases](/reference/elasticsearch/mapping-reference/field-alias.md)
* Formats dates and spatial data types
* Retrieves [runtime field values](docs-content://manage-data/data-store/mapping/retrieve-runtime-field.md)
* Returns fields calculated by a script at index time
* Returns fields from related indices using [lookup runtime fields](docs-content://manage-data/data-store/mapping/retrieve-runtime-field.md#lookup-runtime-fields)

Other mapping options are also respected, including [`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md), [`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md), and [`null_value`](/reference/elasticsearch/mapping-reference/null-value.md).

The `fields` option returns values in the way that matches how {{es}} indexes them. For standard fields, this means that the `fields` option looks in `_source` to find the values, then parses and formats them using the mappings. Selected fields that can’t be found in `_source` are skipped.


#### Retrieve specific fields [search-fields-request]

The following search request uses the `fields` parameter to retrieve values for the `user.id` field, all fields starting with `http.response.`, and the `@timestamp` field.

Using object notation, you can pass a [`format`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) argument to customize the format of returned date or geospatial values.

```console
POST my-index-000001/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  },
  "fields": [
    "user.id",
    "http.response.*",         <1>
    {
      "field": "@timestamp",
      "format": "epoch_millis" <2>
    }
  ],
  "_source": false
}
```

1. Both full field names and wildcard patterns are accepted.
2. Use the `format` parameter to apply a custom format for the field’s values.


::::{note}
By default, document metadata fields like `_id` or `_index` are not returned when the requested `fields` option uses wildcard patterns like `*`. However, when explicitly requested using the field name, the `_id`, `_routing`, `_ignored`, `_index` and `_version` metadata fields can be retrieved.
::::



#### Response always returns an array [search-fields-response]

The `fields` response always returns an array of values for each field, even when there is a single value in the `_source`. This is because {{es}} has no dedicated array type, and any field could contain multiple values. The `fields` parameter also does not guarantee that array values are returned in a specific order. See the mapping documentation on [arrays](/reference/elasticsearch/mapping-reference/array.md) for more background.

The response includes values as a flat list in the `fields` section for each hit. Because the `fields` parameter doesn’t fetch entire objects, only leaf fields are returned.

```console-result
{
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "my-index-000001",
        "_id" : "0",
        "_score" : 1.0,
        "fields" : {
          "user.id" : [
            "kimchy"
          ],
          "@timestamp" : [
            "4098435132000"
          ],
          "http.response.bytes": [
            1070000
          ],
          "http.response.status_code": [
            200
          ]
        }
      }
    ]
  }
}
```


#### Retrieve nested fields [search-fields-nested]

::::{dropdown}
The `fields` response for [`nested` fields](/reference/elasticsearch/mapping-reference/nested.md) is slightly different from that of regular object fields. While leaf values inside regular `object` fields are returned as a flat list, values inside `nested` fields are grouped to maintain the independence of each object inside the original nested array. For each entry inside a nested field array, values are again returned as a flat list unless there are other `nested` fields inside the parent nested object, in which case the same procedure is repeated again for the deeper nested fields.

Given the following mapping where `user` is a nested field, after indexing the following document and retrieving all fields under the `user` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "group" : { "type" : "keyword" },
      "user": {
        "type": "nested",
        "properties": {
          "first" : { "type" : "keyword" },
          "last" : { "type" : "keyword" }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1?refresh=true
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

POST my-index-000001/_search
{
  "fields": ["*"],
  "_source": false
}
```

The response will group `first` and `last` name instead of returning them as a flat list.

```console-result
{
  "took": 2,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [{
      "_index": "my-index-000001",
      "_id": "1",
      "_score": 1.0,
      "fields": {
        "group" : ["fans"],
        "user": [{
            "first": ["John"],
            "last": ["Smith"]
          },
          {
            "first": ["Alice"],
            "last": ["White"]
          }
        ]
      }
    }]
  }
}
```

Nested fields will be grouped by their nested paths, no matter the pattern used to retrieve them. For example, if you query only for the `user.first` field from the previous example:

```console
POST my-index-000001/_search
{
  "fields": ["user.first"],
  "_source": false
}
```

The response returns only the user’s first name, but still maintains the structure of the nested `user` array:

```console-result
{
  "took": 2,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [{
      "_index": "my-index-000001",
      "_id": "1",
      "_score": 1.0,
      "fields": {
        "user": [{
            "first": ["John"]
          },
          {
            "first": ["Alice"]
          }
        ]
      }
    }]
  }
}
```

However, when the `fields` pattern targets the nested `user` field directly, no values will be returned because the pattern doesn’t match any leaf fields.

::::



#### Retrieve unmapped fields [retrieve-unmapped-fields]

::::{dropdown}
By default, the `fields` parameter returns only values of mapped fields. However, {{es}} allows storing fields in `_source` that are unmapped, such as setting [dynamic field mapping](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md) to `false` or by using an object field with `enabled: false`. These options disable parsing and indexing of the object content.

To retrieve unmapped fields in an object from `_source`, use the `include_unmapped` option in the `fields` section:

```console
PUT my-index-000001
{
  "mappings": {
    "enabled": false <1>
  }
}

PUT my-index-000001/_doc/1?refresh=true
{
  "user_id": "kimchy",
  "session_data": {
     "object": {
       "some_field": "some_value"
     }
   }
}

POST my-index-000001/_search
{
  "fields": [
    "user_id",
    {
      "field": "session_data.object.*",
      "include_unmapped" : true <2>
    }
  ],
  "_source": false
}
```

1. Disable all mappings.
2. Include unmapped fields matching this field pattern.


The response will contain field results under the  `session_data.object.*` path, even if the fields are unmapped. The `user_id` field is also unmapped, but it won’t be included in the response because `include_unmapped` isn’t set to `true` for that field pattern.

```console-result
{
  "took" : 2,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "my-index-000001",
        "_id" : "1",
        "_score" : 1.0,
        "fields" : {
          "session_data.object.some_field": [
            "some_value"
          ]
        }
      }
    ]
  }
}
```

::::



#### Ignored field values [ignored-field-values]

::::{dropdown}
The `fields` section of the response only returns values that were valid when indexed. If your search request asks for values from a field that ignored certain values because they were malformed or too large these values are returned separately in an `ignored_field_values` section.

In this example we index a document that has a value which is ignored and not added to the index so is shown separately in search results:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my-small" : { "type" : "keyword", "ignore_above": 2 }, <1>
      "my-large" : { "type" : "keyword" }
    }
  }
}

PUT my-index-000001/_doc/1?refresh=true
{
  "my-small": ["ok", "bad"], <2>
  "my-large": "ok content"
}

POST my-index-000001/_search
{
  "fields": ["my-*"],
  "_source": false
}
```

1. This field has a size restriction
2. This document field has a value that exceeds the size restriction so is ignored and not indexed


The response will contain ignored field values under the  `ignored_field_values` path. These values are retrieved from the document’s original JSON source and are raw so will not be formatted or treated in any way, unlike the successfully indexed fields which are returned in the `fields` section.

```console-result
{
  "took" : 2,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "my-index-000001",
        "_id" : "1",
        "_score" : 1.0,
        "_ignored" : [ "my-small"],
        "fields" : {
          "my-large": [
            "ok content"
          ],
          "my-small": [
            "ok"
          ]
        },
        "ignored_field_values" : {
          "my-small": [
            "bad"
          ]
        }
      }
    ]
  }
}
```

::::



## The `_source` option [source-filtering]

You can use the `_source` parameter to select what fields of the source are returned. This is called *source filtering*.

The following search API request sets the `_source` request body parameter to `false`. The document source is not included in the response.

```console
GET /_search
{
  "_source": false,
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```

To return only a subset of source fields, specify a wildcard (`*`) pattern in the `_source` parameter. The following search API request returns the source for only the `obj` field and its properties.

```console
GET /_search
{
  "_source": "obj.*",
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```

You can also specify an array of wildcard patterns in the `_source` field. The following search API request returns the source for only the `obj1` and `obj2` fields and their properties.

```console
GET /_search
{
  "_source": [ "obj1.*", "obj2.*" ],
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```

For finer control, you can specify an object containing arrays of `includes` and `excludes` patterns in the `_source` parameter.

If the `includes` property is specified, only source fields that match one of its patterns are returned. You can exclude fields from this subset using the `excludes` property.

If the `includes` property is not specified, the entire document source is returned, excluding any fields that match a pattern in the `excludes` property.

The following search API request returns the source for only the `obj1` and `obj2` fields and their properties, excluding any child `description` fields.

```console
GET /_search
{
  "_source": {
    "includes": [ "obj1.*", "obj2.*" ],
    "excludes": [ "*.description" ]
  },
  "query": {
    "term": {
      "user.id": "kimchy"
    }
  }
}
```


### Other methods of retrieving data [field-retrieval-methods]

::::{admonition} Using fields is typically better
These options are usually not required. Using the `fields` option is typically the better choice, unless you absolutely need to force loading a stored or `docvalue_fields`.

::::


A document’s `_source` is stored as a single field in Lucene. This structure means that the whole `_source` object must be loaded and parsed even if you’re only requesting part of it. To avoid this limitation, you can try other options for loading fields:

* Use the [`docvalue_fields`](#docvalue-fields) parameter to get values for selected fields. This can be a good choice when returning a fairly small number of fields that support doc values, such as keywords and dates.
* Use the [`stored_fields`](#stored-fields) parameter to get the values for specific stored fields (fields that use the [`store`](/reference/elasticsearch/mapping-reference/mapping-store.md) mapping option).

{{es}} always attempts to load values from `_source`. This behavior has the same implications of source filtering where {{es}} needs to load and parse the entire `_source` to retrieve just one field.


#### Doc value fields [docvalue-fields]

You can use the [`docvalue_fields`](#docvalue-fields) parameter to return [doc values](/reference/elasticsearch/mapping-reference/doc-values.md) for one or more fields in the search response.

Doc values store the same values as the `_source` but in an on-disk, column-based structure that’s optimized for sorting and aggregations. Since each field is stored separately, {{es}} only reads the field values that were requested and can avoid loading the whole document `_source`.

Doc values are stored for supported fields by default. However, doc values are not supported for [`text`](/reference/elasticsearch/mapping-reference/text.md) or [`text_annotated`](/reference/elasticsearch-plugins/mapper-annotated-text-usage.md) fields.

The following search request uses the `docvalue_fields` parameter to retrieve doc values for the `user.id` field, all fields starting with `http.response.`, and the `@timestamp` field:

```console
GET my-index-000001/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  },
  "docvalue_fields": [
    "user.id",
    "http.response.*", <1>
    {
      "field": "date",
      "format": "epoch_millis" <2>
    }
  ]
}
```

1. Both full field names and wildcard patterns are accepted.
2. Using object notation, you can pass a `format` parameter to apply a custom format for the field’s doc values. [Date fields](/reference/elasticsearch/mapping-reference/date.md) support a [date `format`](/reference/elasticsearch/mapping-reference/mapping-date-format.md). [Numeric fields](/reference/elasticsearch/mapping-reference/number.md) support a [DecimalFormat pattern](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.md). Other field datatypes do not support the `format` parameter.


::::{tip}
You cannot use the `docvalue_fields` parameter to retrieve doc values for nested objects. If you specify a nested object, the search returns an empty array (`[ ]`) for the field. To access nested fields, use the [`inner_hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) parameter’s `docvalue_fields` property.
::::



#### Stored fields [stored-fields]

It’s also possible to store an individual field’s values by using the [`store`](/reference/elasticsearch/mapping-reference/mapping-store.md) mapping option. You can use the `stored_fields` parameter to include these stored values in the search response.

::::{warning}
The `stored_fields` parameter is for fields that are explicitly marked as stored in the mapping, which is off by default and generally not recommended. Use [source filtering](#source-filtering) instead to select subsets of the original source document to be returned.
::::


Allows to selectively load specific stored fields for each document represented by a search hit.

```console
GET /_search
{
  "stored_fields" : ["user", "postDate"],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```

`*` can be used to load all stored fields from the document.

An empty array will cause only the `_id` and `_type` for each hit to be returned, for example:

```console
GET /_search
{
  "stored_fields" : [],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```

If the requested fields are not stored (`store` mapping set to `false`), they will be ignored.

Stored field values fetched from the document itself are always returned as an array. On the contrary, metadata fields like `_routing` are never returned as an array.

Also only leaf fields can be returned via the `stored_fields` option. If an object field is specified, it will be ignored.

::::{note}
On its own, `stored_fields` cannot be used to load fields in nested objects — if a field contains a nested object in its path, then no data will be returned for that stored field. To access nested fields, `stored_fields` must be used within an [`inner_hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) block.
::::



##### Disable stored fields [disable-stored-fields]

To disable the stored fields (and metadata fields) entirely use: `_none_`:

```console
GET /_search
{
  "stored_fields": "_none_",
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```

::::{note}
[`_source`](#source-filtering) and [`version`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) parameters cannot be activated if `_none_` is used.
::::



#### Script fields [script-fields]

You can use the `script_fields` parameter to retrieve a [script evaluation](docs-content://explore-analyze/scripting.md) (based on different fields) for each hit. For example:

```console
GET /_search
{
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "test1": {
      "script": {
        "lang": "painless",
        "source": "doc['price'].value * 2"
      }
    },
    "test2": {
      "script": {
        "lang": "painless",
        "source": "doc['price'].value * params.factor",
        "params": {
          "factor": 2.0
        }
      }
    }
  }
}
```

Script fields can work on fields that are not stored (`price` in the above case), and allow to return custom values to be returned (the evaluated value of the script).

Script fields can also access the actual `_source` document and extract specific elements to be returned from it by using `params['_source']`. Here is an example:

```console
GET /_search
{
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "test1": {
      "script": "params['_source']['message']"
    }
  }
}
```

Note the `_source` keyword here to navigate the json-like model.

It’s important to understand the difference between `doc['my_field'].value` and `params['_source']['my_field']`. The first, using the doc keyword, will cause the terms for that field to be loaded to memory (cached), which will result in faster execution, but more memory consumption. Also, the `doc[...]` notation only allows for simple valued fields (you can’t return a json object from it) and makes sense only for non-analyzed or single term based fields. However, using `doc` is still the recommended way to access values from the document, if at all possible, because `_source` must be loaded and parsed every time it’s used. Using `_source` is very slow.

