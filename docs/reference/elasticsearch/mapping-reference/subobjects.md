---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/subobjects.html
---

# subobjects [subobjects]

When indexing a document or updating mappings, Elasticsearch accepts fields that contain dots in their names, which get expanded to their corresponding object structure. For instance, the  field `metrics.time.max` is mapped as a `max` leaf field with a parent `time` object, belonging to its parent `metrics` object.

The described default behaviour is reasonable for most scenarios, but causes problems in certain situations where for instance a field `metrics.time` holds a value too, which is common when indexing metrics data. A document holding a value for both `metrics.time.max` and `metrics.time` gets rejected given that `time` would need to be a leaf field to hold a value as well as an object to hold the `max` sub-field.

The `subobjects` setting, which can be applied only to the top-level mapping definition and to [`object`](/reference/elasticsearch/mapping-reference/object.md) fields, disables the ability for an object to hold further subobjects and makes it possible to store documents where field names contain dots and share common prefixes. From the example above, if the object container `metrics` has `subobjects` set to `false`, it can hold values for both `time` and `time.max` directly without the need for any intermediate object, as dots in field names are preserved.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "metrics": {
        "type":  "object",
        "subobjects": false, <1>
        "properties": {
          "time": { "type": "long" },
          "time.min": { "type": "long" },
          "time.max": { "type": "long" }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/metric_1
{
  "metrics.time" : 100, <2>
  "metrics.time.min" : 10,
  "metrics.time.max" : 900
}

PUT my-index-000001/_doc/metric_2
{
  "metrics" : {
    "time" : 100, <3>
    "time.min" : 10,
    "time.max" : 900
  }
}

GET my-index-000001/_mapping
```

1. The `metrics` field cannot hold other objects.
2. Sample document holding flat paths
3. Sample document holding an object (configured to not hold subobjects) and its leaf sub-fields

```console-result
{
  "my-index-000001" : {
    "mappings" : {
      "properties" : {
        "metrics" : {
          "subobjects" : false,
          "properties" : {
            "time" : {
              "type" : "long"
            },
            "time.min" : { <1>
              "type" : "long"
            },
            "time.max" : {
              "type" : "long"
            }
          }
        }
      }
    }
  }
}
```
1. The resulting mapping where dots in field names were preserved


The entire mapping may be configured to not support subobjects as well, in which case the document can only ever hold leaf sub-fields:

```console
PUT my-index-000001
{
  "mappings": {
    "subobjects": false <1>
  }
}

PUT my-index-000001/_doc/metric_1
{
  "time" : "100ms", <2>
  "time.min" : "10ms",
  "time.max" : "900ms"
}
```

1. The entire mapping is configured to not support objects.
2. The document does not support objects


The `subobjects` setting for existing fields and the top-level mapping definition cannot be updated.

## Auto-flattening object mappings [subobjects-auto-flattening]

It is generally recommended to define the properties of an object that is configured with `subobjects: false` with dotted field names (as shown in the first example). However, it is also possible to define these properties as sub-objects in the mappings. In that case, the mapping will be automatically flattened before it is stored. This makes it easier to re-use existing mappings without having to re-write them.

Note that auto-flattening will not work when certain [mapping parameters](/reference/elasticsearch/mapping-reference/mapping-parameters.md) are set on object mappings that are defined under an object configured with `subobjects: false`:

* The [`enabled`](/reference/elasticsearch/mapping-reference/enabled.md) mapping parameter must not be `false`.
* The [`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md) mapping parameter must not contradict the implicit or explicit value of the parent. For example, when `dynamic` is set to `false` in the root of the mapping, object mappers that set `dynamic` to `true` can’t be auto-flattened.
* The `subobjects` mapping parameter must not be set to `true` explicitly.

```console
PUT my-index-000002
{
  "mappings": {
    "properties": {
      "metrics": {
        "subobjects": false,
        "properties": {
          "time": {
            "type": "object", <1>
            "properties": {
              "min": { "type": "long" }, <2>
              "max": { "type": "long" }
            }
          }
        }
      }
    }
  }
}
GET my-index-000002/_mapping
```
1. The metrics object can contain further object mappings that will be auto-flattened. Object mappings at this level must not set certain mapping parameters as explained above.
2. This field will be auto-flattened to `"time.min"` before the mapping is stored.

```console-result
{
  "my-index-000002" : {
    "mappings" : {
      "properties" : {
        "metrics" : {
          "subobjects" : false,
          "properties" : {
            "time.min" : { <1>
              "type" : "long"
            },
            "time.max" : {
              "type" : "long"
            }
          }
        }
      }
    }
  }
}
```
1. The auto-flattened `"time.min"` field can be inspected by looking at the index mapping.



