---
navigation_title: "Pass-through object"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/passthrough.html
---

# Pass-through object field type [passthrough]


Pass-through objects extend the functionality of [objects](/reference/elasticsearch/mapping-reference/object.md) by allowing to access their subfields without including the name of the pass-through object as prefix. For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "attributes": {
        "type": "passthrough", <1>
        "priority": 10,
        "properties": {
          "id": {
            "type": "keyword"
          }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "attributes" : {  <2>
    "id": "foo",
    "zone": 10
  }
}

GET my-index-000001/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "id": "foo" }},  <3>
        { "match": { "zone": 10 }}
      ]
    }
  }
}

GET my-index-000001/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "attributes.id": "foo" }}, <4>
        { "match": { "attributes.zone": 10 }}
      ]
    }
  }
}
```

1. An object is defined as pass-through. Its priority (required) is used for conflict resolution.
2. Object contents get indexed as usual, including dynamic mappings.
3. Sub-fields can be referenced in queries as if they’re defined at the root level.
4. Sub-fields can also be referenced including the object name as prefix.


## Conflict resolution [passthrough-conflicts]

It’s possible for conflicting names to arise, for fields that are defined within different scopes:

1. A pass-through object is defined next to a field that has the same name as one of the pass-through object sub-fields, e.g.

    ```console
    PUT my-index-000001/_doc/1
    {
      "attributes" : {
        "id": "foo"
      },
      "id": "bar"
    }
    ```

    In this case, references to `id` point to the field at the root level, while field `attributes.id` can only be accessed using the full path.

2. Two (or more) pass-through objects are defined within the same object and contain fields with the same name, e.g.

    ```console
    PUT my-index-000002
    {
      "mappings": {
        "properties": {
          "attributes": {
            "type": "passthrough",
            "priority": 10,
            "properties": {
              "id": {
                "type": "keyword"
              }
            }
          },
          "resource.attributes": {
            "type": "passthrough",
            "priority": 20,
            "properties": {
              "id": {
                "type": "keyword"
              }
            }
          }
        }
      }
    }
    ```

    In this case, param `priority` is used for conflict resolution, with the higher values taking precedence. In the example above, `resource.attributes` has higher priority than `attributes`, so references to `id` point to the field within `resource.attributes`. `attributes.id` can still be accessed using its full path.



## Defining sub-fields as time-series dimensions [passthrough-dimensions]

It is possible to configure a pass-through field as a container for  [time-series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension). In this case, all sub-fields get annotated with the same parameter under the covers, and they’re also included in [routing path](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#dimension-based-routing) and [tsid](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsid) calculations, thus simplifying the [TSDS](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) setup:

```console
PUT _index_template/my-metrics
{
  "index_patterns": ["metrics-mymetrics-*"],
  "priority": 200,
  "data_stream": { },
  "template": {
    "settings": {
      "index.mode": "time_series"
    },
    "mappings": {
      "properties": {
        "attributes": {
          "type": "passthrough",
          "priority": 10,
          "time_series_dimension": true,
          "properties": {
            "host.name": {
              "type": "keyword"
            }
          }
        },
        "cpu": {
          "type": "integer",
          "time_series_metric": "counter"
        }
      }
    }
  }
}

POST metrics-mymetrics-test/_doc
{
  "@timestamp": "2020-01-01T00:00:00.000Z",
  "attributes" : {
    "host.name": "foo",
    "zone": "bar"
  },
  "cpu": 10
}
```

In the example above, `attributes` is defined as a dimension container. Its sub-fields `host.name` (static) and `zone` (dynamic) get included in the routing path and tsid, and can be referenced in queries without the `attributes.` prefix.


## Sub-field auto-flattening [passthrough-flattening]

Pass-through fields apply [auto-flattening](/reference/elasticsearch/mapping-reference/subobjects.md#subobjects-auto-flattening) to sub-fields by default, to reduce dynamic mapping conflicts. As a consequence, no sub-object definitions are allowed within pass-through fields.


## Parameters for `passthrough` fields [passthrough-params]

The following parameters are accepted by `passthrough` fields:

[`priority`](#passthrough-conflicts)
:   (Required) used for naming conflict resolution between pass-through fields. The field with the highest value wins. Accepts non-negative integer values.

[`time_series_dimension`](#passthrough-dimensions)
:   Whether or not to treat sub-fields as [time-series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension). Accepts `false` (default) or `true`.

[`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md)
:   Whether or not new `properties` should be added dynamically to an existing object. Accepts `true` (default), `runtime`, `false` and `strict`.

[`enabled`](/reference/elasticsearch/mapping-reference/enabled.md)
:   Whether the JSON value given for the object field should be parsed and indexed (`true`, default) or completely ignored (`false`).

[`properties`](/reference/elasticsearch/mapping-reference/properties.md)
:   The fields within the object, which can be of any [data type](/reference/elasticsearch/mapping-reference/field-data-types.md), including `object`. New properties may be added to an existing object.

::::{important}
If you need to index arrays of objects instead of single objects, read [Nested](/reference/elasticsearch/mapping-reference/nested.md) first.
::::



