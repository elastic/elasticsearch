---
navigation_title: "Shape"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-shape-query.html
---

# Shape query [query-dsl-shape-query]


Queries documents that contain fields indexed using the `shape` type.

Requires the [`shape` Mapping](/reference/elasticsearch/mapping-reference/shape.md).

The query supports two ways of defining the target shape, either by providing a whole shape definition, or by referencing the name, or id, of a shape pre-indexed in another index. Both formats are defined below with examples.

## Inline Shape Definition [_inline_shape_definition_2]

Similar to the `geo_shape` query, the `shape` query uses [GeoJSON](http://geojson.org) or [Well Known Text](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) (WKT) to represent shapes.

Given the following index:

```console
PUT /example
{
  "mappings": {
    "properties": {
      "geometry": {
        "type": "shape"
      }
    }
  }
}

PUT /example/_doc/1?refresh=wait_for
{
  "name": "Lucky Landing",
  "geometry": {
    "type": "point",
    "coordinates": [ 1355.400544, 5255.530286 ]
  }
}
```

The following query will find the point using the Elasticsearch’s `envelope` GeoJSON extension:

```console
GET /example/_search
{
  "query": {
    "shape": {
      "geometry": {
        "shape": {
          "type": "envelope",
          "coordinates": [ [ 1355.0, 5355.0 ], [ 1400.0, 5200.0 ] ]
        },
        "relation": "within"
      }
    }
  }
}
```


## Pre-Indexed Shape [_pre_indexed_shape_2]

The Query also supports using a shape which has already been indexed in another index. This is particularly useful for when you have a pre-defined list of shapes which are useful to your application and you want to reference this using a logical name (for example *New Zealand*) rather than having to provide their coordinates each time. In this situation it is only necessary to provide:

* `id` - The ID of the document that containing the pre-indexed shape.
* `index` - Name of the index where the pre-indexed shape is. Defaults to *shapes*.
* `path` - The field specified as path containing the pre-indexed shape. Defaults to *shape*.
* `routing` - The routing of the shape document if required.

The following is an example of using the Filter with a pre-indexed shape:

```console
PUT /shapes
{
  "mappings": {
    "properties": {
      "geometry": {
        "type": "shape"
      }
    }
  }
}

PUT /shapes/_doc/footprint
{
  "geometry": {
    "type": "envelope",
    "coordinates": [ [ 1355.0, 5355.0 ], [ 1400.0, 5200.0 ] ]
  }
}

GET /example/_search
{
  "query": {
    "shape": {
      "geometry": {
        "indexed_shape": {
          "index": "shapes",
          "id": "footprint",
          "path": "geometry"
        }
      }
    }
  }
}
```


## Spatial Relations [_spatial_relations]

The following is a complete list of spatial relation operators available:

* `INTERSECTS` - (default) Return all documents whose `shape` field intersects the query geometry.
* `DISJOINT` - Return all documents whose `shape` field has nothing in common with the query geometry.
* `WITHIN` - Return all documents whose `shape` field is within the query geometry.
* `CONTAINS` - Return all documents whose `shape` field contains the query geometry.


### Ignore Unmapped [_ignore_unmapped_5]

When set to `true` the `ignore_unmapped` option will ignore an unmapped field and will not match any documents for this query. This can be useful when querying multiple indexes which might have different mappings. When set to `false` (the default value) the query will throw an exception if the field is not mapped.


