---
navigation_title: "Geoshape"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html
---

# Geoshape query [query-dsl-geo-shape-query]


Filter documents indexed using either the [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) or the [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) type.

The `geo_shape` query uses the same [index](/reference/elasticsearch/mapping-reference/geo-shape.md#geoshape-indexing-approach) as the `geo_shape` or `geo_point` mapping to find documents that have a shape that is related to the query shape, using a specified [spatial relationship](#geo-shape-spatial-relations): either intersects, contained, within or disjoint.

The query supports two ways of defining the query shape, either by providing a whole shape definition, or by referencing the name of a shape pre-indexed in another index. Both formats are defined below with examples.

## Inline shape definition [_inline_shape_definition]

Similar to the `geo_point` type, the `geo_shape` query uses [GeoJSON](http://geojson.org) to represent shapes.

Given the following index with locations as `geo_shape` fields:

```console
PUT /example
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_shape"
      }
    }
  }
}

POST /example/_doc?refresh
{
  "name": "Wind & Wetter, Berlin, Germany",
  "location": {
    "type": "point",
    "coordinates": [ 13.400544, 52.530286 ]
  }
}
```

The following query will find the point using {{es}}'s `envelope` GeoJSON extension:

```console
GET /example/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_shape": {
          "location": {
            "shape": {
              "type": "envelope",
              "coordinates": [ [ 13.0, 53.0 ], [ 14.0, 52.0 ] ]
            },
            "relation": "within"
          }
        }
      }
    }
  }
}
```

The above query can, similarly, be queried on `geo_point` fields.

```console
PUT /example_points
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}

PUT /example_points/_doc/1?refresh
{
  "name": "Wind & Wetter, Berlin, Germany",
  "location": [13.400544, 52.530286]
}
```

Using the same query, the documents with matching `geo_point` fields are returned.

```console
GET /example_points/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_shape": {
          "location": {
            "shape": {
              "type": "envelope",
              "coordinates": [ [ 13.0, 53.0 ], [ 14.0, 52.0 ] ]
            },
            "relation": "intersects"
          }
        }
      }
    }
  }
}
```

```console-result
{
  "took" : 17,
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
        "_index" : "example_points",
        "_id" : "1",
        "_score" : 1.0,
        "_source" : {
          "name": "Wind & Wetter, Berlin, Germany",
          "location": [13.400544, 52.530286]
        }
      }
    ]
  }
}
```


## Pre-indexed shape [_pre_indexed_shape]

The query also supports using a shape which has already been indexed in another index. This is particularly useful for when you have a pre-defined list of shapes and you want to reference the list using a logical name (for example *New Zealand*) rather than having to provide coordinates each time. In this situation, it is only necessary to provide:

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
      "location": {
        "type": "geo_shape"
      }
    }
  }
}

PUT /shapes/_doc/deu
{
  "location": {
    "type": "envelope",
    "coordinates" : [[13.0, 53.0], [14.0, 52.0]]
  }
}

GET /example/_search
{
  "query": {
    "bool": {
      "filter": {
        "geo_shape": {
          "location": {
            "indexed_shape": {
              "index": "shapes",
              "id": "deu",
              "path": "location"
            }
          }
        }
      }
    }
  }
}
```


## Spatial relations [geo-shape-spatial-relations]

The following is a complete list of spatial relation operators available when searching a geo field:

* `INTERSECTS` - (default) Return all documents whose `geo_shape` or `geo_point` field intersects the query geometry.
* `DISJOINT` - Return all documents whose `geo_shape` or `geo_point` field has nothing in common with the query geometry.
* `WITHIN` - Return all documents whose `geo_shape` or `geo_point` field is within the query geometry. Line geometries are not supported.
* `CONTAINS` - Return all documents whose `geo_shape` or `geo_point` field contains the query geometry.


### Ignore unmapped [_ignore_unmapped_4]

When set to `true` the `ignore_unmapped` option will ignore an unmapped field and will not match any documents for this query. This can be useful when querying multiple indexes which might have different mappings. When set to `false` (the default value) the query will throw an exception if the field is not mapped.


## Notes [geo-shape-query-notes]

* When data is indexed in a `geo_shape` field as an array of shapes, the arrays are treated as one shape. For this reason, the following requests are equivalent.

```console
PUT /test/_doc/1
{
  "location": [
    {
      "coordinates": [46.25,20.14],
      "type": "point"
    },
    {
      "coordinates": [47.49,19.04],
      "type": "point"
    }
  ]
}
```

```console
PUT /test/_doc/1
{
  "location":
    {
      "coordinates": [[46.25,20.14],[47.49,19.04]],
      "type": "multipoint"
    }
}
```

* The `geo_shape` query assumes `geo_shape` fields use a default `orientation` of `RIGHT` (counterclockwise). See [Polygon orientation](/reference/elasticsearch/mapping-reference/geo-shape.md#polygon-orientation).


