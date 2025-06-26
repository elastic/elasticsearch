---
navigation_title: "Cartesian-bounds"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cartesian-bounds-aggregation.html
---

# Cartesian-bounds aggregation [search-aggregations-metrics-cartesian-bounds-aggregation]


A metric aggregation that computes the spatial bounding box containing all values for a [Point](/reference/elasticsearch/mapping-reference/point.md) or [Shape](/reference/elasticsearch/mapping-reference/shape.md) field.

Example:

```console
PUT /museums
{
  "mappings": {
    "properties": {
      "location": {
        "type": "point"
      }
    }
  }
}

POST /museums/_bulk?refresh
{"index":{"_id":1}}
{"location": "POINT (491.2350 5237.4081)", "city": "Amsterdam", "name": "NEMO Science Museum"}
{"index":{"_id":2}}
{"location": "POINT (490.1618 5236.9219)", "city": "Amsterdam", "name": "Museum Het Rembrandthuis"}
{"index":{"_id":3}}
{"location": "POINT (491.4722 5237.1667)", "city": "Amsterdam", "name": "Nederlands Scheepvaartmuseum"}
{"index":{"_id":4}}
{"location": "POINT (440.5200 5122.2900)", "city": "Antwerp", "name": "Letterenhuis"}
{"index":{"_id":5}}
{"location": "POINT (233.6389 4886.1111)", "city": "Paris", "name": "Musée du Louvre"}
{"index":{"_id":6}}
{"location": "POINT (232.7000 4886.0000)", "city": "Paris", "name": "Musée d'Orsay"}

POST /museums/_search?size=0
{
  "query": {
    "match": { "name": "musée" }
  },
  "aggs": {
    "viewport": {
      "cartesian_bounds": {
        "field": "location"    <1>
      }
    }
  }
}
```

1. The `cartesian_bounds` aggregation specifies the field to use to obtain the bounds, which must be a [Point](/reference/elasticsearch/mapping-reference/point.md) or a [Shape](/reference/elasticsearch/mapping-reference/shape.md) type.


::::{note}
Unlike the case with the [`geo_bounds`](/reference/aggregations/search-aggregations-metrics-geobounds-aggregation.md#geobounds-aggregation-geo-shape) aggregation, there is no option to set [`wrap_longitude`](/reference/aggregations/search-aggregations-metrics-geobounds-aggregation.md#geo-bounds-wrap-longitude). This is because the cartesian space is euclidean and does not wrap back on itself. So the bounds will always have a minimum x value less than or equal to the maximum x value.
::::


The above aggregation demonstrates how one would compute the bounding box of the location field for all documents with a name matching "musée".

The response for the above aggregation:

```console-result
{
  ...
  "aggregations": {
    "viewport": {
      "bounds": {
        "top_left": {
          "x": 232.6999969482422,
          "y": 4886.111328125
        },
        "bottom_right": {
          "x": 233.63890075683594,
          "y": 4886.0
        }
      }
    }
  }
}
```


## Cartesian Bounds Aggregation on `shape` fields [cartesian-bounds-aggregation-shape]

The Cartesian Bounds Aggregation is also supported on `cartesian_shape` fields.

Example:

```console
PUT /places
{
  "mappings": {
    "properties": {
      "geometry": {
        "type": "shape"
      }
    }
  }
}

POST /places/_bulk?refresh
{"index":{"_id":1}}
{"name": "NEMO Science Museum", "geometry": "POINT(491.2350 5237.4081)" }
{"index":{"_id":2}}
{"name": "Sportpark De Weeren", "geometry": { "type": "Polygon", "coordinates": [ [ [ 496.5305328369141, 5239.347642069457 ], [ 496.6979026794433, 5239.1721758934835 ], [ 496.9425201416015, 5239.238958618537 ], [ 496.7944622039794, 5239.420969150824 ], [ 496.5305328369141, 5239.347642069457 ] ] ] } }

POST /places/_search?size=0
{
  "aggs": {
    "viewport": {
      "cartesian_bounds": {
        "field": "geometry"
      }
    }
  }
}
```

```console-result
{
  ...
  "aggregations": {
    "viewport": {
      "bounds": {
        "top_left": {
          "x": 491.2349853515625,
          "y": 5239.4208984375
        },
        "bottom_right": {
          "x": 496.9425048828125,
          "y": 5237.408203125
        }
      }
    }
  }
}
```

