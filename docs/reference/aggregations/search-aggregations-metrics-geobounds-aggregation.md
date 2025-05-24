---
navigation_title: "Geo-bounds"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-geobounds-aggregation.html
---

# Geo-bounds aggregation [search-aggregations-metrics-geobounds-aggregation]


A metric aggregation that computes the geographic bounding box containing all values for a [Geopoint](/reference/elasticsearch/mapping-reference/geo-point.md) or [Geoshape](/reference/elasticsearch/mapping-reference/geo-shape.md) field.

Example:

```console
PUT /museums
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}

POST /museums/_bulk?refresh
{"index":{"_id":1}}
{"location": "POINT (4.912350 52.374081)", "name": "NEMO Science Museum"}
{"index":{"_id":2}}
{"location": "POINT (4.901618 52.369219)", "name": "Museum Het Rembrandthuis"}
{"index":{"_id":3}}
{"location": "POINT (4.914722 52.371667)", "name": "Nederlands Scheepvaartmuseum"}
{"index":{"_id":4}}
{"location": "POINT (4.405200 51.222900)", "name": "Letterenhuis"}
{"index":{"_id":5}}
{"location": "POINT (2.336389 48.861111)", "name": "Musée du Louvre"}
{"index":{"_id":6}}
{"location": "POINT (2.327000 48.860000)", "name": "Musée d'Orsay"}

POST /museums/_search?size=0
{
  "query": {
    "match": { "name": "musée" }
  },
  "aggs": {
    "viewport": {
      "geo_bounds": {
        "field": "location",    <1>
        "wrap_longitude": true  <2>
      }
    }
  }
}
```

1. The `geo_bounds` aggregation specifies the field to use to obtain the bounds, which must be a [Geopoint](/reference/elasticsearch/mapping-reference/geo-point.md) or a [Geoshape](/reference/elasticsearch/mapping-reference/geo-shape.md) type.
2. $$$geo-bounds-wrap-longitude$$$ `wrap_longitude` is an optional parameter which specifies whether the bounding box should be allowed to overlap the international date line. The default value is `true`.


The above aggregation demonstrates how one would compute the bounding box of the location field for all documents with a name matching "musée".

The response for the above aggregation:

```console-result
{
  ...
  "aggregations": {
    "viewport": {
      "bounds": {
        "top_left": {
          "lat": 48.86111099738628,
          "lon": 2.3269999679178
        },
        "bottom_right": {
          "lat": 48.85999997612089,
          "lon": 2.3363889567553997
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


## Geo Bounds Aggregation on `geo_shape` fields [geobounds-aggregation-geo-shape]

The Geo Bounds Aggregation is also supported on `geo_shape` fields.

If [`wrap_longitude`](#geo-bounds-wrap-longitude) is set to `true` (the default), the bounding box can overlap the international date line and return a bounds where the `top_left` longitude is larger than the `top_right` longitude.

For example, the upper right longitude will typically be greater than the lower left longitude of a geographic bounding box. However, when the area crosses the 180° meridian, the value of the lower left longitude will be greater than the value of the upper right longitude. See [Geographic bounding box](http://docs.opengeospatial.org/is/12-063r5/12-063r5.md#30) on the Open Geospatial Consortium website for more information.

Example:

```console
PUT /places
{
  "mappings": {
    "properties": {
      "geometry": {
        "type": "geo_shape"
      }
    }
  }
}

POST /places/_bulk?refresh
{"index":{"_id":1}}
{"name": "NEMO Science Museum", "geometry": "POINT(4.912350 52.374081)" }
{"index":{"_id":2}}
{"name": "Sportpark De Weeren", "geometry": { "type": "Polygon", "coordinates": [ [ [ 4.965305328369141, 52.39347642069457 ], [ 4.966979026794433, 52.391721758934835 ], [ 4.969425201416015, 52.39238958618537 ], [ 4.967944622039794, 52.39420969150824 ], [ 4.965305328369141, 52.39347642069457 ] ] ] } }

POST /places/_search?size=0
{
  "aggs": {
    "viewport": {
      "geo_bounds": {
        "field": "geometry"
      }
    }
  }
}
```
% TEST

```console-result
{
  ...
  "aggregations": {
    "viewport": {
      "bounds": {
        "top_left": {
          "lat": 52.39420966710895,
          "lon": 4.912349972873926
        },
        "bottom_right": {
          "lat": 52.374080987647176,
          "lon": 4.969425117596984
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]

