---
navigation_title: "Geo-distance"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-geodistance-aggregation.html
---

# Geo-distance aggregation [search-aggregations-bucket-geodistance-aggregation]


A multi-bucket aggregation that works on `geo_point` fields and conceptually works very similar to the [range](/reference/aggregations/search-aggregations-bucket-range-aggregation.md) aggregation. The user can define a point of origin and a set of distance range buckets. The aggregation evaluates the distance of each document value from the origin point and determines the buckets it belongs to based on the ranges (a document belongs to a bucket if the distance between the document and the origin falls within the distance range of the bucket).

$$$geodistance-aggregation-example$$$

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
  "aggs": {
    "rings_around_amsterdam": {
      "geo_distance": {
        "field": "location",
        "origin": "POINT (4.894 52.3760)",
        "ranges": [
          { "to": 100000 },
          { "from": 100000, "to": 300000 },
          { "from": 300000 }
        ]
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "rings_around_amsterdam": {
      "buckets": [
        {
          "key": "*-100000.0",
          "from": 0.0,
          "to": 100000.0,
          "doc_count": 3
        },
        {
          "key": "100000.0-300000.0",
          "from": 100000.0,
          "to": 300000.0,
          "doc_count": 1
        },
        {
          "key": "300000.0-*",
          "from": 300000.0,
          "doc_count": 2
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]

The specified field must be of type `geo_point` (which can only be set explicitly in the mappings). And it can also hold an array of `geo_point` fields, in which case all will be taken into account during aggregation. The origin point can accept all formats supported by the [`geo_point` type](/reference/elasticsearch/mapping-reference/geo-point.md):

* Object format: `{ "lat" : 52.3760, "lon" : 4.894 }` - this is the safest format as it is the most explicit about the `lat` & `lon` values
* String format: `"52.3760, 4.894"` - where the first number is the `lat` and the second is the `lon`
* Array format: `[4.894, 52.3760]` - which is based on the GeoJSON standard where the first number is the `lon` and the second one is the `lat`

By default, the distance unit is `m` (meters) but it can also accept: `mi` (miles), `in` (inches), `yd` (yards), `km` (kilometers), `cm` (centimeters), `mm` (millimeters).

```console
POST /museums/_search?size=0
{
  "aggs": {
    "rings": {
      "geo_distance": {
        "field": "location",
        "origin": "POINT (4.894 52.3760)",
        "unit": "km", <1>
        "ranges": [
          { "to": 100 },
          { "from": 100, "to": 300 },
          { "from": 300 }
        ]
      }
    }
  }
}
```
% TEST[continued]

1. The distances will be computed in kilometers


There are two distance calculation modes: `arc` (the default), and `plane`. The `arc` calculation is the most accurate. The `plane` is the fastest but least accurate. Consider using `plane` when your search context is "narrow", and spans smaller geographical areas (~5km). `plane` will return higher error margins for searches across very large areas (e.g. cross continent search). The distance calculation type can be set using the `distance_type` parameter:

```console
POST /museums/_search?size=0
{
  "aggs": {
    "rings": {
      "geo_distance": {
        "field": "location",
        "origin": "POINT (4.894 52.3760)",
        "unit": "km",
        "distance_type": "plane",
        "ranges": [
          { "to": 100 },
          { "from": 100, "to": 300 },
          { "from": 300 }
        ]
      }
    }
  }
}
```
% TEST[continued]

## Keyed Response [_keyed_response_2]

Setting the `keyed` flag to `true` will associate a unique string key with each bucket and return the ranges as a hash rather than an array:

```console
POST /museums/_search?size=0
{
  "aggs": {
    "rings_around_amsterdam": {
      "geo_distance": {
        "field": "location",
        "origin": "POINT (4.894 52.3760)",
        "ranges": [
          { "to": 100000 },
          { "from": 100000, "to": 300000 },
          { "from": 300000 }
        ],
        "keyed": true
      }
    }
  }
}
```
% TEST[continued]

Response:

```console-result
{
  ...
  "aggregations": {
    "rings_around_amsterdam": {
      "buckets": {
        "*-100000.0": {
          "from": 0.0,
          "to": 100000.0,
          "doc_count": 3
        },
        "100000.0-300000.0": {
          "from": 100000.0,
          "to": 300000.0,
          "doc_count": 1
        },
        "300000.0-*": {
          "from": 300000.0,
          "doc_count": 2
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]

It is also possible to customize the key for each range:

```console
POST /museums/_search?size=0
{
  "aggs": {
    "rings_around_amsterdam": {
      "geo_distance": {
        "field": "location",
        "origin": "POINT (4.894 52.3760)",
        "ranges": [
          { "to": 100000, "key": "first_ring" },
          { "from": 100000, "to": 300000, "key": "second_ring" },
          { "from": 300000, "key": "third_ring" }
        ],
        "keyed": true
      }
    }
  }
}
```
% TEST[continued]

Response:

```console-result
{
  ...
  "aggregations": {
    "rings_around_amsterdam": {
      "buckets": {
        "first_ring": {
          "from": 0.0,
          "to": 100000.0,
          "doc_count": 3
        },
        "second_ring": {
          "from": 100000.0,
          "to": 300000.0,
          "doc_count": 1
        },
        "third_ring": {
          "from": 300000.0,
          "doc_count": 2
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


