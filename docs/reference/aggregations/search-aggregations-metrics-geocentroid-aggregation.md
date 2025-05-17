---
navigation_title: "Geo-centroid"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-geocentroid-aggregation.html
---

# Geo-centroid aggregation [search-aggregations-metrics-geocentroid-aggregation]


A metric aggregation that computes the weighted [centroid](https://en.wikipedia.org/wiki/Centroid) from all coordinate values for geo fields.

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
{"location": "POINT (4.912350 52.374081)", "city": "Amsterdam", "name": "NEMO Science Museum"}
{"index":{"_id":2}}
{"location": "POINT (4.901618 52.369219)", "city": "Amsterdam", "name": "Museum Het Rembrandthuis"}
{"index":{"_id":3}}
{"location": "POINT (4.914722 52.371667)", "city": "Amsterdam", "name": "Nederlands Scheepvaartmuseum"}
{"index":{"_id":4}}
{"location": "POINT (4.405200 51.222900)", "city": "Antwerp", "name": "Letterenhuis"}
{"index":{"_id":5}}
{"location": "POINT (2.336389 48.861111)", "city": "Paris", "name": "Musée du Louvre"}
{"index":{"_id":6}}
{"location": "POINT (2.327000 48.860000)", "city": "Paris", "name": "Musée d'Orsay"}

POST /museums/_search?size=0
{
  "aggs": {
    "centroid": {
      "geo_centroid": {
        "field": "location" <1>
      }
    }
  }
}
```

1. The `geo_centroid` aggregation specifies the field to use for computing the centroid. (NOTE: field must be a [Geopoint](/reference/elasticsearch/mapping-reference/geo-point.md) type)


The above aggregation demonstrates how one would compute the centroid of the location field for all museums' documents.

The response for the above aggregation:

```console-result
{
  ...
  "aggregations": {
    "centroid": {
      "location": {
        "lat": 51.00982965203002,
        "lon": 3.9662131341174245
      },
      "count": 6
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]

The `geo_centroid` aggregation is more interesting when combined as a sub-aggregation to other bucket aggregations.

Example:

```console
POST /museums/_search?size=0
{
  "aggs": {
    "cities": {
      "terms": { "field": "city.keyword" },
      "aggs": {
        "centroid": {
          "geo_centroid": { "field": "location" }
        }
      }
    }
  }
}
```
% TEST[continued]

The above example uses `geo_centroid` as a sub-aggregation to a [terms](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md) bucket aggregation for finding the central location for museums in each city.

The response for the above aggregation:

```console-result
{
  ...
  "aggregations": {
    "cities": {
      "sum_other_doc_count": 0,
      "doc_count_error_upper_bound": 0,
      "buckets": [
        {
          "key": "Amsterdam",
          "doc_count": 3,
          "centroid": {
            "location": {
              "lat": 52.371655656024814,
              "lon": 4.909563297405839
            },
            "count": 3
          }
        },
        {
          "key": "Paris",
          "doc_count": 2,
          "centroid": {
            "location": {
              "lat": 48.86055548675358,
              "lon": 2.3316944623366
            },
            "count": 2
          }
        },
        {
          "key": "Antwerp",
          "doc_count": 1,
          "centroid": {
            "location": {
              "lat": 51.22289997059852,
              "lon": 4.40519998781383
            },
            "count": 1
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


## Geo Centroid Aggregation on `geo_shape` fields [geocentroid-aggregation-geo-shape]

The centroid metric for geoshapes is more nuanced than for points. The centroid of a specific aggregation bucket containing shapes is the centroid of the highest-dimensionality shape type in the bucket. For example, if a bucket contains shapes comprising of polygons and lines, then the lines do not contribute to the centroid metric. Each type of shape’s centroid is calculated differently. Envelopes and circles ingested via the [Circle](/reference/enrich-processor/ingest-circle-processor.md) are treated as polygons.

| Geometry Type | Centroid Calculation |
| --- | --- |
| [Multi]Point | equally weighted average of all the coordinates |
| [Multi]LineString | a weighted average of all the centroids of each segment, where the weight of each segment is its length in degrees |
| [Multi]Polygon | a weighted average of all the centroids of all the triangles of a polygon where the triangles are formed by every two consecutive vertices and the starting-point. holes have negative weights. weights represent the area of the triangle in deg^2 calculated |
| GeometryCollection | The centroid of all the underlying geometries with the highest dimension. If Polygons and Lines and/or Points, then lines and/or points are ignored. If Lines and Points, then points are ignored |

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
    "centroid": {
      "geo_centroid": {
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
    "centroid": {
      "location": {
        "lat": 52.39296147599816,
        "lon": 4.967404240742326
      },
      "count": 2
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]

::::{admonition} Using geo_centroid as a sub-aggregation of geohash_grid
:class: warning

The [`geohash_grid`](/reference/aggregations/search-aggregations-bucket-geohashgrid-aggregation.md) aggregation places documents, not individual geopoints, into buckets. If a document’s `geo_point` field contains [multiple values](/reference/elasticsearch/mapping-reference/array.md), the document could be assigned to multiple buckets, even if one or more of its geopoints are outside the bucket boundaries.

If a `geocentroid` sub-aggregation is also used, each centroid is calculated using all geopoints in a bucket, including those outside the bucket boundaries. This can result in centroids outside of bucket boundaries.

::::


