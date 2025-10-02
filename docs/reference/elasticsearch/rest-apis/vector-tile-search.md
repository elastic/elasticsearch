---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/search-vector-tile-api.html#search-vector-tile-api-api-example
applies_to:
  stack: all
navigation_title: Vector tile search API
---

# Vector tile search API examples

This page shows how to create an index with geospatial data and retrieve vector tile results using the [vector tile search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-mvt-1). 


You can learn how to:
- [Create an index with geospatial fields](#create-an-index-with-geospatial-fields)
- [Query geospatial data using a vector tile](#query-a-vector-tile-for-geospatial-data)
- [Understand the structure of the API response](#example-response)
- [Understand how the request is internally translated](#how-elasticsearch-translates-the-request-internally)

## Create an index with geospatial fields

The following requests create the `museum` index and add several geospatial
`location` values.

```console
PUT museums
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      },
      "name": {
        "type": "keyword"
      },
      "price": {
        "type": "long"
      },
      "included": {
        "type": "boolean"
      }
    }
  }
}

POST museums/_bulk?refresh
{ "index": { "_id": "1" } }
{ "location": "POINT (4.912350 52.374081)", "name": "NEMO Science Museum",  "price": 1750, "included": true }
{ "index": { "_id": "2" } }
{ "location": "POINT (4.901618 52.369219)", "name": "Museum Het Rembrandthuis", "price": 1500, "included": false }
{ "index": { "_id": "3" } }
{ "location": "POINT (4.914722 52.371667)", "name": "Nederlands Scheepvaartmuseum", "price":1650, "included": true }
{ "index": { "_id": "4" } }
{ "location": "POINT (4.914722 52.371667)", "name": "Amsterdam Centre for Architecture", "price":0, "included": true }
```

## Query a vector tile for geospatial data

The following request searches the index for `location` values that intersect
the `13/4207/2692` vector tile.

```console
GET museums/_mvt/location/13/4207/2692
{
  "grid_agg": "geotile",
  "grid_precision": 2,
  "fields": [
    "name",
    "price"
  ],
  "query": {
    "term": {
      "included": true
    }
  },
  "aggs": {
    "min_price": {
      "min": {
        "field": "price"
      }
    },
    "max_price": {
      "max": {
        "field": "price"
      }
    },
    "avg_price": {
      "avg": {
        "field": "price"
      }
    }
  }
}
```
% TEST[continued]

## Example response

The API returns results as a binary vector tile. When decoded into JSON, the
tile contains the following data:

```js
{
  "hits": {
    "extent": 4096,
    "version": 2,
    "features": [
      {
        "geometry": {
          "type": "Point",
          "coordinates": [
            3208,
            3864
          ]
        },
        "properties": {
          "_id": "1",
          "_index": "museums",
          "name": "NEMO Science Museum",
          "price": 1750
        },
        "type": 1
      },
      {
        "geometry": {
          "type": "Point",
          "coordinates": [
            3429,
            3496
          ]
        },
        "properties": {
          "_id": "3",
          "_index": "museums",
          "name": "Nederlands Scheepvaartmuseum",
          "price": 1650
        },
        "type": 1
      },
      {
        "geometry": {
          "type": "Point",
          "coordinates": [
            3429,
            3496
          ]
        },
        "properties": {
          "_id": "4",
          "_index": "museums",
          "name": "Amsterdam Centre for Architecture",
          "price": 0
        },
        "type": 1
      }
    ]
  },
  "aggs": {
    "extent": 4096,
    "version": 2,
    "features": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              [
                3072,
                3072
              ],
              [
                4096,
                3072
              ],
              [
                4096,
                4096
              ],
              [
                3072,
                4096
              ],
              [
                3072,
                3072
              ]
            ]
          ]
        },
        "properties": {
          "_count": 3,
          "max_price.value": 1750.0,
          "min_price.value": 0.0,
          "avg_price.value": 1133.3333333333333
        },
        "type": 3
      }
    ]
  },
  "meta": {
    "extent": 4096,
    "version": 2,
    "features": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              [
                0,
                0
              ],
              [
                4096,
                0
              ],
              [
                4096,
                4096
              ],
              [
                0,
                4096
              ],
              [
                0,
                0
              ]
            ]
          ]
        },
        "properties": {
          "_shards.failed": 0,
          "_shards.skipped": 0,
          "_shards.successful": 1,
          "_shards.total": 1,
          "aggregations._count.avg": 3.0,
          "aggregations._count.count": 1,
          "aggregations._count.max": 3.0,
          "aggregations._count.min": 3.0,
          "aggregations._count.sum": 3.0,
          "aggregations.avg_price.avg": 1133.3333333333333,
          "aggregations.avg_price.count": 1,
          "aggregations.avg_price.max": 1133.3333333333333,
          "aggregations.avg_price.min": 1133.3333333333333,
          "aggregations.avg_price.sum": 1133.3333333333333,
          "aggregations.max_price.avg": 1750.0,
          "aggregations.max_price.count": 1,
          "aggregations.max_price.max": 1750.0,
          "aggregations.max_price.min": 1750.0,
          "aggregations.max_price.sum": 1750.0,
          "aggregations.min_price.avg": 0.0,
          "aggregations.min_price.count": 1,
          "aggregations.min_price.max": 0.0,
          "aggregations.min_price.min": 0.0,
          "aggregations.min_price.sum": 0.0,
          "hits.max_score": 0.0,
          "hits.total.relation": "eq",
          "hits.total.value": 3,
          "timed_out": false,
          "took": 2
        },
        "type": 3
      }
    ]
  }
}
```
% NOTCONSOLE

## How Elasticsearch translates the request internally

{{es}} may translate a vector tile search API request with a
`grid_agg` argument of `geotile` and an `exact_bounds` argument of `true`
into the following search:

```console
GET my-index/_search
{
  "size": 10000,
  "query": {
    "geo_bounding_box": {
      "my-geo-field": {
        "top_left": {
          "lat": -40.979898069620134,
          "lon": -45
        },
        "bottom_right": {
          "lat": -66.51326044311186,
          "lon": 0
        }
      }
    }
  },
  "aggregations": {
    "grid": {
      "geotile_grid": {
        "field": "my-geo-field",
        "precision": 11,
        "size": 65536,
        "bounds": {
          "top_left": {
            "lat": -40.979898069620134,
            "lon": -45
          },
          "bottom_right": {
            "lat": -66.51326044311186,
            "lon": 0
          }
        }
      }
    },
    "bounds": {
      "geo_bounds": {
        "field": "my-geo-field",
        "wrap_longitude": false
      }
    }
  }
}
```
% TEST[continued]

