---
navigation_title: "Geo-grid"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-grid-query.html
---

# Geo-grid query [query-dsl-geo-grid-query]


Matches [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) and [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) values that intersect a grid cell from a GeoGrid aggregation.

The query is designed to match the documents that fall inside a bucket of a geogrid aggregation by providing the key of the bucket. For geohash and geotile grids, the query can be used for geo_point and geo_shape fields. For geo_hex grid, it can only be used for geo_point fields.


### Example [geo-grid-query-ex]

Assume the following the following documents are indexed:

```console
PUT /my_locations
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}

PUT /my_locations/_doc/1?refresh
{
  "location" : "POINT(4.912350 52.374081)",
  "city": "Amsterdam",
  "name": "NEMO Science Museum"
}

PUT /my_locations/_doc/2?refresh
{
  "location" : "POINT(4.405200 51.222900)",
  "city": "Antwerp",
  "name": "Letterenhuis"
}

PUT /my_locations/_doc/3?refresh
{
  "location" : "POINT(2.336389 48.861111)",
  "city": "Paris",
  "name": "Musée du Louvre"
}
```
% TESTSETUP

## geohash grid [query-dsl-geo-grid-query-geohash]

Using a geohash_grid aggregation, it is possible to group documents depending on their geohash value:

```console
GET /my_locations/_search
{
  "size" : 0,
  "aggs" : {
     "grouped" : {
        "geohash_grid" : {
           "field" : "location",
           "precision" : 2
        }
     }
  }
}
```
% TESTRESPONSE[s/"took" : 10/"took" : $body.took/]

```console-result
{
  "took" : 10,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 3,
      "relation" : "eq"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "grouped" : {
      "buckets" : [
        {
          "key" : "u1",
          "doc_count" : 2
        },
        {
          "key" : "u0",
          "doc_count" : 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/"took" : 10/"took" : $body.took/]

We can extract the documents on one of those buckets by executing a geo_grid query using the bucket key with the following syntax:

```console
GET /my_locations/_search
{
  "query": {
    "geo_grid" :{
      "location" : {
        "geohash" : "u0"
      }
    }
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]

```console-result
{
  "took" : 1,
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
        "_index" : "my_locations",
        "_id" : "3",
        "_score" : 1.0,
        "_source" : {
          "location" : "POINT(2.336389 48.861111)",
          "city" : "Paris",
          "name" : "Musée du Louvre"
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]


## geotile grid [query-dsl-geo-grid-query-geotile]

Using a geotile_grid aggregation, it is possible to group documents depending on their geotile value:

```console
GET /my_locations/_search
{
  "size" : 0,
  "aggs" : {
     "grouped" : {
        "geotile_grid" : {
           "field" : "location",
           "precision" : 6
        }
     }
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]

```console-result
{
  "took" : 1,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 3,
      "relation" : "eq"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "grouped" : {
      "buckets" : [
        {
          "key" : "6/32/21",
          "doc_count" : 2
        },
        {
          "key" : "6/32/22",
          "doc_count" : 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]

We can extract the documents on one of those buckets by executing a geo_grid query using the bucket key with the following syntax:

```console
GET /my_locations/_search
{
  "query": {
    "geo_grid" :{
      "location" : {
        "geotile" : "6/32/22"
      }
    }
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]

```console-result
{
  "took" : 1,
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
        "_index" : "my_locations",
        "_id" : "3",
        "_score" : 1.0,
        "_source" : {
          "location" : "POINT(2.336389 48.861111)",
          "city" : "Paris",
          "name" : "Musée du Louvre"
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took" : 1/"took" : $body.took/]


## geohex grid [query-dsl-geo-grid-query-geohex]

Using a geohex_grid aggregation, it is possible to group documents depending on their geohex value:

```console
GET /my_locations/_search
{
  "size" : 0,
  "aggs" : {
     "grouped" : {
        "geohex_grid" : {
           "field" : "location",
           "precision" : 1
        }
     }
  }
}
```
% TESTRESPONSE[s/"took" : 2/"took" : $body.took/]

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
      "value" : 3,
      "relation" : "eq"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "grouped" : {
      "buckets" : [
        {
          "key" : "81197ffffffffff",
          "doc_count" : 2
        },
        {
          "key" : "811fbffffffffff",
          "doc_count" : 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/"took" : 2/"took" : $body.took/]

We can extract the documents on one of those buckets by executing a geo_grid query using the bucket key with the following syntax:

```console
GET /my_locations/_search
{
  "query": {
    "geo_grid" :{
      "location" : {
        "geohex" : "811fbffffffffff"
      }
    }
  }
}
```
% TESTRESPONSE[s/"took" : 26/"took" : $body.took/]

```console-result
{
  "took" : 26,
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
        "_index" : "my_locations",
        "_id" : "3",
        "_score" : 1.0,
        "_source" : {
          "location" : "POINT(2.336389 48.861111)",
          "city" : "Paris",
          "name" : "Musée du Louvre"
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took" : 26/"took" : $body.took/]


