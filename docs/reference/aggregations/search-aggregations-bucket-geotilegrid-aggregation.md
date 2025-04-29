---
navigation_title: "Geotile grid"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-geotilegrid-aggregation.html
---

# Geotile grid aggregation [search-aggregations-bucket-geotilegrid-aggregation]


A multi-bucket aggregation that groups [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) and [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) values into buckets that represent a grid. The resulting grid can be sparse and only contains cells that have matching data. Each cell corresponds to a [map tile](https://en.wikipedia.org/wiki/Tiled_web_map) as used by many online map sites. Each cell is labeled using a `"{{zoom}}/{x}/{{y}}"` format, where zoom is equal to the user-specified precision.

* High precision keys have a larger range for x and y, and represent tiles that cover only a small area.
* Low precision keys have a smaller range for x and y, and represent tiles that each cover a large area.

See [zoom level documentation](https://wiki.openstreetmap.org/wiki/Zoom_levels) on how precision (zoom) correlates to size on the ground. Precision for this aggregation can be between 0 and 29, inclusive.

::::{warning}
The highest-precision geotile of length 29 produces cells that cover less than a 10cm by 10cm of land and so high-precision requests can be very costly in terms of RAM and result sizes. Please see the example below on how to first filter the aggregation to a smaller geographic area before requesting high-levels of detail.
::::


You can only use `geotile_grid` to aggregate an explicitly mapped `geo_point` or `geo_shape` field. If the `geo_point` field contains an array, `geotile_grid` aggregates all the array values.

## Simple low-precision request [_simple_low_precision_request_2]

$$$geotilegrid-aggregation-example$$$

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
  "aggregations": {
    "large-grid": {
      "geotile_grid": {
        "field": "location",
        "precision": 8
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
    "large-grid": {
      "buckets": [
        {
          "key": "8/131/84",
          "doc_count": 3
        },
        {
          "key": "8/129/88",
          "doc_count": 2
        },
        {
          "key": "8/131/85",
          "doc_count": 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


## High-precision requests [geotilegrid-high-precision]

When requesting detailed buckets (typically for displaying a "zoomed in" map), a filter like [geo_bounding_box](/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query.md) should be applied to narrow the subject area. Otherwise, potentially millions of buckets will be created and returned.

$$$geotilegrid-high-precision-ex$$$

```console
POST /museums/_search?size=0
{
  "aggregations": {
    "zoomed-in": {
      "filter": {
        "geo_bounding_box": {
          "location": {
            "top_left": "POINT (4.9 52.4)",
            "bottom_right": "POINT (5.0 52.3)"
          }
        }
      },
      "aggregations": {
        "zoom1": {
          "geotile_grid": {
            "field": "location",
            "precision": 22
          }
        }
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
    "zoomed-in": {
      "doc_count": 3,
      "zoom1": {
        "buckets": [
          {
            "key": "22/2154412/1378379",
            "doc_count": 1
          },
          {
            "key": "22/2154385/1378332",
            "doc_count": 1
          },
          {
            "key": "22/2154259/1378425",
            "doc_count": 1
          }
        ]
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


## Requests with additional bounding box filtering [geotilegrid-addtl-bounding-box-filtering]

The `geotile_grid` aggregation supports an optional `bounds` parameter that restricts the cells considered to those that intersect the provided bounds. The `bounds` parameter accepts the same [bounding box formats](/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query.md#query-dsl-geo-bounding-box-query-accepted-formats) as the geo-bounding box query. This bounding box can be used with or without an additional `geo_bounding_box` query for filtering the points prior to aggregating. It is an independent bounding box that can intersect with, be equal to, or be disjoint to any additional `geo_bounding_box` queries defined in the context of the aggregation.

$$$geotilegrid-aggregation-with-bounds$$$

```console
POST /museums/_search?size=0
{
  "aggregations": {
    "tiles-in-bounds": {
      "geotile_grid": {
        "field": "location",
        "precision": 22,
        "bounds": {
          "top_left": "POINT (4.9 52.4)",
          "bottom_right": "POINT (5.0 52.3)"
        }
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
    "tiles-in-bounds": {
      "buckets": [
        {
          "key": "22/2154412/1378379",
          "doc_count": 1
        },
        {
          "key": "22/2154385/1378332",
          "doc_count": 1
        },
        {
          "key": "22/2154259/1378425",
          "doc_count": 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"_shards": $body._shards,"hits":$body.hits,"timed_out":false,/]


### Aggregating `geo_shape` fields [geotilegrid-aggregating-geo-shape]

Aggregating on [Geoshape](/reference/elasticsearch/mapping-reference/geo-shape.md) fields works almost as it does for points, except that a single shape can be counted for in multiple tiles. A shape will contribute to the count of matching values if any part of its shape intersects with that tile. Below is an image that demonstrates this:

![geoshape grid](images/geoshape_grid.png "")


## Options [_options_5]

field
:   (Required, string) Field containing indexed geo-point or geo-shape values. Must be explicitly mapped as a [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) or a [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) field. If the field contains an array, `geotile_grid` aggregates all array values.

precision
:   (Optional, integer) Integer zoom of the key used to define cells/buckets in the results. Defaults to `7`. Values outside of [`0`,`29`] will be rejected.

bounds
:   (Optional, object) Bounding box used to filter the geo-points or geo-shapes in each bucket. Accepts the same bounding box formats as the [geo-bounding box query](/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query.md#query-dsl-geo-bounding-box-query-accepted-formats).

size
:   (Optional, integer) Maximum number of buckets to return. Defaults to 10,000. When results are trimmed, buckets are prioritized based on the volume of documents they contain.

shard_size
:   (Optional, integer) Number of buckets returned from each shard. Defaults to `max(10,(size x number-of-shards))` to allow for a more accurate count of the top cells in the final result. Since each shard could have a different top result order, using a larger number here reduces the risk of inaccurate counts, but incurs a performance cost.


