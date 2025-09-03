---
navigation_title: "Geo-polygon"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-polygon-query.html
---

# Geo-polygon query [query-dsl-geo-polygon-query]


::::{admonition} Deprecated in 7.12.
:class: warning

Use [Geoshape](/reference/query-languages/query-dsl/query-dsl-geo-shape-query.md) instead where polygons are defined in GeoJSON or [Well-Known Text (WKT)](http://docs.opengeospatial.org/is/18-010r7/18-010r7.html).
::::


A query returning hits that only fall within a polygon of points. Here is an example:

```console
GET /_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_polygon": {
          "person.location": {
            "points": [
              { "lat": 40, "lon": -70 },
              { "lat": 30, "lon": -80 },
              { "lat": 20, "lon": -90 }
            ]
          }
        }
      }
    }
  }
}
```


## Query options [_query_options_2]

| Option | Description |
| --- | --- |
| `_name` | Optional name field to identify the filter |
| `validation_method` | Set to `IGNORE_MALFORMED` to accept geo points withinvalid latitude or longitude, `COERCE` to try and infer correct latitudeor longitude, or `STRICT` (default is `STRICT`). |


## Allowed formats [_allowed_formats]


### Lat long as array [_lat_long_as_array]

Format as `[lon, lat]`

Note: the order of lon/lat here must conform with [GeoJSON](http://geojson.org/).

```console
GET /_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_polygon": {
          "person.location": {
            "points": [
              [ -70, 40 ],
              [ -80, 30 ],
              [ -90, 20 ]
            ]
          }
        }
      }
    }
  }
}
```


### Lat lon as string [_lat_lon_as_string_2]

Format in `lat,lon`.

```console
GET /_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_polygon": {
          "person.location": {
            "points": [
              "40, -70",
              "30, -80",
              "20, -90"
            ]
          }
        }
      }
    }
  }
}
```


### Geohash [_geohash_4]

```console
GET /_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_polygon": {
          "person.location": {
            "points": [
              "drn5x1g8cu2y",
              "30, -80",
              "20, -90"
            ]
          }
        }
      }
    }
  }
}
```


## `geo_point` type [_geo_point_type]

The query **requires** the [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) type to be set on the relevant field.


## Ignore unmapped [_ignore_unmapped_3]

When set to `true` the `ignore_unmapped` option will ignore an unmapped field and will not match any documents for this query. This can be useful when querying multiple indexes which might have different mappings. When set to `false` (the default value) the query will throw an exception if the field is not mapped.

