---
navigation_title: "Geopoint"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html
---

# Geopoint field type [geo-point]


Fields of type `geo_point` accept latitude-longitude pairs, which can be used:

* to find geopoints within a [bounding box](/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query.md),
  within a certain [distance](/reference/query-languages/query-dsl/query-dsl-geo-distance-query.md) of a central point,
  or within a [`geo_shape` query](/reference/query-languages/query-dsl/query-dsl-geo-shape-query.md) (for example, points in a polygon).
* to aggregate documents by [distance](/reference/aggregations/search-aggregations-bucket-geodistance-aggregation.md) from a central point.
* to aggregate documents by geographic grids: either
  [`geo_hash`](/reference/aggregations/search-aggregations-bucket-geohashgrid-aggregation.md),
  [`geo_tile`](/reference/aggregations/search-aggregations-bucket-geotilegrid-aggregation.md) or
  [`geo_hex`](/reference/aggregations/search-aggregations-bucket-geohexgrid-aggregation.md).
* to aggregate geopoints into a track using the metrics aggregation
  [`geo_line`](/reference/aggregations/search-aggregations-metrics-geo-line.md).
* to integrate distance into a document’s [relevance score](/reference/query-languages/query-dsl/query-dsl-function-score-query.md).
* to [sort](/reference/elasticsearch/rest-apis/sort-search-results.md#geo-sorting) documents by distance.

As with [geo_shape](/reference/elasticsearch/mapping-reference/geo-shape.md) and [point](/reference/elasticsearch/mapping-reference/point.md), `geo_point` can be specified in [GeoJSON](http://geojson.org)
and [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) formats.
However, there are a number of additional formats that are supported for convenience and historical reasons.
In total there are six ways that a geopoint may be specified, as demonstrated below:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "text": "Geopoint as an object using GeoJSON format",
  "location": { <1>
    "type": "Point",
    "coordinates": [-71.34, 41.12]
  }
}

PUT my-index-000001/_doc/2
{
  "text": "Geopoint as a WKT POINT primitive",
  "location" : "POINT (-71.34 41.12)" <2>
}

PUT my-index-000001/_doc/3
{
  "text": "Geopoint as an object with 'lat' and 'lon' keys",
  "location": { <3>
    "lat": 41.12,
    "lon": -71.34
  }
}

PUT my-index-000001/_doc/4
{
  "text": "Geopoint as an array",
  "location": [ -71.34, 41.12 ] <4>
}

PUT my-index-000001/_doc/5
{
  "text": "Geopoint as a string",
  "location": "41.12,-71.34" <5>
}

PUT my-index-000001/_doc/6
{
  "text": "Geopoint as a geohash",
  "location": "drm3btev3e86" <6>
}

GET my-index-000001/_search
{
  "query": {
    "geo_bounding_box": { <7>
      "location": {
        "top_left": {
          "lat": 42,
          "lon": -72
        },
        "bottom_right": {
          "lat": 40,
          "lon": -74
        }
      }
    }
  }
}
```

1. Geopoint expressed as an object, in [GeoJSON](https://geojson.org/) format, with `type` and `coordinates` keys.
2. Geopoint expressed as a [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) POINT with the format: `"POINT(lon lat)"`
3. Geopoint expressed as an object, with `lat` and `lon` keys.
4. Geopoint expressed as an array with the format: [ `lon`, `lat`]
5. Geopoint expressed as a string with the format: `"lat,lon"`.
6. Geopoint expressed as a geohash.
7. A geo-bounding box query which finds all geopoints that fall inside the box.


::::{admonition} Geopoints expressed as an array or string
:class: important

Please note that string geopoints are ordered as `lat,lon`, while array
geopoints, GeoJSON and WKT are ordered as the reverse: `lon,lat`.

The reasons for this are historical. Geographers traditionally write `latitude`
before `longitude`, while recent formats specified for geographic data like
[GeoJSON](https://geojson.org/) and [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html)
order `longitude` before `latitude` (easting before northing) in order to match
the mathematical convention of ordering `x` before `y`.

::::


::::{note}
A point can be expressed as a [geohash](https://en.wikipedia.org/wiki/Geohash).
Geohashes are [base32](https://en.wikipedia.org/wiki/Base32) encoded strings of
the bits of the latitude and longitude interleaved. Each character in a geohash
adds additional 5 bits to the precision. So the longer the hash, the more
precise it is. For the indexing purposed geohashs are translated into
latitude-longitude pairs. During this process only first 12 characters are
used, so specifying more than 12 characters in a geohash doesn’t increase the
precision. The 12 characters provide 60 bits, which should reduce a possible
error to less than 2cm.
::::


## Parameters for `geo_point` fields [geo-point-params]

The following parameters are accepted by `geo_point` fields:

[`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md)
:   If `true`, malformed geopoints are ignored.
    If `false` (default), malformed geopoints throw an exception and reject the whole document.
    A geopoint is considered malformed if its latitude is outside the range -90 ⇐ latitude ⇐ 90,
    or if its longitude is outside the range -180 ⇐ longitude ⇐ 180.
    When set to `true`, if the format is valid, but the values are out of range,
    the values will be normalized into the valid range, and the document will be indexed.
    This is a special case, and a [different behaviour](/reference/elasticsearch/mapping-reference/ignore-malformed.md#_ignore_malformed_geo_point) from the normal for `ignore_malformed`.
    Note that this cannot be set if the `script` parameter is used.

`ignore_z_value`
:   If `true` (default) three dimension points will be accepted (stored in source)
    but only latitude and longitude values will be indexed; the third dimension is
    ignored. If `false`, geopoints containing any more than latitude and longitude
    (two dimensions) values throw an exception and reject the whole document. Note
    that this cannot be set if the `script` parameter is used.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be quickly searchable? Accepts `true` (default) and
    `false`. Fields that only have [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
    enabled can still be queried, albeit slower.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a geopoint value which is substituted for any explicit `null` values.
    Defaults to `null`, which means the field is treated as missing. Note that this
    cannot be set if the `script` parameter is used.

`on_script_error`
:   Defines what to do if the script defined by the `script` parameter
    throws an error at indexing time. Accepts `fail` (default), which
    will cause the entire document to be rejected, and `continue`, which
    will register the field in the document’s [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) metadata field and continue
    indexing. This parameter can only be set if the `script` field is
    also set.

`script`
:   If this parameter is set, then the field will index values generated
    by this script, rather than reading the values directly from the
    source. If a value is set for this field on the input document, then
    the document will be rejected with an error.
    Scripts are in the same format as their [runtime equivalent](docs-content://manage-data/data-store/mapping/map-runtime-field.md), and should emit points
    as a pair of (lat, lon) double values.


## Using geopoints in scripts [_using_geopoints_in_scripts]

When accessing the value of a geopoint in a script, the value is returned as
a `GeoPoint` object, which allows access to the `.lat` and `.lon` values
respectively:

```painless
def geopoint = doc['location'].value;
def lat      = geopoint.lat;
def lon      = geopoint.lon;
```

For performance reasons, it is better to access the lat/lon values directly:

```painless
def lat      = doc['location'].lat;
def lon      = doc['location'].lon;
```


## Synthetic source [geo-point-synthetic-source]

Synthetic source may sort `geo_point` fields (first by latitude and then
longitude) and reduces them to their stored precision. For example:

$$$synthetic-source-geo-point-example$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "point": { "type": "geo_point" }
    }
  }
}
PUT idx/_doc/1
{
  "point": [
    {"lat":-90, "lon":-80},
    {"lat":10, "lon":30}
  ]
}
```

Will become:

```console-result
{
  "point": [
    {"lat":-90.0, "lon":-80.00000000931323},
    {"lat":9.999999990686774, "lon":29.999999972060323}
   ]
}
```


