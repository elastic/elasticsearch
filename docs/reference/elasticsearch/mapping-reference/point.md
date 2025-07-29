---
navigation_title: "Point"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/point.html
---

# Point field type [point]


The `point` data type facilitates the indexing of and searching arbitrary `x, y` pairs that fall in a 2-dimensional planar coordinate system.

You can query documents using this type using [shape Query](/reference/query-languages/query-dsl/query-dsl-shape-query.md).

As with [geo_shape](/reference/elasticsearch/mapping-reference/geo-shape.md) and [geo_point](/reference/elasticsearch/mapping-reference/geo-point.md), `point` can be specified in [GeoJSON](http://geojson.org) and [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) formats. However, there are a number of additional formats that are supported for convenience and historical reasons. In total there are five ways that a cartesian point may be specified, as demonstrated below:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "location": {
        "type": "point"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "text": "Point as an object using GeoJSON format",
  "location": { <1>
    "type": "Point",
    "coordinates": [-71.34, 41.12]
  }
}

PUT my-index-000001/_doc/2
{
  "text": "Point as a WKT POINT primitive",
  "location" : "POINT (-71.34 41.12)" <2>
}

PUT my-index-000001/_doc/3
{
  "text": "Point as an object with 'x' and 'y' keys",
  "location": { <3>
    "x": -71.34,
    "y": 41.12
  }
}

PUT my-index-000001/_doc/4
{
  "text": "Point as an array",
  "location": [ -71.34, 41.12 ] <4>
}

PUT my-index-000001/_doc/5
{
  "text": "Point as a string",
  "location": "-71.34,41.12" <5>
}
```

1. Point expressed as an object, in [GeoJSON](https://geojson.org/) format, with `type` and `coordinates` keys.
2. Point expressed as a [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) POINT with the format: `"POINT(x y)"`
3. Point expressed as an object, with `x` and `y` keys.
4. Point expressed as an array with the format: [ `x`, `y`]
5. Point expressed as a string with the format: `"x,y"`.


::::{note}
Unlike the case with the [geo-point](/reference/elasticsearch/mapping-reference/geo-point.md) field type, the order of the coordinates `x` and `y` is the same for all formats above.
::::


The coordinates provided to the indexer are single precision floating point values so the field guarantees the same accuracy provided by the java virtual machine (typically `1E-38`).

## Parameters for `point` fields [point-params]

The following parameters are accepted by `point` fields:

[`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md)
:   If `true`, malformed points are ignored. If `false` (default), malformed points throw an exception and reject the whole document.

`ignore_z_value`
:   If `true` (default) three dimension points will be accepted (stored in source) but only x and y values will be indexed; the third dimension is ignored. If `false`, points containing any more than x and y (two dimensions) values throw an exception and reject the whole document.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts an point value which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing.


## Sorting and retrieving points [_sorting_and_retrieving_points]

It is currently not possible to sort points or retrieve their fields directly. The `point` value is only retrievable through the `_source` field.


