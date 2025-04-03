---
navigation_title: "Shape"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shape.html
---

# Shape field type [shape]


The `shape` data type facilitates the indexing of and searching with arbitrary `x, y` cartesian shapes such as rectangles and polygons. It can be used to index and query geometries whose coordinates fall in a 2-dimensional planar coordinate system.

You can query documents using this type using [shape Query](/reference/query-languages/query-dsl/query-dsl-shape-query.md).


## Mapping Options [shape-mapping-options]

Like the [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) field type, the `shape` field mapping maps [GeoJSON](http://geojson.org) or [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) (WKT) geometry objects to the shape type. To enable it, users must explicitly map fields to the shape type.

| Option | Description | Default |
| --- | --- | --- |
| `orientation` | Optionally define how to interpret vertex order forpolygons / multipolygons. This parameter defines one of two coordinatesystem rules (Right-hand or Left-hand) each of which can be specified in threedifferent ways. 1. Right-hand rule: `right`, `ccw`, `counterclockwise`,2. Left-hand rule: `left`, `cw`, `clockwise`. The default orientation(`counterclockwise`) complies with the OGC standard which definesouter ring vertices in counterclockwise order with inner ring(s) vertices (holes)in clockwise order. Setting this parameter in the `geo_shape` mapping explicitlysets vertex order for the coordinate list of a `geo_shape` field but can beoverridden in each individual GeoJSON or WKT document. | `ccw` |
| `ignore_malformed` | If true, malformed GeoJSON or WKT shapes are ignored. Iffalse (default), malformed GeoJSON and WKT shapes throw an exception and reject theentire document. | `false` |
| `ignore_z_value` | If `true` (default) three dimension points will be accepted (stored in source)but only latitude and longitude values will be indexed; the third dimension is ignored. If `false`,geopoints containing any more than latitude and longitude (two dimensions) values throw an exceptionand reject the whole document. | `true` |
| `coerce` | If `true` unclosed linear rings in polygons will be automatically closed. | `false` |


## Indexing approach [shape-indexing-approach]

Like `geo_shape`, the `shape` field type is indexed by decomposing geometries into a triangular mesh and indexing each triangle as a 7 dimension point in a BKD tree. The coordinates provided to the indexer are single precision floating point values so the field guarantees the same accuracy provided by the java virtual machine (typically `1E-38`). For polygons/multi-polygons the performance of the tessellator primarily depends on the number of vertices that define the geometry.

**IMPORTANT NOTES**

`CONTAINS` relation query - `shape` queries with `relation` defined as `contains` are supported for indices created with ElasticSearch 7.5.0 or higher.


### Example [_example_2]

```console
PUT /example
{
  "mappings": {
    "properties": {
      "geometry": {
        "type": "shape"
      }
    }
  }
}
```

This mapping definition maps the geometry field to the shape type. The indexer uses single precision floats for the vertex values so accuracy is guaranteed to the same precision as `float` values provided by the java virtual machine approximately (typically 1E-38).


## Input Structure [shape-input-structure]

Shapes can be represented using either the [GeoJSON](http://geojson.org) or [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html) (WKT) format. The following table provides a mapping of GeoJSON and WKT to Elasticsearch types:

| GeoJSON Type | WKT Type | Elasticsearch Type | Description |
| --- | --- | --- | --- |
| `Point` | `POINT` | `point` | A single `x, y` coordinate. |
| `LineString` | `LINESTRING` | `linestring` | An arbitrary line given two or more points. |
| `Polygon` | `POLYGON` | `polygon` | A *closed* polygon whose first and last pointmust match, thus requiring `n + 1` vertices to create an `n`-sidedpolygon and a minimum of `4` vertices. |
| `MultiPoint` | `MULTIPOINT` | `multipoint` | An array of unconnected, but likely relatedpoints. |
| `MultiLineString` | `MULTILINESTRING` | `multilinestring` | An array of separate linestrings. |
| `MultiPolygon` | `MULTIPOLYGON` | `multipolygon` | An array of separate polygons. |
| `GeometryCollection` | `GEOMETRYCOLLECTION` | `geometrycollection` | A shape collection similar to the`multi*` shapes except that multiple types can coexist (e.g., a Point and a LineString). |
| `N/A` | `BBOX` | `envelope` | A bounding rectangle, or envelope, specified byspecifying only the top left and bottom right points. |

::::{note}
For all types, both the inner `type` and `coordinates` fields are required.

In GeoJSON and WKT, and therefore Elasticsearch, the correct **coordinate order is (X, Y)** within coordinate arrays. This differs from many Geospatial APIs (e.g., `geo_shape`) that typically use the colloquial latitude, longitude (Y, X) ordering.

::::



### [Point](http://geojson.org/geojson-spec.html#id2) [point-shape]

A point is a single coordinate in cartesian `x, y` space. It may represent the location of an item of interest in a virtual world or projected space. The following is an example of a point in GeoJSON.

```console
POST /example/_doc
{
  "location" : {
    "type" : "point",
    "coordinates" : [-377.03653, 389.897676]
  }
}
```

The following is an example of a point in WKT:

```console
POST /example/_doc
{
  "location" : "POINT (-377.03653 389.897676)"
}
```


### [LineString](http://geojson.org/geojson-spec.html#id3) [linestring]

A `linestring` defined by an array of two or more positions. By specifying only two points, the `linestring` will represent a straight line. Specifying more than two points creates an arbitrary path. The following is an example of a LineString in GeoJSON.

```console
POST /example/_doc
{
  "location" : {
    "type" : "linestring",
    "coordinates" : [[-377.03653, 389.897676], [-377.009051, 389.889939]]
  }
}
```

The following is an example of a LineString in WKT:

```console
POST /example/_doc
{
  "location" : "LINESTRING (-377.03653 389.897676, -377.009051 389.889939)"
}
```


### [Polygon](http://geojson.org/geojson-spec.html#id4) [polygon]

A polygon is defined by a list of a list of points. The first and last points in each (outer) list must be the same (the polygon must be closed). The following is an example of a Polygon in GeoJSON.

```console
POST /example/_doc
{
  "location" : {
    "type" : "polygon",
    "coordinates" : [
      [ [1000.0, -1001.0], [1001.0, -1001.0], [1001.0, -1000.0], [1000.0, -1000.0], [1000.0, -1001.0] ]
    ]
  }
}
```

The following is an example of a Polygon in WKT:

```console
POST /example/_doc
{
  "location" : "POLYGON ((1000.0 -1001.0, 1001.0 -1001.0, 1001.0 -1000.0, 1000.0 -1000.0, 1000.0 -1001.0))"
}
```

The first array represents the outer boundary of the polygon, the other arrays represent the interior shapes ("holes"). The following is a GeoJSON example of a polygon with a hole:

```console
POST /example/_doc
{
  "location" : {
    "type" : "polygon",
    "coordinates" : [
      [ [1000.0, -1001.0], [1001.0, -1001.0], [1001.0, -1000.0], [1000.0, -1000.0], [1000.0, -1001.0] ],
      [ [1000.2, -1001.2], [1000.8, -1001.2], [1000.8, -1001.8], [1000.2, -1001.8], [1000.2, -1001.2] ]
    ]
  }
}
```

The following is an example of a Polygon with a hole in WKT:

```console
POST /example/_doc
{
  "location" : "POLYGON ((1000.0 1000.0, 1001.0 1000.0, 1001.0 1001.0, 1000.0 1001.0, 1000.0 1000.0), (1000.2 1000.2, 1000.8 1000.2, 1000.8 1000.8, 1000.2 1000.8, 1000.2 1000.2))"
}
```

**IMPORTANT NOTE:** WKT does not enforce a specific order for vertices. [GeoJSON](https://tools.ietf.org/html/rfc7946#section-3.1.6) mandates that the outer polygon must be counterclockwise and interior shapes must be clockwise, which agrees with the Open Geospatial Consortium (OGC) [Simple Feature Access](https://www.opengeospatial.org/standards/sfa) specification for vertex ordering.

By default Elasticsearch expects vertices in counterclockwise (right hand rule) order. If data is provided in clockwise order (left hand rule) the user can change the `orientation` parameter either in the field mapping, or as a parameter provided with the document.

The following is an example of overriding the `orientation` parameters on a document:

```console
POST /example/_doc
{
  "location" : {
    "type" : "polygon",
    "orientation" : "clockwise",
    "coordinates" : [
      [ [1000.0, 1000.0], [1000.0, 1001.0], [1001.0, 1001.0], [1001.0, 1000.0], [1000.0, 1000.0] ]
    ]
  }
}
```


### [MultiPoint](http://geojson.org/geojson-spec.html#id5) [multipoint]

The following is an example of a list of GeoJSON points:

```console
POST /example/_doc
{
  "location" : {
    "type" : "multipoint",
    "coordinates" : [
      [1002.0, 1002.0], [1003.0, 2000.0]
    ]
  }
}
```

The following is an example of a list of WKT points:

```console
POST /example/_doc
{
  "location" : "MULTIPOINT (1002.0 2000.0, 1003.0 2000.0)"
}
```


### [MultiLineString](http://geojson.org/geojson-spec.html#id6) [multilinestring]

The following is an example of a list of GeoJSON linestrings:

```console
POST /example/_doc
{
  "location" : {
    "type" : "multilinestring",
    "coordinates" : [
      [ [1002.0, 200.0], [1003.0, 200.0], [1003.0, 300.0], [1002.0, 300.0] ],
      [ [1000.0, 100.0], [1001.0, 100.0], [1001.0, 100.0], [1000.0, 100.0] ],
      [ [1000.2, 100.2], [1000.8, 100.2], [1000.8, 100.8], [1000.2, 100.8] ]
    ]
  }
}
```

The following is an example of a list of WKT linestrings:

```console
POST /example/_doc
{
  "location" : "MULTILINESTRING ((1002.0 200.0, 1003.0 200.0, 1003.0 300.0, 1002.0 300.0), (1000.0 100.0, 1001.0 100.0, 1001.0 100.0, 1000.0 100.0), (1000.2 0.2, 1000.8 100.2, 1000.8 100.8, 1000.2 100.8))"
}
```


### [MultiPolygon](http://geojson.org/geojson-spec.html#id7) [multipolygon]

The following is an example of a list of GeoJSON polygons (second polygon contains a hole):

```console
POST /example/_doc
{
  "location" : {
    "type" : "multipolygon",
    "coordinates" : [
      [ [[1002.0, 200.0], [1003.0, 200.0], [1003.0, 300.0], [1002.0, 300.0], [1002.0, 200.0]] ],
      [ [[1000.0, 200.0], [1001.0, 100.0], [1001.0, 100.0], [1000.0, 100.0], [1000.0, 100.0]],
        [[1000.2, 200.2], [1000.8, 100.2], [1000.8, 100.8], [1000.2, 100.8], [1000.2, 100.2]] ]
    ]
  }
}
```

The following is an example of a list of WKT polygons (second polygon contains a hole):

```console
POST /example/_doc
{
  "location" : "MULTIPOLYGON (((1002.0 200.0, 1003.0 200.0, 1003.0 300.0, 1002.0 300.0, 102.0 200.0)), ((1000.0 100.0, 1001.0 100.0, 1001.0 100.0, 1000.0 100.0, 1000.0 100.0), (1000.2 100.2, 1000.8 100.2, 1000.8 100.8, 1000.2 100.8, 1000.2 100.2)))"
}
```


### [Geometry Collection](http://geojson.org/geojson-spec.md#geometrycollection) [geometry_collection]

The following is an example of a collection of GeoJSON geometry objects:

```console
POST /example/_doc
{
  "location" : {
    "type": "geometrycollection",
    "geometries": [
      {
        "type": "point",
        "coordinates": [1000.0, 100.0]
      },
      {
        "type": "linestring",
        "coordinates": [ [1001.0, 100.0], [1002.0, 100.0] ]
      }
    ]
  }
}
```

The following is an example of a collection of WKT geometry objects:

```console
POST /example/_doc
{
  "location" : "GEOMETRYCOLLECTION (POINT (1000.0 100.0), LINESTRING (1001.0 100.0, 1002.0 100.0))"
}
```


### Envelope [_envelope_2]

Elasticsearch supports an `envelope` type, which consists of coordinates for upper left and lower right points of the shape to represent a bounding rectangle in the format `[[minX, maxY], [maxX, minY]]`:

```console
POST /example/_doc
{
  "location" : {
    "type" : "envelope",
    "coordinates" : [ [1000.0, 100.0], [1001.0, 100.0] ]
  }
}
```

The following is an example of an envelope using the WKT BBOX format:

**NOTE:** WKT specification expects the following order: minLon, maxLon, maxLat, minLat.

```console
POST /example/_doc
{
  "location" : "BBOX (1000.0, 1002.0, 2000.0, 1000.0)"
}
```


## Sorting and Retrieving index Shapes [_sorting_and_retrieving_index_shapes_2]

Due to the complex input structure and index representation of shapes, it is not currently possible to sort shapes or retrieve their fields directly. The `shape` value is only retrievable through the `_source` field.

