---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-geo.html
---

# Geo functions [sql-functions-geo]

::::{warning}
This functionality is in beta and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.
::::


The geo functions work with geometries stored in `geo_point`, `geo_shape` and `shape` fields, or returned by other geo functions.

## Limitations [_limitations_4]

[`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md), [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) and [`shape`](/reference/elasticsearch/mapping-reference/shape.md) and types are represented in SQL as geometry and can be used interchangeably with the following exceptions:

* `geo_shape` and `shape` fields don’t have doc values, therefore these fields cannot be used for filtering, grouping or sorting.
* `geo_points` fields are indexed and have doc values by default, however only latitude and longitude are stored and indexed with some loss of precision from the original values (4.190951585769653E-8 for the latitude and 8.381903171539307E-8 for longitude). The altitude component is accepted but not stored in doc values nor indexed. Therefore calling `ST_Z` function in the filtering, grouping or sorting will return `null`.


## Geometry Conversion [_geometry_conversion]

### `ST_AsWKT` [sql-functions-geo-st-as-wkt]

```sql
ST_AsWKT(
    geometry <1>
)
```

**Input**:

1. geometry. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns the WKT representation of the `geometry`.

```sql
SELECT city, ST_AsWKT(location) location FROM "geo" WHERE city = 'Amsterdam';

     city:s    |     location:s
Amsterdam      |POINT (4.850312 52.347557)
```


### `ST_WKTToSQL` [sql-functions-geo-st-wkt-to-sql]

```sql
ST_WKTToSQL(
    string <1>
)
```

**Input**:

1. string WKT representation of geometry. If `null`, the function returns `null`.


**Output**: geometry

**Description**: Returns the geometry from WKT representation.

```sql
SELECT CAST(ST_WKTToSQL('POINT (10 20)') AS STRING) location;

   location:s
POINT (10.0 20.0)
```



## Geometry Properties [_geometry_properties]

### `ST_GeometryType` [sql-functions-geo-st-geometrytype]

```sql
ST_GeometryType(
    geometry <1>
)
```

**Input**:

1. geometry. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns the type of the `geometry` such as POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON, GEOMETRYCOLLECTION, ENVELOPE or CIRCLE.

```sql
SELECT ST_GeometryType(ST_WKTToSQL('POINT (10 20)')) type;

      type:s
POINT
```


### `ST_X` [sql-functions-geo-st-x]

```sql
ST_X(
    geometry <1>
)
```

**Input**:

1. geometry. If `null`, the function returns `null`.


**Output**: double

**Description**: Returns the longitude of the first point in the geometry.

```sql
SELECT ST_X(ST_WKTToSQL('POINT (10 20)')) x;

      x:d
10.0
```


### `ST_Y` [sql-functions-geo-st-y]

```sql
ST_Y(
    geometry <1>
)
```

**Input**:

1. geometry. If `null`, the function returns `null`.


**Output**: double

**Description**: Returns the latitude of the first point in the geometry.

```sql
SELECT ST_Y(ST_WKTToSQL('POINT (10 20)')) y;

      y:d
20.0
```


### `ST_Z` [sql-functions-geo-st-z]

```sql
ST_Z(
    geometry <1>
)
```

**Input**:

1. geometry. If `null`, the function returns `null`.


**Output**: double

**Description**: Returns the altitude of the first point in the geometry.

```sql
SELECT ST_Z(ST_WKTToSQL('POINT (10 20 30)')) z;

      z:d
30.0
```


### `ST_Distance` [sql-functions-geo-st-distance]

```sql
ST_Distance(
    geometry, <1>
    geometry  <2>
)
```

**Input**:

1. source geometry. If `null`, the function returns `null`.
2. target geometry. If `null`, the function returns `null`.


**Output**: Double

**Description**: Returns the distance between geometries in meters. Both geometries have to be points.

```sql
SELECT ST_Distance(ST_WKTToSQL('POINT (10 20)'), ST_WKTToSQL('POINT (20 30)')) distance;

   distance:d
1499101.2889383635
```



