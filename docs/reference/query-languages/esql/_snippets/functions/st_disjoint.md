## `ST_DISJOINT` [esql-st_disjoint]

**Syntax**

:::{image} ../../../../../images/st_disjoint.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the two geometries or geometry columns are disjoint. This is the inverse of the [ST_INTERSECTS](../../esql-functions-operators.md#esql-st_intersects) function. In mathematical terms: ST_Disjoint(A, B) ⇔ A ⋂ B = ∅

**Supported types**

| geomA | geomB | result |
| --- | --- | --- |
| cartesian_point | cartesian_point | boolean |
| cartesian_point | cartesian_shape | boolean |
| cartesian_shape | cartesian_point | boolean |
| cartesian_shape | cartesian_shape | boolean |
| geo_point | geo_point | boolean |
| geo_point | geo_shape | boolean |
| geo_shape | geo_point | boolean |
| geo_shape | geo_shape | boolean |

**Example**

```esql
FROM airport_city_boundaries
| WHERE ST_DISJOINT(city_boundary, TO_GEOSHAPE("POLYGON((-10 -60, 120 -60, 120 60, -10 60, -10 -60))"))
| KEEP abbrev, airport, region, city, city_location
```

| abbrev:keyword | airport:text | region:text | city:keyword | city_location:geo_point |
| --- | --- | --- | --- | --- |
| ACA | General Juan N Alvarez Int’l | Acapulco de Juárez | Acapulco de Juárez | POINT (-99.8825 16.8636) |


