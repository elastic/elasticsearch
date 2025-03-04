## `ST_WITHIN` [esql-st_within]

**Syntax**

:::{image} ../../../../../images/st_within.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the first geometry is within the second geometry. This is the inverse of the [ST_CONTAINS](../../esql-functions-operators.md#esql-st_contains) function.

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
| WHERE ST_WITHIN(city_boundary, TO_GEOSHAPE("POLYGON((109.1 18.15, 109.6 18.15, 109.6 18.65, 109.1 18.65, 109.1 18.15))"))
| KEEP abbrev, airport, region, city, city_location
```

| abbrev:keyword | airport:text | region:text | city:keyword | city_location:geo_point |
| --- | --- | --- | --- | --- |
| SYX | Sanya Phoenix Int’l | 天涯区 | Sanya | POINT(109.5036 18.2533) |


