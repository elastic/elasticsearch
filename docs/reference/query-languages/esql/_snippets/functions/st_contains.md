## `ST_CONTAINS` [esql-st_contains]

**Syntax**

:::{image} ../../../../../images/st_contains.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the first geometry contains the second geometry. This is the inverse of the [ST_WITHIN](../../esql-functions-operators.md#esql-st_within) function.

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
| WHERE ST_CONTAINS(city_boundary, TO_GEOSHAPE("POLYGON((109.35 18.3, 109.45 18.3, 109.45 18.4, 109.35 18.4, 109.35 18.3))"))
| KEEP abbrev, airport, region, city, city_location
```

| abbrev:keyword | airport:text | region:text | city:keyword | city_location:geo_point |
| --- | --- | --- | --- | --- |
| SYX | Sanya Phoenix Int’l | 天涯区 | Sanya | POINT(109.5036 18.2533) |


