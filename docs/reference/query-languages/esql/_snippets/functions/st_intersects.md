## `ST_INTERSECTS` [esql-st_intersects]

**Syntax**

:::{image} ../../../../../images/st_intersects.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns true if two geometries intersect. They intersect if they have any point in common, including their interior points (points along lines or within polygons). This is the inverse of the [ST_DISJOINT](../../esql-functions-operators.md#esql-st_disjoint) function. In mathematical terms: ST_Intersects(A, B) ⇔ A ⋂ B ≠ ∅

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
FROM airports
| WHERE ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
```

| abbrev:keyword | city:keyword | city_location:geo_point | country:keyword | location:geo_point | name:text | scalerank:i | type:k |
| --- | --- | --- | --- | --- | --- | --- | --- |
| HOD | Al Ḩudaydah | POINT(42.9511 14.8022) | Yemen | POINT(42.97109630194 14.7552534413725) | Hodeidah Int’l | 9 | mid |


