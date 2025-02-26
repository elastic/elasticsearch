## `TO_CARTESIANSHAPE` [esql-to_cartesianshape]

**Syntax**

:::{image} ../../../../../images/to_cartesianshape.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a `cartesian_shape` value. A string will only be successfully converted if it respects the [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format.

**Supported types**

| field | result |
| --- | --- |
| cartesian_point | cartesian_shape |
| cartesian_shape | cartesian_shape |
| keyword | cartesian_shape |
| text | cartesian_shape |

**Example**

```esql
ROW wkt = ["POINT(4297.11 -1475.53)", "POLYGON ((3339584.72 1118889.97, 4452779.63 4865942.27, 2226389.81 4865942.27, 1113194.90 2273030.92, 3339584.72 1118889.97))"]
| MV_EXPAND wkt
| EVAL geom = TO_CARTESIANSHAPE(wkt)
```

| wkt:keyword | geom:cartesian_shape |
| --- | --- |
| "POINT(4297.11 -1475.53)" | POINT(4297.11 -1475.53) |
| "POLYGON 3339584.72 1118889.97, 4452779.63 4865942.27, 2226389.81 4865942.27, 1113194.90 2273030.92, 3339584.72 1118889.97" | POLYGON 3339584.72 1118889.97, 4452779.63 4865942.27, 2226389.81 4865942.27, 1113194.90 2273030.92, 3339584.72 1118889.97 |


