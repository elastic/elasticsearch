## `TO_CARTESIANPOINT` [esql-to_cartesianpoint]

**Syntax**

:::{image} ../../../../../images/to_cartesianpoint.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a `cartesian_point` value. A string will only be successfully converted if it respects the [WKT Point](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format.

**Supported types**

| field | result |
| --- | --- |
| cartesian_point | cartesian_point |
| keyword | cartesian_point |
| text | cartesian_point |

**Example**

```esql
ROW wkt = ["POINT(4297.11 -1475.53)", "POINT(7580.93 2272.77)"]
| MV_EXPAND wkt
| EVAL pt = TO_CARTESIANPOINT(wkt)
```

| wkt:keyword | pt:cartesian_point |
| --- | --- |
| "POINT(4297.11 -1475.53)" | POINT(4297.11 -1475.53) |
| "POINT(7580.93 2272.77)" | POINT(7580.93 2272.77) |


