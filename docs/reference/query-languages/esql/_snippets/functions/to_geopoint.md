## `TO_GEOPOINT` [esql-to_geopoint]

**Syntax**

:::{image} ../../../../../images/to_geopoint.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a `geo_point` value. A string will only be successfully converted if it respects the [WKT Point](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format.

**Supported types**

| field | result |
| --- | --- |
| geo_point | geo_point |
| keyword | geo_point |
| text | geo_point |

**Example**

```esql
ROW wkt = "POINT(42.97109630194 14.7552534413725)"
| EVAL pt = TO_GEOPOINT(wkt)
```

| wkt:keyword | pt:geo_point |
| --- | --- |
| "POINT(42.97109630194 14.7552534413725)" | POINT(42.97109630194 14.7552534413725) |


