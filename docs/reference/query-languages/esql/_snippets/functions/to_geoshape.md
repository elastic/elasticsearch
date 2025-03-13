## `TO_GEOSHAPE` [esql-to_geoshape]

**Syntax**

:::{image} ../../../../../images/to_geoshape.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a `geo_shape` value. A string will only be successfully converted if it respects the [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format.

**Supported types**

| field | result |
| --- | --- |
| geo_point | geo_shape |
| geo_shape | geo_shape |
| keyword | geo_shape |
| text | geo_shape |

**Example**

```esql
ROW wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
| EVAL geom = TO_GEOSHAPE(wkt)
```

| wkt:keyword | geom:geo_shape |
| --- | --- |
| "POLYGON 30 10, 40 40, 20 40, 10 20, 30 10" | POLYGON 30 10, 40 40, 20 40, 10 20, 30 10 |


