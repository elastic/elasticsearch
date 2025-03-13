## `ST_ENVELOPE` [esql-st_envelope]

**Syntax**

:::{image} ../../../../../images/st_envelope.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geometry`
:   Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. If `null`, the function returns `null`.

**Description**

Determines the minimum bounding box of the supplied geometry.

**Supported types**

| geometry | result |
| --- | --- |
| cartesian_point | cartesian_shape |
| cartesian_shape | cartesian_shape |
| geo_point | geo_shape |
| geo_shape | geo_shape |

**Example**

```esql
FROM airport_city_boundaries
| WHERE abbrev == "CPH"
| EVAL envelope = ST_ENVELOPE(city_boundary)
| KEEP abbrev, airport, envelope
```

| abbrev:keyword | airport:text | envelope:geo_shape |
| --- | --- | --- |
| CPH | Copenhagen | BBOX(12.453, 12.6398, 55.7327, 55.6318) |


