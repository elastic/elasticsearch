## `ST_XMAX` [esql-st_xmax]

**Syntax**

:::{image} ../../../../../images/st_xmax.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. If `null`, the function returns `null`.

**Description**

Extracts the maximum value of the `x` coordinates from the supplied geometry. If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the maximum `longitude` value.

**Supported types**

| point | result |
| --- | --- |
| cartesian_point | double |
| cartesian_shape | double |
| geo_point | double |
| geo_shape | double |

**Example**

```esql
FROM airport_city_boundaries
| WHERE abbrev == "CPH"
| EVAL envelope = ST_ENVELOPE(city_boundary)
| EVAL xmin = ST_XMIN(envelope), xmax = ST_XMAX(envelope), ymin = ST_YMIN(envelope), ymax = ST_YMAX(envelope)
| KEEP abbrev, airport, xmin, xmax, ymin, ymax
```

| abbrev:keyword | airport:text | xmin:double | xmax:double | ymin:double | ymax:double |
| --- | --- | --- | --- | --- | --- |
| CPH | Copenhagen | 12.453 | 12.6398 | 55.6318 | 55.7327 |


