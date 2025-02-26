## {{esql}} spatial functions [esql-spatial-functions]

{{esql}} supports these spatial functions:

:::{include} lists/spatial-functions.md
:::


## `ST_DISTANCE` [esql-st_distance]

**Syntax**

:::{image} ../../../../images/st_distance.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_point` and `cartesian_point` parameters.

**Description**

Computes the distance between two points. For cartesian geometries, this is the pythagorean distance in the same units as the original coordinates. For geographic geometries, this is the circular distance along the great circle in meters.

**Supported types**

| geomA | geomB | result |
| --- | --- | --- |
| cartesian_point | cartesian_point | double |
| geo_point | geo_point | double |

**Example**

```esql
FROM airports
| WHERE abbrev == "CPH"
| EVAL distance = ST_DISTANCE(location, city_location)
| KEEP abbrev, name, location, city_location, distance
```

| abbrev:k | name:text | location:geo_point | city_location:geo_point | distance:d |
| --- | --- | --- | --- | --- |
| CPH | Copenhagen | POINT(12.6493508684508 55.6285017221528) | POINT(12.5683 55.6761) | 7339.573896618216 |


## `ST_INTERSECTS` [esql-st_intersects]

**Syntax**

:::{image} ../../../../images/st_intersects.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns true if two geometries intersect. They intersect if they have any point in common, including their interior points (points along lines or within polygons). This is the inverse of the [ST_DISJOINT](../esql-functions-operators.md#esql-st_disjoint) function. In mathematical terms: ST_Intersects(A, B) ⇔ A ⋂ B ≠ ∅

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


## `ST_DISJOINT` [esql-st_disjoint]

**Syntax**

:::{image} ../../../../images/st_disjoint.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the two geometries or geometry columns are disjoint. This is the inverse of the [ST_INTERSECTS](../esql-functions-operators.md#esql-st_intersects) function. In mathematical terms: ST_Disjoint(A, B) ⇔ A ⋂ B = ∅

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


## `ST_CONTAINS` [esql-st_contains]

**Syntax**

:::{image} ../../../../images/st_contains.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the first geometry contains the second geometry. This is the inverse of the [ST_WITHIN](../esql-functions-operators.md#esql-st_within) function.

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


## `ST_WITHIN` [esql-st_within]

**Syntax**

:::{image} ../../../../images/st_within.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`geomA`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`.

`geomB`
:   Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`. The second parameter must also have the same coordinate system as the first. This means it is not possible to combine `geo_*` and `cartesian_*` parameters.

**Description**

Returns whether the first geometry is within the second geometry. This is the inverse of the [ST_CONTAINS](../esql-functions-operators.md#esql-st_contains) function.

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


## `ST_X` [esql-st_x]

**Syntax**

:::{image} ../../../../images/st_x.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`.

**Description**

Extracts the `x` coordinate from the supplied point. If the points is of type `geo_point` this is equivalent to extracting the `longitude` value.

**Supported types**

| point | result |
| --- | --- |
| cartesian_point | double |
| geo_point | double |

**Example**

```esql
ROW point = TO_GEOPOINT("POINT(42.97109629958868 14.7552534006536)")
| EVAL x =  ST_X(point), y = ST_Y(point)
```

| point:geo_point | x:double | y:double |
| --- | --- | --- |
| POINT(42.97109629958868 14.7552534006536) | 42.97109629958868 | 14.7552534006536 |


## `ST_Y` [esql-st_y]

**Syntax**

:::{image} ../../../../images/st_y.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`.

**Description**

Extracts the `y` coordinate from the supplied point. If the points is of type `geo_point` this is equivalent to extracting the `latitude` value.

**Supported types**

| point | result |
| --- | --- |
| cartesian_point | double |
| geo_point | double |

**Example**

```esql
ROW point = TO_GEOPOINT("POINT(42.97109629958868 14.7552534006536)")
| EVAL x =  ST_X(point), y = ST_Y(point)
```

| point:geo_point | x:double | y:double |
| --- | --- | --- |
| POINT(42.97109629958868 14.7552534006536) | 42.97109629958868 | 14.7552534006536 |


## `ST_ENVELOPE` [esql-st_envelope]

**Syntax**

:::{image} ../../../../images/st_envelope.svg
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


## `ST_XMAX` [esql-st_xmax]

**Syntax**

:::{image} ../../../../images/st_xmax.svg
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


## `ST_XMIN` [esql-st_xmin]

**Syntax**

:::{image} ../../../../images/st_xmin.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. If `null`, the function returns `null`.

**Description**

Extracts the minimum value of the `x` coordinates from the supplied geometry. If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the minimum `longitude` value.

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


## `ST_YMAX` [esql-st_ymax]

**Syntax**

:::{image} ../../../../images/st_ymax.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. If `null`, the function returns `null`.

**Description**

Extracts the maximum value of the `y` coordinates from the supplied geometry. If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the maximum `latitude` value.

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


## `ST_YMIN` [esql-st_ymin]

**Syntax**

:::{image} ../../../../images/st_ymin.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`point`
:   Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. If `null`, the function returns `null`.

**Description**

Extracts the minimum value of the `y` coordinates from the supplied geometry. If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the minimum `latitude` value.

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
