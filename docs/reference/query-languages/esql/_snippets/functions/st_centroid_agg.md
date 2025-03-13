## `ST_CENTROID_AGG` [esql-st_centroid_agg]

**Syntax**

:::{image} ../../../../../images/st_centroid_agg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Calculate the spatial centroid over a field with spatial point geometry type.

**Supported types**

| field | result |
| --- | --- |
| cartesian_point | cartesian_point |
| geo_point | geo_point |

**Example**

```esql
FROM airports
| STATS centroid=ST_CENTROID_AGG(location)
```

| centroid:geo_point |
| --- |
| POINT(-0.030548143003023033 24.37553649504829) |


