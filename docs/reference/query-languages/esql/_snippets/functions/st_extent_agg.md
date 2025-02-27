## `ST_EXTENT_AGG` [esql-st_extent_agg]

**Syntax**

:::{image} ../../../../../images/st_extent_agg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Calculate the spatial extent over a field with geometry type. Returns a bounding box for all values of the field.

**Supported types**

| field | result |
| --- | --- |
| cartesian_point | cartesian_shape |
| cartesian_shape | cartesian_shape |
| geo_point | geo_shape |
| geo_shape | geo_shape |

**Example**

```esql
FROM airports
| WHERE country == "India"
| STATS extent = ST_EXTENT_AGG(location)
```

| extent:geo_shape |
| --- |
| BBOX (70.77995480038226, 91.5882289968431, 33.9830909203738, 8.47650992218405) |


