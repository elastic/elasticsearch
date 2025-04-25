---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-queries.html
---

# Geo queries [geo-queries]

Elasticsearch supports two types of geo data: [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) fields which support lat/lon pairs, and [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) fields, which support points, lines, circles, polygons, multi-polygons, etc.

The queries in this group are:

[`geo_bounding_box`](/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query.md) query
:   Finds documents with geoshapes or geopoints which intersect the specified rectangle.

[`geo_distance`](/reference/query-languages/query-dsl/query-dsl-geo-distance-query.md) query
:   Finds documents with geoshapes or geopoints within the specified distance of a central point.

[`geo_grid`](/reference/query-languages/query-dsl/query-dsl-geo-grid-query.md) query
:   Finds documents with:

    * Geoshapes or geopoints which intersect the specified geohash
    * Geoshapes or geopoints which intersect the specified map tile
    * Geopoints which intersect the specified H3 bin


[`geo_polygon`](/reference/query-languages/query-dsl/query-dsl-geo-polygon-query.md) query
:   Find documents with geoshapes or geopoints which intersect the specified polygon.

[`geo_shape`](/reference/query-languages/query-dsl/query-dsl-geo-shape-query.md) query
:   Finds documents with geoshapes or geopoints which are related to the specified geoshape. Possible spatial relationships to specify are: intersects, contained, within and disjoint.






