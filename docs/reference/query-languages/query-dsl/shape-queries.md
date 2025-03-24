---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shape-queries.html
---

# Shape queries [shape-queries]

Like [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) Elasticsearch supports the ability to index arbitrary two dimension (non Geospatial) geometries making it possible to map out virtual worlds, sporting venues, theme parks, and CAD diagrams.

Elasticsearch supports two types of cartesian data: [`point`](/reference/elasticsearch/mapping-reference/point.md) fields which support x/y pairs, and [`shape`](/reference/elasticsearch/mapping-reference/shape.md) fields, which support points, lines, circles, polygons, multi-polygons, etc.

The queries in this group are:

[`shape`](/reference/query-languages/query-dsl/query-dsl-shape-query.md) query
:   Finds documents with:

    * `shapes` which either intersect, are contained by, are within or do not intersect with the specified shape
    * `points` which intersect the specified shape



