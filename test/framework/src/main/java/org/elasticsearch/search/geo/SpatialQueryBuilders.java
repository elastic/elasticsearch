/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;

import java.io.IOException;
import java.util.List;

/**
 * This interface allows tests to abstract away which exact QueryBuilder is used in the tests.
 * We have generalized the Geo-specific tests in test.framework to be re-used by Cartesian tests
 * in the xpack-spatial module. This requires implmenting this interface for other QueryBuilders,
 * for example ShapeQueryBuilder in xpack for Cartesian.
 */
public interface SpatialQueryBuilders<T extends AbstractGeometryQueryBuilder<T>> {
    AbstractGeometryQueryBuilder<T> shapeQuery(String name, Geometry shape);

    AbstractGeometryQueryBuilder<T> shapeQuery(String name, String indexedShapeId);

    AbstractGeometryQueryBuilder<T> intersectionQuery(String name, Geometry shape) throws IOException;

    AbstractGeometryQueryBuilder<T> intersectionQuery(String name, String indexedShapeId);

    AbstractGeometryQueryBuilder<T> withinQuery(String name, Geometry shape) throws IOException;

    AbstractGeometryQueryBuilder<T> withinQuery(String name, String indexedShapeId);

    AbstractGeometryQueryBuilder<T> disjointQuery(String name, Geometry shape) throws IOException;

    AbstractGeometryQueryBuilder<T> disjointQuery(String name, String indexedShapeId);

    Geo GEO = new Geo();

    final class Geo implements SpatialQueryBuilders<GeoShapeQueryBuilder> {

        /**
         * A filter to filter based on a specific distance from a specific geo location / point.
         *
         * @param name The location field name.
         */
        public GeoDistanceQueryBuilder distanceQuery(String name) {
            return new GeoDistanceQueryBuilder(name);
        }

        /**
         * A filter to filter based on a bounding box defined by top left and bottom right locations / points
         *
         * @param name The location field name.
         */
        public GeoBoundingBoxQueryBuilder boundingBoxQuery(String name) {
            return new GeoBoundingBoxQueryBuilder(name);
        }

        /**
         * A filter to filter based on a polygon defined by a set of locations  / points.
         *
         * @param name The location field name.
         * @deprecated use {@link #intersectionQuery(String, Geometry)} instead
         */
        @Deprecated
        public GeoPolygonQueryBuilder polygonQuery(String name, List<GeoPoint> points) {
            return new GeoPolygonQueryBuilder(name, points);
        }

        /**
         * A filter based on the relationship of a shape and indexed shapes
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public GeoShapeQueryBuilder shapeQuery(String name, Geometry shape) {
            return new GeoShapeQueryBuilder(name, shape);
        }

        public GeoShapeQueryBuilder shapeQuery(String name, String indexedShapeId) {
            return new GeoShapeQueryBuilder(name, indexedShapeId);
        }

        /**
         * A filter to filter indexed shapes intersecting with shapes
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public GeoShapeQueryBuilder intersectionQuery(String name, Geometry shape) throws IOException {
            GeoShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.INTERSECTS);
            return builder;
        }

        public GeoShapeQueryBuilder intersectionQuery(String name, String indexedShapeId) {
            GeoShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.INTERSECTS);
            return builder;
        }

        /**
         * A filter to filter indexed shapes that are contained by a shape
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public GeoShapeQueryBuilder withinQuery(String name, Geometry shape) throws IOException {
            GeoShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.WITHIN);
            return builder;
        }

        public GeoShapeQueryBuilder withinQuery(String name, String indexedShapeId) {
            GeoShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.WITHIN);
            return builder;
        }

        /**
         * A filter to filter indexed shapes that are not intersection with the query shape
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public GeoShapeQueryBuilder disjointQuery(String name, Geometry shape) throws IOException {
            GeoShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.DISJOINT);
            return builder;
        }

        public GeoShapeQueryBuilder disjointQuery(String name, String indexedShapeId) {
            GeoShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.DISJOINT);
            return builder;
        }
    }
}
