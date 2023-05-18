/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.search.geo.SpatialQueryBuilders;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;

public final class TestSpatialQueryBuilders {
    public static final Cartesian CARTESIAN = new Cartesian();

    private static final class Cartesian implements SpatialQueryBuilders<ShapeQueryBuilder> {
        // TODO: Implement QueryBuilder for distance, boundingBox, polygon

        /**
         * A filter based on the relationship of a shape and indexed shapes
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public ShapeQueryBuilder shapeQuery(String name, Geometry shape) {
            return new ShapeQueryBuilder(name, shape);
        }

        public ShapeQueryBuilder shapeQuery(String name, String indexedShapeId) {
            return new ShapeQueryBuilder(name, indexedShapeId);
        }

        /**
         * A filter to filter indexed shapes intersecting with shapes
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public ShapeQueryBuilder intersectionQuery(String name, Geometry shape) {
            ShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.INTERSECTS);
            return builder;
        }

        public ShapeQueryBuilder intersectionQuery(String name, String indexedShapeId) {
            ShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.INTERSECTS);
            return builder;
        }

        /**
         * A filter to filter indexed shapes that are contained by a shape
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public ShapeQueryBuilder withinQuery(String name, Geometry shape) {
            ShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.WITHIN);
            return builder;
        }

        public ShapeQueryBuilder withinQuery(String name, String indexedShapeId) {
            ShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.WITHIN);
            return builder;
        }

        /**
         * A filter to filter indexed shapes that are not intersection with the query shape
         *
         * @param name  The shape field name
         * @param shape Shape to use in the filter
         */
        public ShapeQueryBuilder disjointQuery(String name, Geometry shape) {
            ShapeQueryBuilder builder = shapeQuery(name, shape);
            builder.relation(ShapeRelation.DISJOINT);
            return builder;
        }

        public ShapeQueryBuilder disjointQuery(String name, String indexedShapeId) {
            ShapeQueryBuilder builder = shapeQuery(name, indexedShapeId);
            builder.relation(ShapeRelation.DISJOINT);
            return builder;
        }
    }
}
