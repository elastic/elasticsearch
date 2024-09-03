/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;

import java.util.Arrays;

/** Utility methods that generate a lucene query for a spatial query over a cartesian field.* */
public class XYQueriesUtils {

    /** Generates a lucene query for a field that has been previously indexed using {@link XYPoint}.It expects
     * either {code indexed} or {@code has docValues} to be true or or both to be true.
     *
     *  Note that lucene only supports intersects spatial relation so we build other relations
     *  using just that one.
     * */
    public static Query toXYPointQuery(Geometry geometry, String fieldName, ShapeRelation relation, boolean indexed, boolean hasDocValues) {
        assert indexed || hasDocValues;
        final XYGeometry[] luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
        // XYPointField only supports intersects query so we build all the relationships using that logic.
        // it is not very efficient but it works.
        return switch (relation) {
            case INTERSECTS -> buildIntersectsQuery(fieldName, indexed, hasDocValues, luceneGeometries);
            case DISJOINT -> buildDisjointQuery(fieldName, indexed, hasDocValues, luceneGeometries);
            case CONTAINS -> buildContainsQuery(fieldName, indexed, hasDocValues, luceneGeometries);
            case WITHIN -> buildWithinQuery(fieldName, indexed, hasDocValues, luceneGeometries);
        };
    }

    private static Query buildIntersectsQuery(String fieldName, boolean isIndexed, boolean hasDocValues, XYGeometry... luceneGeometries) {
        // This is supported natively in lucene
        Query query;
        if (isIndexed) {
            query = XYPointField.newGeometryQuery(fieldName, luceneGeometries);
            if (hasDocValues) {
                final Query queryDocValues = XYDocValuesField.newSlowGeometryQuery(fieldName, luceneGeometries);
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
        } else {
            query = XYDocValuesField.newSlowGeometryQuery(fieldName, luceneGeometries);
        }
        return query;
    }

    private static Query buildDisjointQuery(String fieldName, boolean isIndexed, boolean hasDocValues, XYGeometry... luceneGeometries) {
        // first collect all the documents that contain a shape
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (hasDocValues) {
            builder.add(new FieldExistsQuery(fieldName), BooleanClause.Occur.FILTER);
        } else {
            builder.add(
                buildIntersectsQuery(
                    fieldName,
                    isIndexed,
                    hasDocValues,
                    new XYRectangle(-Float.MAX_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE, Float.MAX_VALUE)
                ),
                BooleanClause.Occur.FILTER
            );
        }
        // then remove all intersecting documents
        builder.add(buildIntersectsQuery(fieldName, isIndexed, hasDocValues, luceneGeometries), BooleanClause.Occur.MUST_NOT);
        return builder.build();
    }

    private static Query buildContainsQuery(String fieldName, boolean isIndexed, boolean hasDocValues, XYGeometry... luceneGeometries) {
        // for non-point data the result is always false
        if (allPoints(luceneGeometries) == false) {
            return new MatchNoDocsQuery();
        }
        // for a unique point, it behaves like intersect
        if (luceneGeometries.length == 1) {
            return buildIntersectsQuery(fieldName, isIndexed, hasDocValues, luceneGeometries);
        }
        // for a multi point, all points needs to be in the document
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (XYGeometry geometry : luceneGeometries) {
            builder.add(buildIntersectsQuery(fieldName, isIndexed, hasDocValues, geometry), BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    private static Query buildWithinQuery(String fieldName, boolean isIndexed, boolean hasDocValues, XYGeometry... luceneGeometries) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        // collect all the intersecting documents
        builder.add(buildIntersectsQuery(fieldName, isIndexed, hasDocValues, luceneGeometries), BooleanClause.Occur.FILTER);
        // This is the tricky part as we need to remove all documents that they have at least one disjoint point.
        // In order to do that, we introduce a InverseXYGeometry which return all documents that have at least one disjoint point
        // with the original geometry.
        builder.add(
            buildIntersectsQuery(fieldName, isIndexed, hasDocValues, new InverseXYGeometry(luceneGeometries)),
            BooleanClause.Occur.MUST_NOT
        );
        return builder.build();
    }

    private static boolean allPoints(XYGeometry[] geometries) {
        return Arrays.stream(geometries).allMatch(g -> g instanceof XYPoint);
    }

    private static class InverseXYGeometry extends XYGeometry {
        private final XYGeometry[] geometries;

        InverseXYGeometry(XYGeometry... geometries) {
            this.geometries = geometries;
        }

        @Override
        protected Component2D toComponent2D() {
            final Component2D component2D = XYGeometry.create(geometries);
            return new Component2D() {
                @Override
                public double getMinX() {
                    return -Float.MAX_VALUE;
                }

                @Override
                public double getMaxX() {
                    return Float.MAX_VALUE;
                }

                @Override
                public double getMinY() {
                    return -Float.MAX_VALUE;
                }

                @Override
                public double getMaxY() {
                    return Float.MAX_VALUE;
                }

                @Override
                public boolean contains(double x, double y) {
                    return component2D.contains(x, y) == false;
                }

                @Override
                public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
                    PointValues.Relation relation = component2D.relate(minX, maxX, minY, maxY);
                    return switch (relation) {
                        case CELL_INSIDE_QUERY -> PointValues.Relation.CELL_OUTSIDE_QUERY;
                        case CELL_OUTSIDE_QUERY -> PointValues.Relation.CELL_INSIDE_QUERY;
                        case CELL_CROSSES_QUERY -> PointValues.Relation.CELL_CROSSES_QUERY;
                    };
                }

                @Override
                public boolean intersectsLine(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    double bX,
                    double bY
                ) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean intersectsTriangle(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    double bX,
                    double bY,
                    double cX,
                    double cY
                ) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean containsLine(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    double bX,
                    double bY
                ) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean containsTriangle(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    double bX,
                    double bY,
                    double cX,
                    double cY
                ) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public WithinRelation withinPoint(double x, double y) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public WithinRelation withinLine(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    boolean ab,
                    double bX,
                    double bY
                ) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public WithinRelation withinTriangle(
                    double minX,
                    double maxX,
                    double minY,
                    double maxY,
                    double aX,
                    double aY,
                    boolean ab,
                    double bX,
                    double bY,
                    boolean bc,
                    double cX,
                    double cY,
                    boolean ca
                ) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InverseXYGeometry that = (InverseXYGeometry) o;
            return Arrays.equals(geometries, that.geometries);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(geometries);
        }
    }

    /** Generates a lucene query for a field that has been previously indexed using {@link XYShape}.It expects
     * either {code indexed} or {@code has docValues} to be true or both to be true. */
    public static Query toXYShapeQuery(Geometry geometry, String fieldName, ShapeRelation relation, boolean indexed, boolean hasDocValues) {
        assert indexed || hasDocValues;
        if (geometry == null || geometry.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        final XYGeometry[] luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
        Query query;
        if (indexed) {
            query = XYShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
            if (hasDocValues) {
                final Query queryDocValues = new CartesianShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
        } else {
            query = new CartesianShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
        }
        return query;
    }
}
