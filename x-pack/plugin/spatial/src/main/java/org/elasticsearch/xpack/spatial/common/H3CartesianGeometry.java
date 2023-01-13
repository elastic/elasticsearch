/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;

/**
 * Lucene geometry representing an H3 bin on the cartesian space.
 */
class H3CartesianGeometry extends LatLonGeometry {

    private final long h3;

    H3CartesianGeometry(long h3) {
        this.h3 = h3;
    }

    @Override
    protected Component2D toComponent2D() {
        return new H3CartesianComponent(h3);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        H3CartesianGeometry that = (H3CartesianGeometry) o;
        return h3 == that.h3;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(h3);
    }

    private static class H3CartesianComponent implements Component2D {

        private final double[] xs, ys;
        private final double minX, maxX, minY, maxY;
        private final boolean crossesDateline;

        H3CartesianComponent(long h3) {
            final double[] xs = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
            final double[] ys = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
            final int numPoints = H3CartesianUtil.computePoints(h3, xs, ys);
            this.xs = ArrayUtil.copyOfSubArray(xs, 0, numPoints);
            this.ys = ArrayUtil.copyOfSubArray(ys, 0, numPoints);
            double minX = Double.POSITIVE_INFINITY;
            double maxX = Double.NEGATIVE_INFINITY;
            double minY = Double.POSITIVE_INFINITY;
            double maxY = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < numPoints; i++) {
                minX = Math.min(minX, xs[i]);
                maxX = Math.max(maxX, xs[i]);
                minY = Math.min(minY, ys[i]);
                maxY = Math.max(maxY, ys[i]);
            }
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.crossesDateline = maxX - minX > 180d && H3CartesianUtil.isPolar(h3) == false;
        }

        @Override
        public double getMinX() {
            return crossesDateline ? -180d : minX;
        }

        @Override
        public double getMaxX() {
            return crossesDateline ? 180d : maxX;
        }

        @Override
        public double getMinY() {
            return minY;
        }

        @Override
        public double getMaxY() {
            return maxY;
        }

        @Override
        public boolean contains(double x, double y) {
            // fail fast if we're outside the bounding box
            if (Rectangle.containsPoint(y, x, getMinY(), getMaxY(), getMinX(), getMaxX()) == false) {
                return false;
            }
            return H3CartesianUtil.relatePoint(xs, ys, xs.length, crossesDateline, x, y) != GeoRelation.QUERY_DISJOINT;
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            if (Component2D.within(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            ContainsCorners containsCorners = containsCorners(minX, maxX, minY, maxY);
            if (containsCorners == ContainsCorners.ALL) {
                if (H3CartesianUtil.crossesBox(xs, ys, xs.length, crossesDateline, minX, maxX, minY, maxY, true)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_INSIDE_QUERY;
            } else if (containsCorners == ContainsCorners.NONE) {
                if (Component2D.containsPoint(xs[0], ys[0], minX, maxX, minY, maxY)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                if (H3CartesianUtil.crossesBox(xs, ys, xs.length, crossesDateline, minX, maxX, minY, maxY, true)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        @Override
        public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return contains(aX, aY) || contains(bX, bY) || crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true);
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
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return false;
            }
            return contains(aX, aY)
                || contains(bX, bY)
                || contains(cX, cY)
                || Component2D.pointInTriangle(minX, maxX, minY, maxY, xs[0], ys[0], aX, aY, bX, bY, cX, cY)
                || H3CartesianUtil.crossesTriangle(
                    xs,
                    ys,
                    xs.length,
                    crossesDateline,
                    minX,
                    maxX,
                    minY,
                    maxY,
                    aX,
                    aY,
                    bX,
                    bY,
                    cX,
                    cY,
                    true
                );
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return contains(aX, aY) && contains(bX, bY) && crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, false) == false;
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
            return (contains(aX, aY)
                && contains(bX, bY)
                && contains(cX, cY)
                && H3CartesianUtil.crossesTriangle(
                    xs,
                    ys,
                    xs.length,
                    crossesDateline,
                    minX,
                    maxX,
                    minY,
                    maxY,
                    aX,
                    aY,
                    bX,
                    bY,
                    cX,
                    cY,
                    false
                ) == false);

        }

        @Override
        public WithinRelation withinPoint(double x, double y) {
            return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
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
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }
            if (contains(aX, aY) || contains(bX, bY)) {
                return WithinRelation.NOTWITHIN;
            }
            if (ab && crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
                return WithinRelation.NOTWITHIN;
            }
            return WithinRelation.DISJOINT;
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
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }

            // if any of the points is inside the polygon, the polygon cannot be within this indexed
            // shape because points belong to the original indexed shape.
            if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
                return WithinRelation.NOTWITHIN;
            }

            WithinRelation relation = WithinRelation.DISJOINT;
            // if any of the edges intersects and the edge belongs to the shape then it cannot be within.
            // if it only intersects edges that do not belong to the shape, then it is a candidate
            // we skip edges at the dateline to support shapes crossing it
            if (crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
                if (ab) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            if (crossesLine(minX, maxX, minY, maxY, bX, bY, cX, cY, true)) {
                if (bc) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }
            if (crossesLine(minX, maxX, minY, maxY, cX, cY, aX, aY, true)) {
                if (ca) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            // if any of the edges crosses and edge that does not belong to the shape
            // then it is a candidate for within
            if (relation == WithinRelation.CANDIDATE) {
                return WithinRelation.CANDIDATE;
            }

            // Check if shape is within the triangle
            if (Component2D.pointInTriangle(minX, maxX, minY, maxY, xs[0], ys[0], aX, aY, bX, bY, cX, cY)) {
                return WithinRelation.CANDIDATE;
            }
            return relation;
        }

        private boolean crossesLine(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            double bX,
            double bY,
            boolean includeBoundary
        ) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return false;
            }
            return H3CartesianUtil.crossesLine(xs, ys, xs.length, crossesDateline, minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary);
        }

        private ContainsCorners containsCorners(double minX, double maxX, double minY, double maxY) {
            int containsCount = 0;
            if (contains(minX, minY)) {
                containsCount++;
            }
            if (contains(maxX, minY)) {
                containsCount++;
            }
            if (containsCount == 1) {
                return ContainsCorners.SOME;
            }
            if (contains(maxX, maxY)) {
                containsCount++;
            }
            if (containsCount == 2) {
                return ContainsCorners.SOME;
            }
            if (contains(minX, maxY)) {
                containsCount++;
            }
            return containsCount == 0 ? ContainsCorners.NONE : containsCount == 4 ? ContainsCorners.ALL : ContainsCorners.SOME;
        }

        private enum ContainsCorners {
            NONE,
            SOME,
            ALL
        }
    }
}
