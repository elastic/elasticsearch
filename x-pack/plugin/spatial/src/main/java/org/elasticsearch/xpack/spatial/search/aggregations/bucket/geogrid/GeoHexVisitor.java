/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;

import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.TriangleTreeDecodedVisitor;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.abFromTriangle;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.bcFromTriangle;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.caFromTriangle;

/**
 * A reusable tree reader visitor for a previous serialized {@link org.elasticsearch.geometry.Geometry}.
 *
 * This class supports checking H3 cells relations against a serialized triangle tree. It has the special property that if
 * a geometry touches any of the edges, then it will never return {@link GeoRelation#QUERY_CONTAINS}, for example a point on the boundary
 * return {@link GeoRelation#QUERY_CROSSES}.
 */
class GeoHexVisitor extends TriangleTreeDecodedVisitor {

    private GeoRelation relation;
    private final double[] xs, ys;
    private double minX, maxX, minY, maxY;
    private boolean crossesDateline;
    private int numPoints;

    GeoHexVisitor() {
        super(CoordinateEncoder.GEO);
        xs = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
        ys = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
    }

    public double[] getXs() {
        return ArrayUtil.copyOfSubArray(xs, 0, numPoints);
    }

    public double[] getYs() {
        return ArrayUtil.copyOfSubArray(ys, 0, numPoints);
    }

    public double getLeftX() {
        return crossesDateline ? maxX : minX;
    }

    public double getRightX() {
        return crossesDateline ? minX : maxX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMaxY() {
        return maxY;
    }

    /**
     * reset this visitor to the provided h3 cell
     */
    public void reset(long h3) {
        numPoints = H3CartesianUtil.computePoints(h3, xs, ys);
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

    /**
     * return the computed relation.
     */
    public GeoRelation relation() {
        return relation;
    }

    @Override
    public void visitDecodedPoint(double x, double y) {
        updateRelation(relatePoint(x, y));
    }

    @Override
    protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
        updateRelation(relateLine(aX, aY, bX, bY));
    }

    @Override
    protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
        final boolean ab = abFromTriangle(metadata);
        final boolean bc = bcFromTriangle(metadata);
        final boolean ca = caFromTriangle(metadata);
        updateRelation(relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca));
    }

    private void updateRelation(GeoRelation relation) {
        if (relation != GeoRelation.QUERY_DISJOINT) {
            if (relation == GeoRelation.QUERY_INSIDE && canBeInside()) {
                this.relation = GeoRelation.QUERY_INSIDE;
            } else if (relation == GeoRelation.QUERY_CONTAINS && canBeContained()) {
                this.relation = GeoRelation.QUERY_CONTAINS;
            } else {
                this.relation = GeoRelation.QUERY_CROSSES;
            }
        } else {
            adjustRelationForNotIntersectingComponent();
        }
    }

    private void adjustRelationForNotIntersectingComponent() {
        if (relation == null) {
            this.relation = GeoRelation.QUERY_DISJOINT;
        } else if (relation == GeoRelation.QUERY_CONTAINS) {
            this.relation = GeoRelation.QUERY_CROSSES;
        }
    }

    private boolean canBeContained() {
        return this.relation == null || this.relation == GeoRelation.QUERY_CONTAINS;
    }

    private boolean canBeInside() {
        return this.relation != GeoRelation.QUERY_CONTAINS;
    }

    @Override
    public boolean push() {
        return this.relation != GeoRelation.QUERY_CROSSES;
    }

    @Override
    public boolean pushDecodedX(double minX) {
        if (crossesDateline || this.maxX >= minX) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
    }

    @Override
    public boolean pushDecodedY(double minY) {
        if (this.maxY >= minY) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
    }

    @Override
    public boolean pushDecoded(double maxX, double maxY) {
        if (this.minY <= maxY && (this.crossesDateline || minX <= maxX)) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
    }

    @Override
    @SuppressWarnings("HiddenField")
    public boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
        if (boxesAreDisjoint(minX, maxX, minY, maxY)) {
            // shapes are disjoint
            this.relation = GeoRelation.QUERY_DISJOINT;
            return false;
        }
        relation = null;
        return true;
    }

    /** Check if the provided bounding box intersect the H3 bin. It supports bounding boxes
     * crossing the dateline. */
    public boolean intersectsBbox(double minX, double maxX, double minY, double maxY) {
        if (minX > maxX) {
            return relateBbox(minX, GeoUtils.MAX_LON_INCL, minY, maxY) || relateBbox(GeoUtils.MIN_LON_INCL, maxX, minY, maxY);
        } else {
            return relateBbox(minX, maxX, minY, maxY);
        }
    }

    private boolean relateBbox(double minX, double maxX, double minY, double maxY) {
        if (boxesAreDisjoint(minX, maxX, minY, maxY)) {
            return false;
        }
        if (minX <= xs[0] && maxX >= xs[0] && minY <= ys[0] && maxY >= ys[0]) {
            return true;
        }
        return relatePoint(minX, minY) != GeoRelation.QUERY_DISJOINT
            || H3CartesianUtil.crossesBox(xs, ys, numPoints, crossesDateline, minX, maxX, minY, maxY, true);
    }

    /**
     * Checks if the rectangle contains the provided point
     **/
    private GeoRelation relatePoint(double x, double y) {
        if (boxesAreDisjoint(x, x, y, y)) {
            return GeoRelation.QUERY_DISJOINT;
        }
        return H3CartesianUtil.relatePoint(xs, ys, numPoints, crossesDateline, x, y);
    }

    /**
     * Compute the relationship between the provided line and this h3 bin
     **/
    private GeoRelation relateLine(double aX, double aY, double bX, double bY) {
        // query contains any points
        GeoRelation relation1 = relatePoint(aX, aY);
        GeoRelation relation2 = relatePoint(bX, bY);
        if (relation1 != relation2 || relation1 == GeoRelation.QUERY_CROSSES) {
            return GeoRelation.QUERY_CROSSES;
        } else if (relation1 == GeoRelation.QUERY_CONTAINS) {
            if (crossesDateline) {
                final double minX = Math.min(aX, bX);
                final double maxX = Math.max(aX, bX);
                final double minY = Math.min(aY, bY);
                final double maxY = Math.max(aY, bY);
                if (H3CartesianUtil.crossesLine(xs, ys, numPoints, crossesDateline, minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
                    return GeoRelation.QUERY_CROSSES;
                }
            }
            return GeoRelation.QUERY_CONTAINS;
        }
        // 2. check crossings
        if (edgeIntersectsQuery(aX, aY, bX, bY)) {
            return GeoRelation.QUERY_CROSSES;
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    /**
     * Compute the relationship between the provided triangle and this h3 bin
     **/
    private GeoRelation relateTriangle(
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
        // compute bounding box of triangle
        double tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
        double tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
        double tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
        double tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

        // 1. check bounding boxes are disjoint
        if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY)) {
            return GeoRelation.QUERY_DISJOINT;
        }

        GeoRelation relation1 = relatePoint(aX, aY);
        GeoRelation relation2 = relatePoint(bX, bY);
        GeoRelation relation3 = relatePoint(cX, cY);

        // 2. query contains any triangle points
        if (relation1 != relation2 || relation1 != relation3 || relation1 == GeoRelation.QUERY_CROSSES) {
            return GeoRelation.QUERY_CROSSES;
        } else if (relation1 == GeoRelation.QUERY_CONTAINS) {
            if (crossesDateline
                && H3CartesianUtil.crossesTriangle(
                    xs,
                    ys,
                    numPoints,
                    crossesDateline,
                    tMinX,
                    tMaxX,
                    tMinY,
                    tMaxY,
                    aX,
                    aY,
                    bX,
                    bY,
                    cX,
                    cY,
                    true
                )) {
                return GeoRelation.QUERY_CROSSES;
            }
            return GeoRelation.QUERY_CONTAINS;
        }

        boolean within = false;
        if (edgeIntersectsQuery(aX, aY, bX, bY)) {
            if (ab) {
                return GeoRelation.QUERY_CROSSES;
            }
            within = true;
        }

        if (edgeIntersectsQuery(bX, bY, cX, cY)) {
            if (bc) {
                return GeoRelation.QUERY_CROSSES;
            }
            within = true;
        }

        if (edgeIntersectsQuery(cX, cY, aX, aY)) {
            if (ca) {
                return GeoRelation.QUERY_CROSSES;
            }
            within = true;
        }

        if (within || Component2D.pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, xs[0], ys[0], aX, aY, bX, bY, cX, cY)) {
            return GeoRelation.QUERY_INSIDE;
        }

        return GeoRelation.QUERY_DISJOINT;
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
    private boolean edgeIntersectsQuery(double ax, double ay, double bx, double by) {
        final double minX = Math.min(ax, bx);
        final double maxX = Math.max(ax, bx);
        final double minY = Math.min(ay, by);
        final double maxY = Math.max(ay, by);
        return boxesAreDisjoint(minX, maxX, minY, maxY) == false
            && H3CartesianUtil.crossesLine(xs, ys, numPoints, crossesDateline, minX, maxX, minY, maxY, ax, ay, bx, by, true);
    }

    /**
     * utility method to check if two boxes are disjoint
     */
    private boolean boxesAreDisjoint(final double minX, final double maxX, final double minY, final double maxY) {
        if ((maxY < this.minY || minY > this.maxY) == false) {
            if (crossesDateline) {
                return maxX < this.minX && minX > this.maxX;
            } else {
                return maxX < this.minX || minX > this.maxX;
            }
        }
        return true;
    }
}
