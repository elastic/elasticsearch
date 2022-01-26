/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * A reusable tree reader visitor for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking bounding box relations against a serialized triangle tree. Note that this differ
 * from Rectangle2D lucene implementation because it excludes north and east boundary intersections with tiles
 * from intersection consideration for consistent tiling definition of shapes on the boundaries of tiles
 *
 */
class Tile2DVisitor implements TriangleTreeReader.Visitor {

    private GeoRelation relation;
    private int minX;
    private int maxX;
    private int minY;
    private int maxY;

    Tile2DVisitor() {}

    public void reset(int minX, int minY, int maxX, int maxY) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        relation = GeoRelation.QUERY_DISJOINT;
    }

    /**
     * return the computed relation.
     */
    public GeoRelation relation() {
        return relation;
    }

    @Override
    public void visitPoint(int x, int y) {
        if (contains(x, y)) {
            relation = GeoRelation.QUERY_CROSSES;
        }
    }

    @Override
    public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
        if (intersectsLine(aX, aY, bX, bY)) {
            relation = GeoRelation.QUERY_CROSSES;
        }
    }

    @Override
    public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
        boolean ab = (metadata & 1 << 4) == 1 << 4;
        boolean bc = (metadata & 1 << 5) == 1 << 5;
        boolean ca = (metadata & 1 << 6) == 1 << 6;
        GeoRelation geoRelation = relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
        if (geoRelation != GeoRelation.QUERY_DISJOINT) {
            this.relation = geoRelation;
        }
    }

    @Override
    public boolean push() {
        return relation != GeoRelation.QUERY_CROSSES;
    }

    @Override
    public boolean pushX(int minX) {
        return this.maxX >= minX;
    }

    @Override
    public boolean pushY(int minY) {
        return this.maxY >= minY;
    }

    @Override
    public boolean push(int maxX, int maxY) {
        return this.minY <= maxY && this.minX <= maxX;
    }

    @Override
    @SuppressWarnings("HiddenField")
    public boolean push(int minX, int minY, int maxX, int maxY) {
        // exclude north and east boundary intersections with tiles from intersection consideration
        // for consistent tiling definition of shapes on the boundaries of tiles
        if (minX >= this.maxX || maxX < this.minX || minY > this.maxY || maxY <= this.minY) {
            // shapes are disjoint
            relation = GeoRelation.QUERY_DISJOINT;
            return false;
        }
        if (this.minX <= minX && this.maxX >= maxX && this.minY <= minY && this.maxY >= maxY) {
            // the rectangle fully contains the shape
            relation = GeoRelation.QUERY_CROSSES;
            return false;
        }
        return true;
    }

    /**
     * Checks if the rectangle contains the provided point
     **/
    public boolean contains(int x, int y) {
        return (x <= minX || x > maxX || y < minY || y >= maxY) == false;
    }

    /**
     * Checks if the rectangle intersects the provided triangle
     **/
    private boolean intersectsLine(int aX, int aY, int bX, int bY) {
        // 1. query contains any triangle points
        if (contains(aX, aY) || contains(bX, bY)) {
            return true;
        }

        // compute bounding box of triangle
        int tMinX = StrictMath.min(aX, bX);
        int tMaxX = StrictMath.max(aX, bX);
        int tMinY = StrictMath.min(aY, bY);
        int tMaxY = StrictMath.max(aY, bY);

        // 2. check bounding boxes are disjoint
        if (tMaxX <= minX || tMinX > maxX || tMinY > maxY || tMaxY <= minY) {
            return false;
        }

        // 4. last ditch effort: check crossings
        if (edgeIntersectsQuery(aX, aY, bX, bY)) {
            return true;
        }
        return false;
    }

    /**
     * Checks if the rectangle intersects the provided triangle
     **/
    private GeoRelation relateTriangle(int aX, int aY, boolean ab, int bX, int bY, boolean bc, int cX, int cY, boolean ca) {
        // compute bounding box of triangle
        int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
        int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
        int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
        int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

        // 1. check bounding boxes are disjoint, where north and east boundaries are not considered as crossing
        if (tMaxX <= minX || tMinX > maxX || tMinY > maxY || tMaxY <= minY) {
            return GeoRelation.QUERY_DISJOINT;
        }

        // 2. query contains any triangle points
        if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
            return GeoRelation.QUERY_CROSSES;
        }

        boolean within = false;
        if (edgeIntersectsQuery(aX, aY, bX, bY)) {
            if (ab) {
                return GeoRelation.QUERY_CROSSES;
            }
            within = true;
        }

        // right
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

        if (within || pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)) {
            return GeoRelation.QUERY_INSIDE;
        }

        return GeoRelation.QUERY_DISJOINT;
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
    private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
        // shortcut: check bboxes of edges are disjoint
        if (boxesAreDisjoint(Math.min(ax, bx), Math.max(ax, bx), Math.min(ay, by), Math.max(ay, by), minX, maxX, minY, maxY)) {
            return false;
        }

        // top
        if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0
            && orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
            return true;
        }

        // right
        if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0
            && orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
            return true;
        }

        // bottom
        if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0
            && orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
            return true;
        }

        // left
        if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0
            && orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
            return true;
        }

        return false;
    }

    /**
     * Compute whether the given x, y point is in a triangle; uses the winding order method
     */
    private static boolean pointInTriangle(
        double minX,
        double maxX,
        double minY,
        double maxY,
        double x,
        double y,
        double aX,
        double aY,
        double bX,
        double bY,
        double cX,
        double cY
    ) {
        // check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
        // coplanar points that are not part of the triangle.
        if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
            int a = orient(x, y, aX, aY, bX, bY);
            int b = orient(x, y, bX, bY, cX, cY);
            if (a == 0 || b == 0 || a < 0 == b < 0) {
                int c = orient(x, y, cX, cY, aX, aY);
                return c == 0 || (c < 0 == (b < 0 || a < 0));
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * utility method to check if two boxes are disjoint
     */
    private static boolean boxesAreDisjoint(
        final int aMinX,
        final int aMaxX,
        final int aMinY,
        final int aMaxY,
        final int bMinX,
        final int bMaxX,
        final int bMinY,
        final int bMaxY
    ) {
        return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
    }
}
