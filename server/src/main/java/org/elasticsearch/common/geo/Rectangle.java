/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.geo;

import java.util.Objects;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D rectangle implementation containing  spatial logic.
 */
public class Rectangle {

    protected final int minX;
    protected final int maxX;
    protected final int minY;
    protected final int maxY;

    public Rectangle(int minX, int maxX, int minY, int maxY) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
    }

    /** Checks if the rectangle contains the provided point **/
    public boolean contains(int x, int y) {
        return (x < minX || x > maxX || y < minY || y > maxY) == false;
    }

    /** Checks if the rectangle intersects the provided triangle **/
    public boolean intersectsLine(int aX, int aY, int bX, int bY) {
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
        if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
            return false;
        }

        // 4. last ditch effort: check crossings
        if (edgeIntersectsQuery(aX, aY, bX, bY)) {
            return true;
        }
        return false;
    }

    /** Checks if the rectangle intersects the provided triangle **/
    public boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
        // 1. query contains any triangle points
        if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
            return true;
        }

        // compute bounding box of triangle
        int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
        int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
        int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
        int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

        // 2. check bounding boxes are disjoint
         if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
            return false;
        }

        // 3. check triangle contains any query points
        if (pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)) {
            return true;
        } else if (pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, maxX, minY, aX, aY, bX, bY, cX, cY)) {
            return true;
        } else if (pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, maxX, maxY, aX, aY, bX, bY, cX, cY)) {
            return true;
        } else if (pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, minX, maxY, aX, aY, bX, bY, cX, cY)) {
            return true;
        }

        // 4. last ditch effort: check crossings
        if (edgeIntersectsQuery(aX, aY, bX, bY) ||
            edgeIntersectsQuery(bX, bY, cX, cY) ||
            edgeIntersectsQuery(cX, cY, aX, aY)) {
            return true;
        }
        return false;
    }

    /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
    private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
        // shortcut: if edge is a point (occurs w/ Line shapes); simply check bbox w/ point
        if (ax == bx && ay == by) {
            return contains(ax, ay);
        }

        // shortcut: check if either of the end points fall inside the box
        if (contains(ax, ay) || contains(bx, by)) {
            return true;
        }

        // shortcut: check bboxes of edges are disjoint
        if (boxesAreDisjoint(Math.min(ax, bx), Math.max(ax, bx), Math.min(ay, by), Math.max(ay, by),
            minX, maxX, minY, maxY)) {
            return false;
        }

        // top
        if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
            orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
            return true;
        }

        // right
        if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
            orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
            return true;
        }

        // bottom
        if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
            orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
            return true;
        }

        // left
        if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
            orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
            return true;
        }
        return false;
    }

    /**
     * Compute whether the given x, y point is in a triangle; uses the winding order method */
    static boolean pointInTriangle(double minX, double maxX, double minY, double maxY, double x, double y,
                                   double aX, double aY, double bX, double bY, double cX, double cY) {
        //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
        //coplanar points that are not part of the triangle.
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

    /** utility method to check if two boxes are disjoint */
    private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                            final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
        return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Rectangle)) return false;
        Rectangle that = (Rectangle) o;
        return minX == that.minX &&
            maxX == that.maxX &&
            minY == that.minY &&
            maxY == that.maxY;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(minX, maxX, minY, maxY);
        return result;
    }
}

