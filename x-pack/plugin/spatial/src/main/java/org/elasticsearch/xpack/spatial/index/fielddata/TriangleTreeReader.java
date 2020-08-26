/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * A tree reusable reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking bounding box
 * relations against the serialized triangle tree.
 *
 * -----------------------------------------
 * |   The binary format of the tree       |
 * -----------------------------------------
 * -----------------------------------------  --
 * |    centroid-x-coord (4 bytes)         |    |
 * -----------------------------------------    |
 * |    centroid-y-coord (4 bytes)         |    |
 * -----------------------------------------    |
 * |    DimensionalShapeType (1 byte)      |    | Centroid-related header
 * -----------------------------------------    |
 * |  Sum of weights (VLong 1-8 bytes)     |    |
 * -----------------------------------------  --
 * |         Extent (var-encoding)         |
 * -----------------------------------------
 * |         Triangle Tree                 |
 * -----------------------------------------
 * -----------------------------------------
 */
public class TriangleTreeReader {
    private final ByteArrayDataInput input;
    private final CoordinateEncoder coordinateEncoder;
    private final Tile2D tile2D;
    private final Extent extent;
    private int treeOffset;
    private int docValueOffset;

    public TriangleTreeReader(CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
        this.tile2D = new Tile2D();
        this.extent = new Extent();
        this.input = new ByteArrayDataInput();
    }

    public void reset(BytesRef bytesRef) throws IOException {
        this.input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        docValueOffset = bytesRef.offset;
        treeOffset = 0;
    }

    /**
     * returns the bounding box of the geometry in the format [minX, maxX, minY, maxY].
     */
    public Extent getExtent() {
        if (treeOffset == 0) {
            getSumCentroidWeight(); // skip CENTROID_HEADER + var-long sum-weight
            Extent.readFromCompressed(input, extent);
            treeOffset = input.getPosition();
        } else {
            input.setPosition(treeOffset);
        }
        return extent;
    }

    /**
     * returns the X coordinate of the centroid.
     */
    public double getCentroidX() {
        input.setPosition(docValueOffset + 0);
        return coordinateEncoder.decodeX(input.readInt());
    }

    /**
     * returns the Y coordinate of the centroid.
     */
    public double getCentroidY() {
        input.setPosition(docValueOffset + 4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    public DimensionalShapeType getDimensionalShapeType() {
        input.setPosition(docValueOffset + 8);
        return DimensionalShapeType.readFrom(input);
    }

    public double getSumCentroidWeight() {
        input.setPosition(docValueOffset + 9);
        return Double.longBitsToDouble(input.readVLong());
    }

    /**
     * Compute the relation with the provided bounding box. If the result is CELL_INSIDE_QUERY
     * then the bounding box is within the shape.
     */
    public GeoRelation relateTile(int minX, int minY, int maxX, int maxY) {
        Extent extent = getExtent();
        int thisMaxX = extent.maxX();
        int thisMinX = extent.minX();
        int thisMaxY = extent.maxY();
        int thisMinY = extent.minY();

        // exclude north and east boundary intersections with tiles from intersection consideration
        // for consistent tiling definition of shapes on the boundaries of tiles
        if ((thisMinX >= maxX || thisMaxX < minX || thisMinY > maxY || thisMaxY <= minY)) {
            // shapes are disjoint
            return GeoRelation.QUERY_DISJOINT;
        }
        if (minX <= thisMinX && maxX >= thisMaxX && minY <= thisMinY && maxY >= thisMaxY) {
            // the rectangle fully contains the shape
            return GeoRelation.QUERY_CROSSES;
        }
        // quick checks failed, need to traverse the tree
        GeoRelation rel = GeoRelation.QUERY_DISJOINT;
        tile2D.setValues(minX, maxX, minY, maxY);
        byte metadata = input.readByte();
        if ((metadata & 1 << 2) == 1 << 2) { // component in this node is a point
            int x = Math.toIntExact(thisMaxX - input.readVLong());
            int y = Math.toIntExact(thisMaxY - input.readVLong());
            if (tile2D.contains(x, y)) {
                return GeoRelation.QUERY_CROSSES;
            }
            thisMinX = x;
        } else if ((metadata & 1 << 3) == 1 << 3) {  // component in this node is a line
            int aX = Math.toIntExact(thisMaxX - input.readVLong());
            int aY = Math.toIntExact(thisMaxY - input.readVLong());
            int bX = Math.toIntExact(thisMaxX - input.readVLong());
            int bY = Math.toIntExact(thisMaxY - input.readVLong());
            if (tile2D.intersectsLine(aX, aY, bX, bY)) {
                return GeoRelation.QUERY_CROSSES;
            }
            thisMinX = aX;
        } else {  // component in this node is a triangle
            int aX = Math.toIntExact(thisMaxX - input.readVLong());
            int aY = Math.toIntExact(thisMaxY - input.readVLong());
            int bX = Math.toIntExact(thisMaxX - input.readVLong());
            int bY = Math.toIntExact(thisMaxY - input.readVLong());
            int cX = Math.toIntExact(thisMaxX - input.readVLong());
            int cY = Math.toIntExact(thisMaxY - input.readVLong());
            boolean ab = (metadata & 1 << 4) == 1 << 4;
            boolean bc = (metadata & 1 << 5) == 1 << 5;
            boolean ca = (metadata & 1 << 6) == 1 << 6;
            rel = tile2D.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
            if (rel == GeoRelation.QUERY_CROSSES) {
                return GeoRelation.QUERY_CROSSES;
            }
            thisMinX = aX;
        }
        if ((metadata & 1 << 0) == 1 << 0) { // left != null
            GeoRelation left = relateTile(tile2D, false, thisMaxX, thisMaxY);
            if (left == GeoRelation.QUERY_CROSSES) {
                return GeoRelation.QUERY_CROSSES;
            } else if (left == GeoRelation.QUERY_INSIDE) {
                rel = left;
            }
        }
        if ((metadata & 1 << 1) == 1 << 1) { // right != null
            if (tile2D.maxX >= thisMinX) {
                GeoRelation right = relateTile(tile2D, false, thisMaxX, thisMaxY);
                if (right == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                } else if (right == GeoRelation.QUERY_INSIDE) {
                    rel = right;
                }
            }
        }

        return rel;
    }

    private GeoRelation relateTile(Tile2D tile2D, boolean splitX, int parentMaxX, int parentMaxY) {
        int thisMaxX = Math.toIntExact(parentMaxX - input.readVLong());
        int thisMaxY = Math.toIntExact(parentMaxY - input.readVLong());
        GeoRelation rel = GeoRelation.QUERY_DISJOINT;
        int size = input.readVInt();
        if (tile2D.minY <= thisMaxY && tile2D.minX <= thisMaxX) {
            byte metadata = input.readByte();
            int thisMinX;
            int thisMinY;
            if ((metadata & 1 << 2) == 1 << 2) { // component in this node is a point
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (tile2D.contains(x, y)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = x;
                thisMinY = y;
            } else if ((metadata & 1 << 3) == 1 << 3) { // component in this node is a line
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                if (tile2D.intersectsLine(aX, aY, bX, bY)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
                thisMinY = Math.min(aY, bY);
            } else { // component in this node is a triangle
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                int cX = Math.toIntExact(thisMaxX - input.readVLong());
                int cY = Math.toIntExact(thisMaxY - input.readVLong());
                boolean ab = (metadata & 1 << 4) == 1 << 4;
                boolean bc = (metadata & 1 << 5) == 1 << 5;
                boolean ca = (metadata & 1 << 6) == 1 << 6;
                rel = tile2D.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
                if (rel == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
                thisMinY = Math.min(Math.min(aY, bY), cY);
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                GeoRelation left = relateTile(tile2D, !splitX, thisMaxX, thisMaxY);
                if (left == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                } else if (left == GeoRelation.QUERY_INSIDE) {
                    rel = left;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                int rightSize = input.readVInt();
                if ((splitX == false && tile2D.maxY >= thisMinY) || (splitX && tile2D.maxX >= thisMinX)) {
                    GeoRelation right = relateTile(tile2D, !splitX, thisMaxX, thisMaxY);
                    if (right == GeoRelation.QUERY_CROSSES) {
                        return GeoRelation.QUERY_CROSSES;
                    } else if (right == GeoRelation.QUERY_INSIDE) {
                        rel = right;
                    }
                } else {
                    input.skipBytes(rightSize);
                }
            }
        } else {
            input.skipBytes(size);
        }
        return rel;
    }

    private static class Tile2D {

        protected int minX;
        protected int maxX;
        protected int minY;
        protected int maxY;

        Tile2D() {
        }

        private void setValues(int minX, int maxX, int minY, int maxY) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
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
         * Compute whether the given x, y point is in a triangle; uses the winding order method
         */
        private static boolean pointInTriangle(double minX, double maxX, double minY, double maxY, double x, double y,
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

        /**
         * utility method to check if two boxes are disjoint
         */
        private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                                final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
            return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
        }
    }
}
