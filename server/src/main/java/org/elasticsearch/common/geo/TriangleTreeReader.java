/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.geo;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A tree reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking bounding box
 * relations against the serialized triangle tree.
 */
public class TriangleTreeReader {

    private final int extentOffset = 8;
    private final ByteBufferStreamInput input;
    private final CoordinateEncoder coordinateEncoder;

    public TriangleTreeReader(BytesRef bytesRef, CoordinateEncoder coordinateEncoder) {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        this.coordinateEncoder = coordinateEncoder;
    }

    /**
     * returns the bounding box of the geometry in the format [minX, maxX, minY, maxY].
     */
    public int[] getExtent() throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        return new int[] {thisMinX, thisMaxX, thisMinY, thisMaxY};
    }

    /**
     * returns the X coordinate of the centroid.
     */
    public double getCentroidX() throws IOException {
        input.position(0);
        return coordinateEncoder.decodeX(input.readInt());
    }

    /**
     * returns the Y coordinate of the centroid.
     */
    public double getCentroidY() throws IOException {
        input.position(4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    /**
     * Compute the intersection with the provided bounding box
     */
    public boolean intersects(int minX, int maxX, int minY, int maxY) throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        if (minX <= thisMinX && maxX >= thisMaxX && minY <= thisMinY && maxY >= thisMaxY) {
            return true;
        }
        if ((thisMinX > maxX || thisMaxX < minX || thisMinY > maxY || thisMaxY < minY) == false) {
            Rectangle component = new Rectangle(minX, maxX, minY, maxY);
            byte metadata = input.readByte();
            if ((metadata & 1 << 2) == 1 << 2) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return true;
                }
            } else if ((metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsLine(aX, aY, bX, bY)) {
                    return true;
                }
            } else {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
                    return true;
                }
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                if (intersects(component, true, thisMaxX, thisMaxY)) {
                    return true;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                if (intersects(component, true, thisMaxX, thisMaxY)) {
                    return true;
                }
            }
        }
       return false;
    }

    private boolean intersects(Rectangle component, boolean splitX, int parentMaxX, int parentMaxY) throws IOException {
        int thisMaxX = Math.toIntExact(parentMaxX - input.readVLong());
        int thisMaxY = Math.toIntExact(parentMaxY - input.readVLong());
        int size =  input.readVInt();
        if (component.minY <= thisMaxY && component.minX <= thisMaxX) {
            byte metadata = input.readByte();
            int thisMinX;
            int thisMinY;
            if ((metadata & 1 << 2) == 1 << 2) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return true;
                }
                thisMinX = x;
                thisMinY = y;
            } else if ((metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsLine(aX, aY, bX, bY)) {
                    return true;
                }
                thisMinX = aX;
                thisMinY = Math.min(aY, bY);
            } else {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
                    return true;
                }
                thisMinX = aX;
                thisMinY = Math.min(Math.min(aY, bY), cY);
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                if (intersects(component, !splitX, thisMaxX, thisMaxY)) {
                    return true;
                }
            }

            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                int rightSize = input.readVInt();
                if ((splitX == false && component.maxY >= thisMinY) || (splitX && component.maxX >= thisMinX)) {
                    if (intersects(component, !splitX, thisMaxX, thisMaxY)) {
                        return true;
                    }
                } else {
                    input.skip(rightSize);
                }
            }
        } else {
            input.skip(size);
        }
        return false;
    }

    /**
     * Compute the relation with the provided bounding box. If the result is CELL_INSIDE_QUERY
     * then the bounding box is within the shape.
     */
    public PointValues.Relation relate(int minX, int maxX, int minY, int maxY) throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        if (minX <= thisMinX && maxX >= thisMaxX && minY <= thisMinY && maxY >= thisMaxY) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
        PointValues.Relation rel = PointValues.Relation.CELL_OUTSIDE_QUERY;
        if ((thisMinX > maxX || thisMaxX < minX || thisMinY > maxY || thisMaxY < minY) == false) {
            Rectangle component = new Rectangle(minX, maxX, minY, maxY);
            byte metadata = input.readByte();
            if ((metadata & 1 << 2) == 1 << 2) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            } else if ((metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsLine(aX, aY, bX, bY)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            } else {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                boolean ab = (metadata & 1 << 4) == 1 << 4;
                boolean bc = (metadata & 1 << 5) == 1 << 5;
                boolean ca = (metadata & 1 << 6) == 1 << 6;
                rel = component.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
                if (rel == PointValues.Relation.CELL_CROSSES_QUERY) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                PointValues.Relation left = relate(component, true, thisMaxX, thisMaxY);
                if (left == PointValues.Relation.CELL_CROSSES_QUERY) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else if (left == PointValues.Relation.CELL_INSIDE_QUERY) {
                    rel = left;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                PointValues.Relation right = relate(component, true, thisMaxX, thisMaxY);
                if (right == PointValues.Relation.CELL_CROSSES_QUERY) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else if (right == PointValues.Relation.CELL_INSIDE_QUERY) {
                    rel = right;
                }
            }
        }
        return rel;
    }

    private PointValues.Relation relate(Rectangle component, boolean splitX, int parentMaxX, int parentMaxY) throws IOException {
        int thisMaxX = Math.toIntExact(parentMaxX - input.readVLong());
        int thisMaxY = Math.toIntExact(parentMaxY - input.readVLong());
        PointValues.Relation rel = PointValues.Relation.CELL_OUTSIDE_QUERY;
        int size = input.readVInt();
        if (component.minY <= thisMaxY && component.minX <= thisMaxX) {
            byte metadata = input.readByte();
            int thisMinX;
            int thisMinY;
            if ((metadata & 1 << 2) == 1 << 2) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                thisMinX = x;
                thisMinY = y;
            } else if ((metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsLine(aX, aY, bX, bY)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                thisMinX = aX;
                thisMinY = Math.min(aY, bY);
            } else {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                boolean ab = (metadata & 1 << 4) == 1 << 4;
                boolean bc = (metadata & 1 << 5) == 1 << 5;
                boolean ca = (metadata & 1 << 6) == 1 << 6;
                rel = component.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
                if (rel == PointValues.Relation.CELL_CROSSES_QUERY) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                thisMinX = aX;
                thisMinY = Math.min(Math.min(aY, bY), cY);
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                PointValues.Relation left = relate(component, !splitX, thisMaxX, thisMaxY);
                if (left == PointValues.Relation.CELL_CROSSES_QUERY) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else if (left == PointValues.Relation.CELL_INSIDE_QUERY) {
                    rel = left;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                int rightSize = input.readVInt();
                if ((splitX == false && component.maxY >= thisMinY) || (splitX && component.maxX >= thisMinX)) {
                    PointValues.Relation right = relate(component, !splitX, thisMaxX, thisMaxY);
                    if (right == PointValues.Relation.CELL_CROSSES_QUERY) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    } else if (right == PointValues.Relation.CELL_INSIDE_QUERY) {
                        rel = right;
                    }
                } else {
                    input.skip(rightSize);
                }
            }
        } else {
            input.skip(size);
        }
        return rel;
    }
}
