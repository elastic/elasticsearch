/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * A tree reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * The tree structure is navigated using a {@link Visitor}.
 *
 */
class TriangleTreeReader {

    private TriangleTreeReader() {
    }

    /**
     * Visit the Triangle tree using the {@link Visitor} provided.
     */
    public static void visit(ByteArrayStreamInput input, TriangleTreeReader.Visitor visitor, int thisMaxX, int thisMaxY)
        throws IOException {
        visit(input, visitor, true, thisMaxX, thisMaxY, true);
    }

    private static boolean visit(ByteArrayStreamInput input, TriangleTreeReader.Visitor visitor,
                                 boolean splitX, int thisMaxX, int thisMaxY, boolean isRoot) throws IOException {
        byte metadata = input.readByte();
        int thisMinX;
        int thisMinY;
        if ((metadata & 1 << 2) == 1 << 2) { // component in this node is a point
            int x = Math.toIntExact(thisMaxX - input.readVLong());
            int y = Math.toIntExact(thisMaxY - input.readVLong());
            visitor.visitPoint(x, y);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = x;
            thisMinY = y;
        } else if ((metadata & 1 << 3) == 1 << 3) { // component in this node is a line
            int aX = Math.toIntExact(thisMaxX - input.readVLong());
            int aY = Math.toIntExact(thisMaxY - input.readVLong());
            int bX = Math.toIntExact(thisMaxX - input.readVLong());
            int bY = Math.toIntExact(thisMaxY - input.readVLong());
            visitor.visitLine(aX, aY, bX, bY, metadata);
            if (visitor.push() == false) {
                return false;
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
            visitor.visitTriangle(aX, aY, bX, bY, cX, cY, metadata);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = aX;
            thisMinY = Math.min(Math.min(aY, bY), cY);
        }
        if ((metadata & 1 << 0) == 1 << 0) { // left != null
            if (pushLeft(input, visitor, thisMaxX, thisMaxY, splitX) == false) {
                return false;
            }
        }
        if ((metadata & 1 << 1) == 1 << 1) { // right != null
            // root node does not have a size
            int rightSize = isRoot ? 0 : input.readVInt();
            if (pushRight(input, visitor, thisMaxX, thisMaxY, thisMinX, thisMinY, splitX, rightSize) == false) {
                return false;
            }
        }
        return visitor.push();
    }

    private static boolean pushLeft(ByteArrayStreamInput input, TriangleTreeReader.Visitor visitor,
                                    int thisMaxX, int thisMaxY, boolean splitX)  throws IOException {
        int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
        int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
        int size = input.readVInt();
        if (visitor.push(nextMaxX, nextMaxY)) {
            return visit(input, visitor, splitX == false, nextMaxX, nextMaxY, false);
        } else {
            input.skipBytes(size);
            return visitor.push();
        }
    }

    private static boolean pushRight(ByteArrayStreamInput input, TriangleTreeReader.Visitor visitor, int thisMaxX,
                                     int thisMaxY, int thisMinX, int thisMinY, boolean splitX, int rightSize) throws IOException {
        if ((splitX == false && visitor.pushY(thisMinY)) || (splitX && visitor.pushX(thisMinX))) {
            int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
            int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
            int size = input.readVInt();
            if (visitor.push(nextMaxX, nextMaxY)) {
                return visit(input, visitor, splitX == false, nextMaxX, nextMaxY, false);
            } else {
                input.skipBytes(size);
            }
        } else {
            input.skipBytes(rightSize);
        }
        return visitor.push();
    }

    /** Visitor for triangle interval tree */
   interface Visitor {

        /** visit a node point. */
        void visitPoint(int x, int y);

        /** visit a node line. */
        void visitLine(int aX, int aY, int bX, int bY, byte metadata);

        /** visit a node triangle. */
        void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata);

        /** Should the visitor keep visiting the tree. Called after visiting a node or skipping
         * a tree branch, if the return value is {@code false}, no more nodes will be visited. */
        boolean push();

        /** Should the visitor visit nodes that have bounds greater or equal
         * than the {@code minX} provided. */
        boolean pushX(int minX);

        /** Should the visitor visit nodes that have bounds greater or equal
         * than the {@code minY} provided. */
        boolean pushY(int minY);

        /** Should the visitor visit nodes that have bounds lower or equal than the
         * {@code maxX} and {@code minX} provided. */
        boolean push(int maxX, int maxY);

        /** Should the visitor visit the tree given the bounding box of the tree. Called before
         * visiting the tree. */
        boolean push(int minX, int minY, int maxX, int maxY);
    }
}
