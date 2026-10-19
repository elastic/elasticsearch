/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * Traverses the triangle interval tree stored in {@code geo_shape} doc values, dispatching each node
 * to a {@link TriangleTreeVisitor}.
 * <p>
 * This is a deliberate copy of the traversal loop in {@code org.elasticsearch.lucene.spatial.TriangleTreeReader},
 * <b>not</b> shared code to be re-unified: see {@link TriangleTreeVisitor} for why ES|QL keeps its own copy of
 * the loop and the visitor hierarchy. The binary format it reads is owned by the server's
 * {@code TriangleTreeWriter}; the node-type metadata bits are restated here because the writer's constants are
 * package-private.
 */
final class TriangleTreeReader {

    // Metadata bits as written by the server's package-private TriangleTreeWriter.
    private static final byte LEFT = 1;
    private static final byte RIGHT = 1 << 1;
    private static final byte POINT = 1 << 2;
    private static final byte LINE = 1 << 3;

    private TriangleTreeReader() {}

    /**
     * Visit the Triangle tree using the {@link TriangleTreeVisitor} provided. The input must be positioned
     * at the start of the tree, directly after the extent.
     */
    static void visit(ByteArrayStreamInput input, TriangleTreeVisitor visitor, int thisMaxX, int thisMaxY) throws IOException {
        visit(input, visitor, true, thisMaxX, thisMaxY, true);
    }

    private static boolean visit(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        boolean splitX,
        int thisMaxX,
        int thisMaxY,
        boolean isRoot
    ) throws IOException {
        byte metadata = input.readByte();
        int thisMinX;
        int thisMinY;
        if ((metadata & POINT) == POINT) { // component in this node is a point
            int x = Math.toIntExact(thisMaxX - input.readVLong());
            int y = Math.toIntExact(thisMaxY - input.readVLong());
            visitor.visitPoint(x, y);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = x;
            thisMinY = y;
        } else if ((metadata & LINE) == LINE) { // component in this node is a line
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
        if ((metadata & LEFT) == LEFT) { // left != null
            if (pushLeft(input, visitor, thisMaxX, thisMaxY, splitX) == false) {
                return false;
            }
        }
        if ((metadata & RIGHT) == RIGHT) { // right != null
            // root node does not have a size
            int rightSize = isRoot ? 0 : input.readVInt();
            if (pushRight(input, visitor, thisMaxX, thisMaxY, thisMinX, thisMinY, splitX, rightSize) == false) {
                return false;
            }
        }
        return visitor.push();
    }

    private static boolean pushLeft(ByteArrayStreamInput input, TriangleTreeVisitor visitor, int thisMaxX, int thisMaxY, boolean splitX)
        throws IOException {
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

    private static boolean pushRight(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        int thisMinX,
        int thisMinY,
        boolean splitX,
        int rightSize
    ) throws IOException {
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
}
