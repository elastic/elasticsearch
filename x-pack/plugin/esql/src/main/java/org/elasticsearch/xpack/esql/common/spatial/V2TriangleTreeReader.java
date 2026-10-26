/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.lucene.spatial.VertexLookupTable;

import java.io.IOException;

/**
 * Traverses the V2 triangle interval tree stored in {@code geo_shape} doc values, where each node references its
 * vertices by ordinal into a {@link VertexLookupTable} instead of storing coordinate deltas inline.
 * <p>
 * Like {@link TriangleTreeReader}, this is a deliberate copy of the server's {@code V2TriangleTreeReader} so that
 * ES|QL's visitors do not share JIT type profiles with the spatial plugin's: see {@link TriangleTreeVisitor}. The
 * binary format is owned by the server's {@code V2TriangleTreeWriter}.
 */
final class V2TriangleTreeReader {

    // Metadata bits as written by the server's package-private TriangleTreeWriter.
    private static final byte LEFT = 1;
    private static final byte RIGHT = 1 << 1;
    private static final byte POINT = 1 << 2;
    private static final byte LINE = 1 << 3;

    private V2TriangleTreeReader() {}

    /**
     * Visit the triangle tree using the {@link TriangleTreeVisitor} provided. The input must be positioned
     * at the start of the tree, directly after the tree length that follows the extent.
     */
    static void visit(ByteArrayStreamInput input, TriangleTreeVisitor visitor, int thisMaxX, int thisMaxY, VertexLookupTable vertices)
        throws IOException {
        visit(input, visitor, true, thisMaxX, thisMaxY, true, vertices);
    }

    private static boolean visit(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        boolean splitX,
        int thisMaxX,
        int thisMaxY,
        boolean isRoot,
        VertexLookupTable vertices
    ) throws IOException {
        byte metadata = input.readByte();
        int thisMinX;
        int thisMinY;
        if ((metadata & POINT) == POINT) { // component in this node is a point
            int ord = input.readVInt();
            int x = vertices.getX(ord);
            int y = vertices.getY(ord);
            visitor.visitPoint(x, y);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = x;
            thisMinY = y;
        } else if ((metadata & LINE) == LINE) { // component in this node is a line
            int aOrd = input.readVInt();
            int bOrd = input.readVInt();
            int aX = vertices.getX(aOrd);
            int aY = vertices.getY(aOrd);
            int bX = vertices.getX(bOrd);
            int bY = vertices.getY(bOrd);
            visitor.visitLine(aX, aY, bX, bY, metadata);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = aX;
            thisMinY = Math.min(aY, bY);
        } else { // component in this node is a triangle
            int aOrd = input.readVInt();
            int bOrd = input.readVInt();
            int cOrd = input.readVInt();
            int aX = vertices.getX(aOrd);
            int aY = vertices.getY(aOrd);
            int bX = vertices.getX(bOrd);
            int bY = vertices.getY(bOrd);
            int cX = vertices.getX(cOrd);
            int cY = vertices.getY(cOrd);
            visitor.visitTriangle(aX, aY, bX, bY, cX, cY, metadata);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = aX;
            thisMinY = Math.min(Math.min(aY, bY), cY);
        }
        if ((metadata & LEFT) == LEFT) { // left != null
            if (pushLeft(input, visitor, thisMaxX, thisMaxY, splitX, vertices) == false) {
                return false;
            }
        }
        if ((metadata & RIGHT) == RIGHT) { // right != null
            // root node does not have a size
            int rightSize = isRoot ? 0 : input.readVInt();
            if (pushRight(input, visitor, thisMaxX, thisMaxY, thisMinX, thisMinY, splitX, rightSize, vertices) == false) {
                return false;
            }
        }
        return visitor.push();
    }

    private static boolean pushLeft(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        boolean splitX,
        VertexLookupTable vertices
    ) throws IOException {
        int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
        int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
        int size = input.readVInt();
        if (visitor.push(nextMaxX, nextMaxY)) {
            return visit(input, visitor, splitX == false, nextMaxX, nextMaxY, false, vertices);
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
        int rightSize,
        VertexLookupTable vertices
    ) throws IOException {
        if ((splitX == false && visitor.pushY(thisMinY)) || (splitX && visitor.pushX(thisMinX))) {
            int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
            int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
            int size = input.readVInt();
            if (visitor.push(nextMaxX, nextMaxY)) {
                return visit(input, visitor, splitX == false, nextMaxX, nextMaxY, false, vertices);
            } else {
                input.skipBytes(size);
            }
        } else {
            input.skipBytes(rightSize);
        }
        return visitor.push();
    }
}
