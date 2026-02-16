/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

import static org.elasticsearch.lucene.spatial.TriangleTreeWriter.LEFT;
import static org.elasticsearch.lucene.spatial.TriangleTreeWriter.LINE;
import static org.elasticsearch.lucene.spatial.TriangleTreeWriter.POINT;
import static org.elasticsearch.lucene.spatial.TriangleTreeWriter.RIGHT;

/**
 * A tree reader for a previously serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * <p>Supports two formats:
 * <ul>
 *   <li><b>V2 (new)</b>: Triangle components store vertex ordinals referencing a
 *       {@link VertexLookupTable}. The reader resolves ordinals to coordinates transparently.</li>
 *   <li><b>Legacy</b>: Triangle components store coordinate values as VLong deltas from the
 *       parent node's maxX/maxY bounds.</li>
 * </ul>
 *
 * <p>The tree structure (bounding box encoding, left/right children) is identical in both formats.
 * The {@link TriangleTreeVisitor} API receives actual coordinate values in both cases.
 */
class TriangleTreeReader {

    private TriangleTreeReader() {}

    // ---- V2 format: vertex ordinals resolved via VertexLookupTable ----

    /**
     * Visit the V2 triangle tree. Vertex coordinates are resolved from the lookup table.
     */
    public static void visit(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        VertexLookupTable vertexTable
    ) throws IOException {
        visitV2(input, visitor, true, thisMaxX, thisMaxY, true, vertexTable);
    }

    private static boolean visitV2(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        boolean splitX,
        int thisMaxX,
        int thisMaxY,
        boolean isRoot,
        VertexLookupTable vertexTable
    ) throws IOException {
        byte metadata = input.readByte();
        int thisMinX;
        int thisMinY;
        if ((metadata & POINT) == POINT) {
            int ord = input.readVInt();
            int x = vertexTable.getX(ord);
            int y = vertexTable.getY(ord);
            visitor.visitPoint(x, y);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = x;
            thisMinY = y;
        } else if ((metadata & LINE) == LINE) {
            int aOrd = input.readVInt();
            int bOrd = input.readVInt();
            int aX = vertexTable.getX(aOrd);
            int aY = vertexTable.getY(aOrd);
            int bX = vertexTable.getX(bOrd);
            int bY = vertexTable.getY(bOrd);
            visitor.visitLine(aX, aY, bX, bY, metadata);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = aX;
            thisMinY = Math.min(aY, bY);
        } else {
            int aOrd = input.readVInt();
            int bOrd = input.readVInt();
            int cOrd = input.readVInt();
            int aX = vertexTable.getX(aOrd);
            int aY = vertexTable.getY(aOrd);
            int bX = vertexTable.getX(bOrd);
            int bY = vertexTable.getY(bOrd);
            int cX = vertexTable.getX(cOrd);
            int cY = vertexTable.getY(cOrd);
            visitor.visitTriangle(aX, aY, bX, bY, cX, cY, metadata);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = aX;
            thisMinY = Math.min(Math.min(aY, bY), cY);
        }
        if ((metadata & LEFT) == LEFT) {
            if (pushLeftV2(input, visitor, thisMaxX, thisMaxY, splitX, vertexTable) == false) {
                return false;
            }
        }
        if ((metadata & RIGHT) == RIGHT) {
            int rightSize = isRoot ? 0 : input.readVInt();
            if (pushRightV2(input, visitor, thisMaxX, thisMaxY, thisMinX, thisMinY, splitX, rightSize, vertexTable) == false) {
                return false;
            }
        }
        return visitor.push();
    }

    private static boolean pushLeftV2(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        boolean splitX,
        VertexLookupTable vertexTable
    ) throws IOException {
        int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
        int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
        int size = input.readVInt();
        if (visitor.push(nextMaxX, nextMaxY)) {
            return visitV2(input, visitor, splitX == false, nextMaxX, nextMaxY, false, vertexTable);
        } else {
            input.skipBytes(size);
            return visitor.push();
        }
    }

    private static boolean pushRightV2(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        int thisMinX,
        int thisMinY,
        boolean splitX,
        int rightSize,
        VertexLookupTable vertexTable
    ) throws IOException {
        if ((splitX == false && visitor.pushY(thisMinY)) || (splitX && visitor.pushX(thisMinX))) {
            int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
            int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
            int size = input.readVInt();
            if (visitor.push(nextMaxX, nextMaxY)) {
                return visitV2(input, visitor, splitX == false, nextMaxX, nextMaxY, false, vertexTable);
            } else {
                input.skipBytes(size);
            }
        } else {
            input.skipBytes(rightSize);
        }
        return visitor.push();
    }

    // ---- Legacy format: inline coordinate deltas ----

    /**
     * Visit the legacy triangle tree where coordinates are stored as VLong deltas from parent maxX/maxY.
     */
    public static void visitLegacy(ByteArrayStreamInput input, TriangleTreeVisitor visitor, int thisMaxX, int thisMaxY)
        throws IOException {
        visitLegacy(input, visitor, true, thisMaxX, thisMaxY, true);
    }

    private static boolean visitLegacy(
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
        if ((metadata & POINT) == POINT) {
            int x = Math.toIntExact(thisMaxX - input.readVLong());
            int y = Math.toIntExact(thisMaxY - input.readVLong());
            visitor.visitPoint(x, y);
            if (visitor.push() == false) {
                return false;
            }
            thisMinX = x;
            thisMinY = y;
        } else if ((metadata & LINE) == LINE) {
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
        } else {
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
        if ((metadata & LEFT) == LEFT) {
            if (pushLeftLegacy(input, visitor, thisMaxX, thisMaxY, splitX) == false) {
                return false;
            }
        }
        if ((metadata & RIGHT) == RIGHT) {
            int rightSize = isRoot ? 0 : input.readVInt();
            if (pushRightLegacy(input, visitor, thisMaxX, thisMaxY, thisMinX, thisMinY, splitX, rightSize) == false) {
                return false;
            }
        }
        return visitor.push();
    }

    private static boolean pushLeftLegacy(
        ByteArrayStreamInput input,
        TriangleTreeVisitor visitor,
        int thisMaxX,
        int thisMaxY,
        boolean splitX
    ) throws IOException {
        int nextMaxX = Math.toIntExact(thisMaxX - input.readVLong());
        int nextMaxY = Math.toIntExact(thisMaxY - input.readVLong());
        int size = input.readVInt();
        if (visitor.push(nextMaxX, nextMaxY)) {
            return visitLegacy(input, visitor, splitX == false, nextMaxX, nextMaxY, false);
        } else {
            input.skipBytes(size);
            return visitor.push();
        }
    }

    private static boolean pushRightLegacy(
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
                return visitLegacy(input, visitor, splitX == false, nextMaxX, nextMaxY, false);
            } else {
                input.skipBytes(size);
            }
        } else {
            input.skipBytes(rightSize);
        }
        return visitor.push();
    }
}
