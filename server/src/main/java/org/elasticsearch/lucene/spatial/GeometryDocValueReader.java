/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * A reusable Geometry doc value reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link GeometryDocValueWriter}.
 *
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
public class GeometryDocValueReader {
    private final ByteArrayStreamInput input;
    private final Extent extent;
    private int treeOffset;
    private int docValueOffset;
    private BytesRef bytesRef;

    public GeometryDocValueReader() {
        this.extent = new Extent();
        this.input = new ByteArrayStreamInput();
    }

    /**
     * reset the geometry.
     */
    public void reset(BytesRef bytesRef) throws IOException {
        this.bytesRef = bytesRef; // Needed only for supporting Writable, maintaining original offset, not adjusted on from input
        this.input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        docValueOffset = bytesRef.offset;
        treeOffset = 0;
    }

    /**
     * returns the {@link Extent} of this geometry.
     */
    public Extent getExtent() throws IOException {
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
     * returns the encoded X coordinate of the centroid.
     */
    public int getCentroidX() throws IOException {
        input.setPosition(docValueOffset + 0);
        return input.readInt();
    }

    /**
     * returns the encoded Y coordinate of the centroid.
     */
    public int getCentroidY() throws IOException {
        input.setPosition(docValueOffset + 4);
        return input.readInt();
    }

    public DimensionalShapeType getDimensionalShapeType() {
        input.setPosition(docValueOffset + 8);
        return DimensionalShapeType.readFrom(input);
    }

    public double getSumCentroidWeight() throws IOException {
        input.setPosition(docValueOffset + 9);
        return Double.longBitsToDouble(input.readVLong());
    }

    /**
     * Visit the triangle tree with the provided visitor
     */
    public void visit(TriangleTreeVisitor visitor) throws IOException {
        Extent geometryExtent = getExtent();
        int thisMaxX = geometryExtent.maxX();
        int thisMinX = geometryExtent.minX();
        int thisMaxY = geometryExtent.maxY();
        int thisMinY = geometryExtent.minY();
        if (visitor.push(thisMinX, thisMinY, thisMaxX, thisMaxY)) {
            TriangleTreeReader.visit(input, visitor, thisMaxX, thisMaxY);
        }
    }

    public BytesRef getBytesRef() {
        return bytesRef;
    }
}
