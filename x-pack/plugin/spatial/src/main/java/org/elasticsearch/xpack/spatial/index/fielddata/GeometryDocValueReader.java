/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;

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
    private final ByteArrayDataInput input;
    private final Extent extent;
    private int treeOffset;
    private int docValueOffset;

    public GeometryDocValueReader() {
        this.extent = new Extent();
        this.input = new ByteArrayDataInput();
    }

    /**
     * reset the geometry.
     */
    public void reset(BytesRef bytesRef) {
        this.input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        docValueOffset = bytesRef.offset;
        treeOffset = 0;
    }

    /**
     * returns the {@link Extent} of this geometry.
     */
    protected Extent getExtent() {
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
    protected int getCentroidX() {
        input.setPosition(docValueOffset + 0);
        try {
            return CodecUtil.readBEInt(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * returns the encoded Y coordinate of the centroid.
     */
    protected int getCentroidY() {
        input.setPosition(docValueOffset + 4);
        try {
            return CodecUtil.readBEInt(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected DimensionalShapeType getDimensionalShapeType() {
        input.setPosition(docValueOffset + 8);
        return DimensionalShapeType.readFrom(input);
    }

    protected double getSumCentroidWeight() {
        input.setPosition(docValueOffset + 9);
        return Double.longBitsToDouble(input.readVLong());
    }

    /**
     * Visit the triangle tree with the provided visitor
     */
    public void visit(TriangleTreeReader.Visitor visitor) {
        Extent extent = getExtent();
        int thisMaxX = extent.maxX();
        int thisMinX = extent.minX();
        int thisMaxY = extent.maxY();
        int thisMinY = extent.minY();
        if(visitor.push(thisMinX, thisMinY, thisMaxX, thisMaxY)) {
            TriangleTreeReader.visit(input, visitor, thisMaxX, thisMaxY);
        }
    }

}
