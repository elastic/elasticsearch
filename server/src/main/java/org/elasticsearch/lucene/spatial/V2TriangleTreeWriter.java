/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * V2 triangle tree writer where vertex coordinates are stored as ordinals referencing a
 * {@link VertexLookupTable}, deduplicating shared vertices across adjacent triangles.
 *
 * <p>Since the vertex table is in geometry order, the two vertices of a line segment almost always have consecutive
 * ordinals. A line then stores only its first ordinal, and flags the second as the next or previous ordinal using
 * metadata bits that are otherwise unused for lines.
 */
class V2TriangleTreeWriter extends TriangleTreeWriter {

    /** Line metadata bit: the second ordinal is not stored, and is the first ordinal plus one. */
    static final byte LINE_NEXT_ORDINAL = BC_FROM_TRIANGLE;
    /** Line metadata bit: the second ordinal is not stored, and is the first ordinal minus one. */
    static final byte LINE_PREVIOUS_ORDINAL = CA_FROM_TRIANGLE;

    private V2TriangleTreeWriter() {}

    private static byte lineFlags(int aOrd, int bOrd) {
        if (bOrd == aOrd + 1) {
            return LINE_NEXT_ORDINAL;
        } else if (bOrd == aOrd - 1) {
            return LINE_PREVIOUS_ORDINAL;
        }
        return 0;
    }

    /**
     * Builds the triangle tree and extent from the given fields, populating the vertex table builder
     * with unique vertices, then writes the extent, tree length, and tree to the output.
     * The tree length prefix allows the reader to skip past the tree to reach subsequent sections
     * (vertex table, connectivity) without traversing it.
     *
     * @return the extent that was written, which the vertex table coordinates are relative to
     */
    static Extent writeTo(StreamOutput out, List<IndexableField> fields, VertexLookupTable.Builder vertexTableBuilder) throws IOException {
        final Extent extent = new Extent();
        final TriangleTreeNode node = build(fields, extent, c -> new V2Node(c, vertexTableBuilder));
        extent.writeCompressed(out);
        CountingStreamOutput countingBuffer = new CountingStreamOutput();
        out.writeVInt(Math.toIntExact(node.totalSize(countingBuffer)));
        node.writeTo(out);
        return extent;
    }

    static class V2Node extends TriangleTreeNode {
        private final int aOrd;
        private final int bOrd;
        private final int cOrd;

        V2Node(ShapeField.DecodedTriangle component, VertexLookupTable.Builder vertexTableBuilder) {
            super(component);
            this.aOrd = vertexTableBuilder.addVertex(component.aX, component.aY);
            this.bOrd = vertexTableBuilder.addVertex(component.bX, component.bY);
            this.cOrd = vertexTableBuilder.addVertex(component.cX, component.cY);
        }

        @Override
        byte componentFlags() {
            return component.type == ShapeField.DecodedTriangle.TYPE.LINE ? lineFlags(aOrd, bOrd) : 0;
        }

        @Override
        void writeComponent(StreamOutput out) throws IOException {
            out.writeVInt(aOrd);
            if (component.type == ShapeField.DecodedTriangle.TYPE.POINT) {
                return;
            }
            if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                if (lineFlags(aOrd, bOrd) == 0) {
                    out.writeVInt(bOrd);
                }
                return;
            }
            out.writeVInt(bOrd);
            out.writeVInt(cOrd);
        }

        @Override
        long componentSize(CountingStreamOutput countingBuffer) throws IOException {
            countingBuffer.reset();
            countingBuffer.writeVInt(aOrd);
            if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                if (lineFlags(aOrd, bOrd) == 0) {
                    countingBuffer.writeVInt(bOrd);
                }
            } else if (component.type == ShapeField.DecodedTriangle.TYPE.TRIANGLE) {
                countingBuffer.writeVInt(bOrd);
                countingBuffer.writeVInt(cOrd);
            }
            return countingBuffer.position();
        }
    }
}
