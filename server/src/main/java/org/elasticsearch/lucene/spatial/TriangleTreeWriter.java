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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * This is a tree-writer that serializes a list of {@link ShapeField.DecodedTriangle} as an interval tree
 * into a byte array. Contains shared constants, tree building, and the abstract {@link TriangleTreeNode}
 * with common tree traversal logic. Also provides the legacy writer that stores coordinate values as
 * VLong deltas from the node's bounding box maxX/maxY.
 *
 * @see V2TriangleTreeWriter
 */
public class TriangleTreeWriter {

    static final byte LEFT = 1;
    static final byte RIGHT = 1 << 1;
    static final byte POINT = 1 << 2;
    static final byte LINE = 1 << 3;
    static final byte AB_FROM_TRIANGLE = 1 << 4;
    static final byte BC_FROM_TRIANGLE = 1 << 5;
    static final byte CA_FROM_TRIANGLE = 1 << 6;

    TriangleTreeWriter() {}

    /**
     * Builds the triangle tree and writes it in legacy format (extent + tree with coordinate deltas).
     * No vertex table is used. This is the original format before V2.
     */
    public static void writeTo(StreamOutput out, List<IndexableField> fields) throws IOException {
        final Extent extent = new Extent();
        final TriangleTreeNode node = build(fields, extent, LegacyNode::new);
        extent.writeCompressed(out);
        node.writeTo(out);
    }

    static ShapeField.DecodedTriangle toDecodedTriangle(IndexableField field, byte[] scratch) {
        final BytesRef bytesRef = field.binaryValue();
        assert bytesRef.length == 7 * Integer.BYTES;
        System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
        final ShapeField.DecodedTriangle decodedTriangle = new ShapeField.DecodedTriangle();
        ShapeField.decodeTriangle(scratch, decodedTriangle);
        return decodedTriangle;
    }

    /**
     * Builds a tree from the given fields using the provided factory to create format-specific nodes.
     */
    static TriangleTreeNode build(
        List<IndexableField> fields,
        Extent extent,
        Function<ShapeField.DecodedTriangle, ? extends TriangleTreeNode> nodeFactory
    ) {
        final byte[] scratch = new byte[7 * Integer.BYTES];
        if (fields.size() == 1) {
            final TriangleTreeNode node = nodeFactory.apply(toDecodedTriangle(fields.get(0), scratch));
            extent.addRectangle(node.minX, node.minY, node.maxX, node.maxY);
            return node;
        }
        final TriangleTreeNode[] nodes = new TriangleTreeNode[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            nodes[i] = nodeFactory.apply(toDecodedTriangle(fields.get(i), scratch));
            extent.addRectangle(nodes[i].minX, nodes[i].minY, nodes[i].maxX, nodes[i].maxY);
        }
        return createTree(nodes, 0, fields.size() - 1, true);
    }

    /** Creates tree from sorted components (with range low and high inclusive) */
    private static TriangleTreeNode createTree(TriangleTreeNode[] components, int low, int high, boolean splitX) {
        if (low > high) {
            return null;
        }
        final int mid = (low + high) >>> 1;
        if (low < high) {
            Comparator<TriangleTreeNode> comparator;
            if (splitX) {
                comparator = Comparator.comparingInt((TriangleTreeNode left) -> left.minX).thenComparingInt(left -> left.maxX);
            } else {
                comparator = Comparator.comparingInt((TriangleTreeNode left) -> left.minY).thenComparingInt(left -> left.maxY);
            }
            ArrayUtil.select(components, low, high + 1, mid, comparator);
        }
        TriangleTreeNode newNode = components[mid];
        newNode.left = createTree(components, low, mid - 1, splitX == false);
        newNode.right = createTree(components, mid + 1, high, splitX == false);

        if (newNode.left != null) {
            newNode.maxX = Math.max(newNode.maxX, newNode.left.maxX);
            newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
        }
        if (newNode.right != null) {
            newNode.maxX = Math.max(newNode.maxX, newNode.right.maxX);
            newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
        }
        return newNode;
    }

    /**
     * A node in the triangle tree. Contains shared tree traversal and serialization logic.
     * Subclasses provide format-specific component writing via {@link #writeComponent} and {@link #componentSize}.
     */
    abstract static class TriangleTreeNode {
        final int minY;
        int maxY;
        final int minX;
        int maxX;
        TriangleTreeNode left;
        TriangleTreeNode right;
        final ShapeField.DecodedTriangle component;

        TriangleTreeNode(ShapeField.DecodedTriangle component) {
            this.minY = Math.min(Math.min(component.aY, component.bY), component.cY);
            this.maxY = Math.max(Math.max(component.aY, component.bY), component.cY);
            this.minX = Math.min(Math.min(component.aX, component.bX), component.cX);
            this.maxX = Math.max(Math.max(component.aX, component.bX), component.cX);
            this.component = component;
        }

        abstract void writeComponent(StreamOutput out) throws IOException;

        abstract long componentSize(CountingStreamOutput countingBuffer) throws IOException;

        /**
         * Computes the total size in bytes of the root node's serialized tree.
         * Unlike non-root nodes, the root does NOT write a rightSize prefix before the right child.
         */
        long totalSize(CountingStreamOutput countingBuffer) throws IOException {
            long size = 1; // metadata byte
            size += componentSize(countingBuffer);
            if (left != null) {
                size += left.nodeSize(true, maxX, maxY, countingBuffer);
            }
            if (right != null) {
                size += right.nodeSize(true, maxX, maxY, countingBuffer);
            }
            return size;
        }

        void writeTo(StreamOutput out) throws IOException {
            CountingStreamOutput countingBuffer = new CountingStreamOutput();
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, countingBuffer);
            }
            if (right != null) {
                right.writeNode(out, maxX, maxY, countingBuffer);
            }
        }

        private void writeNode(StreamOutput out, int parentMaxX, int parentMaxY, CountingStreamOutput countingBuffer) throws IOException {
            out.writeVLong((long) parentMaxX - maxX);
            out.writeVLong((long) parentMaxY - maxY);
            long size = nodeSize(false, parentMaxX, parentMaxY, countingBuffer);
            out.writeVInt(Math.toIntExact(size));
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, countingBuffer);
            }
            if (right != null) {
                long rightSize = right.nodeSize(true, maxX, maxY, countingBuffer);
                out.writeVInt(Math.toIntExact(rightSize));
                right.writeNode(out, maxX, maxY, countingBuffer);
            }
        }

        private void writeMetadata(StreamOutput out) throws IOException {
            byte metadata = 0;
            metadata |= left != null ? LEFT : 0;
            metadata |= right != null ? RIGHT : 0;
            if (component.type == ShapeField.DecodedTriangle.TYPE.POINT) {
                metadata |= POINT;
            } else if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                metadata |= LINE;
                metadata |= component.ab ? AB_FROM_TRIANGLE : 0;
            } else {
                metadata |= component.ab ? AB_FROM_TRIANGLE : 0;
                metadata |= component.bc ? BC_FROM_TRIANGLE : 0;
                metadata |= component.ca ? CA_FROM_TRIANGLE : 0;
            }
            out.writeByte(metadata);
        }

        private long nodeSize(boolean includeBox, int parentMaxX, int parentMaxY, CountingStreamOutput countingBuffer) throws IOException {
            long size = 1; // metadata
            size += componentSize(countingBuffer);
            if (left != null) {
                size += left.nodeSize(true, maxX, maxY, countingBuffer);
            }
            if (right != null) {
                long rightSize = right.nodeSize(true, maxX, maxY, countingBuffer);
                countingBuffer.reset();
                countingBuffer.writeVLong(rightSize);
                size += countingBuffer.position();
                size += rightSize;
            }
            if (includeBox) {
                long jumpSize = size;
                countingBuffer.reset();
                countingBuffer.writeVLong((long) parentMaxX - maxX);
                countingBuffer.writeVLong((long) parentMaxY - maxY);
                countingBuffer.writeVLong(jumpSize);
                size += countingBuffer.position();
            }
            return size;
        }
    }

    /** Legacy node that writes coordinate deltas from the node's bounding box maxX/maxY. */
    private static class LegacyNode extends TriangleTreeNode {
        LegacyNode(ShapeField.DecodedTriangle component) {
            super(component);
        }

        @Override
        void writeComponent(StreamOutput out) throws IOException {
            out.writeVLong((long) maxX - component.aX);
            out.writeVLong((long) maxY - component.aY);
            if (component.type == ShapeField.DecodedTriangle.TYPE.POINT) {
                return;
            }
            out.writeVLong((long) maxX - component.bX);
            out.writeVLong((long) maxY - component.bY);
            if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                return;
            }
            out.writeVLong((long) maxX - component.cX);
            out.writeVLong((long) maxY - component.cY);
        }

        @Override
        long componentSize(CountingStreamOutput countingBuffer) throws IOException {
            countingBuffer.reset();
            countingBuffer.writeVLong((long) maxX - component.aX);
            countingBuffer.writeVLong((long) maxY - component.aY);
            if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                countingBuffer.writeVLong((long) maxX - component.bX);
                countingBuffer.writeVLong((long) maxY - component.bY);
            } else if (component.type == ShapeField.DecodedTriangle.TYPE.TRIANGLE) {
                countingBuffer.writeVLong((long) maxX - component.bX);
                countingBuffer.writeVLong((long) maxY - component.bY);
                countingBuffer.writeVLong((long) maxX - component.cX);
                countingBuffer.writeVLong((long) maxY - component.cY);
            }
            return countingBuffer.position();
        }
    }
}
