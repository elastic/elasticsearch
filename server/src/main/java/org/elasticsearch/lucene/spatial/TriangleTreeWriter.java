/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

/**
 * This is a tree-writer that serializes a list of {@link ShapeField.DecodedTriangle} as an interval tree
 * into a byte array.
 */
public class TriangleTreeWriter {

    static final byte LEFT = 1;
    static final byte RIGHT = 1 << 1;
    static final byte POINT = 1 << 2;
    static final byte LINE = 1 << 3;
    static final byte AB_FROM_TRIANGLE = 1 << 4;
    static final byte BC_FROM_TRIANGLE = 1 << 5;
    static final byte CA_FROM_TRIANGLE = 1 << 6;

    private TriangleTreeWriter() {}

    /*** Serialize the interval tree in the provided data output */
    public static void writeTo(StreamOutput out, List<IndexableField> fields) throws IOException {
        final Extent extent = new Extent();
        final TriangleTreeNode node = build(fields, extent);
        ;
        extent.writeCompressed(out);
        node.writeTo(out);
    }

    private static TriangleTreeNode build(List<IndexableField> fields, Extent extent) {
        final byte[] scratch = new byte[7 * Integer.BYTES];
        if (fields.size() == 1) {
            final TriangleTreeNode triangleTreeNode = new TriangleTreeNode(toDecodedTriangle(fields.get(0), scratch));
            extent.addRectangle(triangleTreeNode.minX, triangleTreeNode.minY, triangleTreeNode.maxX, triangleTreeNode.maxY);
            return triangleTreeNode;
        }
        final TriangleTreeNode[] nodes = new TriangleTreeNode[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            nodes[i] = new TriangleTreeNode(toDecodedTriangle(fields.get(i), scratch));
            extent.addRectangle(nodes[i].minX, nodes[i].minY, nodes[i].maxX, nodes[i].maxY);
        }
        return createTree(nodes, 0, fields.size() - 1, true);
    }

    private static ShapeField.DecodedTriangle toDecodedTriangle(IndexableField field, byte[] scratch) {
        final BytesRef bytesRef = field.binaryValue();
        assert bytesRef.length == 7 * Integer.BYTES;
        System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
        final ShapeField.DecodedTriangle decodedTriangle = new ShapeField.DecodedTriangle();
        ShapeField.decodeTriangle(scratch, decodedTriangle);
        return decodedTriangle;
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
        // find children
        newNode.left = createTree(components, low, mid - 1, splitX == false);
        newNode.right = createTree(components, mid + 1, high, splitX == false);

        // pull up max values to this node
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

    /** Represents an inner node of the tree. */
    private static class TriangleTreeNode {
        /** minimum latitude of this geometry's bounding box area */
        private final int minY;
        /** maximum latitude of this geometry's bounding box area */
        private int maxY;
        /** minimum longitude of this geometry's bounding box area */
        private final int minX;
        /**  maximum longitude of this geometry's bounding box area */
        private int maxX;
        // child components, or null.
        private TriangleTreeNode left;
        private TriangleTreeNode right;
        /** root node of edge tree */
        private final ShapeField.DecodedTriangle component;

        private TriangleTreeNode(ShapeField.DecodedTriangle component) {
            this.minY = Math.min(Math.min(component.aY, component.bY), component.cY);
            this.maxY = Math.max(Math.max(component.aY, component.bY), component.cY);
            this.minX = Math.min(Math.min(component.aX, component.bX), component.cX);
            this.maxX = Math.max(Math.max(component.aX, component.bX), component.cX);
            this.component = component;
        }

        private void writeTo(StreamOutput out) throws IOException {
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

        private void writeComponent(StreamOutput out) throws IOException {
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

        private long nodeSize(boolean includeBox, int parentMaxX, int parentMaxY, CountingStreamOutput countingBuffer) throws IOException {
            long size = 0;
            size++; // metadata
            size += componentSize(countingBuffer);
            if (left != null) {
                size += left.nodeSize(true, maxX, maxY, countingBuffer);
            }
            if (right != null) {
                long rightSize = right.nodeSize(true, maxX, maxY, countingBuffer);
                countingBuffer.reset();
                countingBuffer.writeVLong(rightSize);
                size += countingBuffer.size(); // jump size
                size += rightSize;
            }
            if (includeBox) {
                long jumpSize = size;
                countingBuffer.reset();
                countingBuffer.writeVLong((long) parentMaxX - maxX);
                countingBuffer.writeVLong((long) parentMaxY - maxY);
                countingBuffer.writeVLong(jumpSize);
                size += countingBuffer.size(); // box size
            }
            return size;
        }

        private long componentSize(CountingStreamOutput countingBuffer) throws IOException {
            countingBuffer.reset();
            if (component.type == ShapeField.DecodedTriangle.TYPE.POINT) {
                countingBuffer.writeVLong((long) maxX - component.aX);
                countingBuffer.writeVLong((long) maxY - component.aY);
            } else if (component.type == ShapeField.DecodedTriangle.TYPE.LINE) {
                countingBuffer.writeVLong((long) maxX - component.aX);
                countingBuffer.writeVLong((long) maxY - component.aY);
                countingBuffer.writeVLong((long) maxX - component.bX);
                countingBuffer.writeVLong((long) maxY - component.bY);
            } else {
                countingBuffer.writeVLong((long) maxX - component.aX);
                countingBuffer.writeVLong((long) maxY - component.aY);
                countingBuffer.writeVLong((long) maxX - component.bX);
                countingBuffer.writeVLong((long) maxY - component.bY);
                countingBuffer.writeVLong((long) maxX - component.cX);
                countingBuffer.writeVLong((long) maxY - component.cY);
            }
            return countingBuffer.size();
        }
    }
}
