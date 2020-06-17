/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * This is a tree-writer that serializes a list of {@link ShapeField.DecodedTriangle} as an interval tree
 * into a byte array.
 */
public class TriangleTreeWriter {

    private final TriangleTreeNode node;
    private final CoordinateEncoder coordinateEncoder;
    private final CentroidCalculator centroidCalculator;
    private Extent extent;

    public TriangleTreeWriter(List<ShapeField.DecodedTriangle> triangles, CoordinateEncoder coordinateEncoder,
                              CentroidCalculator centroidCalculator) {
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = centroidCalculator;
        this.extent = new Extent();
        this.node = build(triangles);
    }

    /*** Serialize the interval tree in the provided data output */
    public void writeTo(ByteBuffersDataOutput out) throws IOException {
        out.writeInt(coordinateEncoder.encodeX(centroidCalculator.getX()));
        out.writeInt(coordinateEncoder.encodeY(centroidCalculator.getY()));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));
        extent.writeCompressed(out);
        node.writeTo(out);
    }

    private void addToExtent(TriangleTreeNode treeNode) {
        extent.addRectangle(treeNode.minX, treeNode.minY, treeNode.maxX, treeNode.maxY);
    }

    private TriangleTreeNode build(List<ShapeField.DecodedTriangle> triangles) {
        if (triangles.size() == 1) {
            TriangleTreeNode triangleTreeNode =  new TriangleTreeNode(triangles.get(0));
            addToExtent(triangleTreeNode);
            return triangleTreeNode;
        }
        TriangleTreeNode[] nodes = new TriangleTreeNode[triangles.size()];
        for (int i = 0; i < triangles.size(); i++) {
            nodes[i] = new TriangleTreeNode(triangles.get(i));
            addToExtent(nodes[i]);
        }
        return createTree(nodes, 0, triangles.size() - 1, true);
    }

    /** Creates tree from sorted components (with range low and high inclusive) */
    private TriangleTreeNode createTree(TriangleTreeNode[] components, int low, int high, boolean splitX) {
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
        newNode.left = createTree(components, low, mid - 1, !splitX);
        newNode.right = createTree(components, mid + 1, high, !splitX);

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
        /** type of component */
        public enum TYPE {
            POINT, LINE, TRIANGLE
        }
        /** minimum latitude of this geometry's bounding box area */
        private int minY;
        /** maximum latitude of this geometry's bounding box area */
        private int maxY;
        /** minimum longitude of this geometry's bounding box area */
        private int minX;
        /**  maximum longitude of this geometry's bounding box area */
        private int maxX;
        // child components, or null.
        private TriangleTreeNode left;
        private TriangleTreeNode right;
        /** root node of edge tree */
        private final ShapeField.DecodedTriangle component;
        /** component type */
        private final TYPE type;

        private TriangleTreeNode(ShapeField.DecodedTriangle component) {
            this.minY = Math.min(Math.min(component.aY, component.bY), component.cY);
            this.maxY = Math.max(Math.max(component.aY, component.bY), component.cY);
            this.minX = Math.min(Math.min(component.aX, component.bX), component.cX);
            this.maxX = Math.max(Math.max(component.aX, component.bX), component.cX);
            this.component = component;
            this.type = getType(component);
        }

        private static TYPE getType(ShapeField.DecodedTriangle triangle) {
            // the issue in lucene: https://github.com/apache/lucene-solr/pull/927
            // can help here
            if (triangle.aX == triangle.bX && triangle.aY == triangle.bY) {
                if (triangle.aX == triangle.cX && triangle.aY == triangle.cY) {
                    return TYPE.POINT;
                }
                return TYPE.LINE;
            } else if ((triangle.aX == triangle.cX && triangle.aY == triangle.cY) ||
                (triangle.bX == triangle.cX && triangle.bY == triangle.cY)) {
                return TYPE.LINE;
            } else {
                return TYPE.TRIANGLE;
            }
        }

        private void writeTo(ByteBuffersDataOutput out) throws IOException {
            ByteBuffersDataOutput scratchBuffer = ByteBuffersDataOutput.newResettableInstance();
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                right.writeNode(out, maxX, maxY, scratchBuffer);
            }
        }

        private void writeNode(ByteBuffersDataOutput out, int parentMaxX, int parentMaxY,
                               ByteBuffersDataOutput scratchBuffer) throws IOException {
            out.writeVLong((long) parentMaxX - maxX);
            out.writeVLong((long) parentMaxY - maxY);
            int size = nodeSize(false, parentMaxX, parentMaxY, scratchBuffer);
            out.writeVInt(size);
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY, scratchBuffer);
                out.writeVInt(rightSize);
                right.writeNode(out, maxX, maxY, scratchBuffer);
            }
        }

        private void writeMetadata(ByteBuffersDataOutput out) {
            byte metadata = 0;
            metadata |= (left != null) ? (1 << 0) : 0;
            metadata |= (right != null) ? (1 << 1) : 0;
            if (type == TYPE.POINT) {
                metadata |= (1 << 2);
            } else if (type == TYPE.LINE) {
                metadata |= (1 << 3);
            } else {
                metadata |= (component.ab) ? (1 << 4) : 0;
                metadata |= (component.bc) ? (1 << 5) : 0;
                metadata |= (component.ca) ? (1 << 6) : 0;
            }
            out.writeByte(metadata);
        }

        private void writeComponent(ByteBuffersDataOutput out) throws IOException {
            if (type == TYPE.POINT) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
            } else if (type == TYPE.LINE) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
            } else {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
                out.writeVLong((long) maxX - component.cX);
                out.writeVLong((long) maxY - component.cY);
            }
        }

        private int nodeSize(boolean includeBox, int parentMaxX, int parentMaxY, ByteBuffersDataOutput scratchBuffer) throws IOException {
            int size =0;
            size++; //metadata
            size += componentSize(scratchBuffer);
            if (left != null) {
                size += left.nodeSize(true, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY, scratchBuffer);
                scratchBuffer.reset();
                scratchBuffer.writeVLong(rightSize);
                size += scratchBuffer.size(); // jump size
                size += rightSize;
            }
            if (includeBox) {
                int jumpSize = size;
                scratchBuffer.reset();
                scratchBuffer.writeVLong((long) parentMaxX - maxX);
                scratchBuffer.writeVLong((long) parentMaxY - maxY);
                scratchBuffer.writeVLong(jumpSize);
                size += scratchBuffer.size(); // box size
            }
            return size;
        }

        private int componentSize(ByteBuffersDataOutput scratchBuffer) throws IOException {
            scratchBuffer.reset();
            if (type == TYPE.POINT) {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
            } else if (type == TYPE.LINE) {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
                scratchBuffer.writeVLong((long) maxX - component.bX);
                scratchBuffer.writeVLong((long) maxY - component.bY);
            } else {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
                scratchBuffer.writeVLong((long) maxX - component.bX);
                scratchBuffer.writeVLong((long) maxY - component.bY);
                scratchBuffer.writeVLong((long) maxX - component.cX);
                scratchBuffer.writeVLong((long) maxY - component.cY);
            }
            return Math.toIntExact(scratchBuffer.size());
        }
    }
}
