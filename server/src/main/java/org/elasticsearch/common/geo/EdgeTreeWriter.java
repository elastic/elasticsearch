/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.ShapeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shape edge-tree writer for use in doc-values
 */
public class EdgeTreeWriter extends ShapeTreeWriter {

    private final Extent extent;
    private final int numShapes;
    private final CentroidCalculator centroidCalculator;
    final Edge tree;


    /**
     * @param x                  array of the x-coordinate of points.
     * @param y                  array of the y-coordinate of points.
     * @param coordinateEncoder  class that encodes from real-valued x/y to serialized integer coordinate values.
     * @param hasArea            whether the tree represents a Polygon that has a defined area
     */
    EdgeTreeWriter(double[] x, double[] y, CoordinateEncoder coordinateEncoder, boolean hasArea) {
        this(Collections.singletonList(x), Collections.singletonList(y), coordinateEncoder, hasArea);
    }

    EdgeTreeWriter(List<double[]> x, List<double[]> y, CoordinateEncoder coordinateEncoder, boolean hasArea) {
        this.centroidCalculator = new CentroidCalculator();
        this.numShapes = x.size();
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;

        List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < y.size(); i++) {
            for (int j = 1; j < y.get(i).length; j++) {
                double y1 = y.get(i)[j - 1];
                double x1 = x.get(i)[j - 1];
                double y2 = y.get(i)[j];
                double x2 = x.get(i)[j];
                double edgeMinY, edgeMaxY;
                if (y1 < y2) {
                    edgeMinY = y1;
                    edgeMaxY = y2;
                } else {
                    edgeMinY = y2;
                    edgeMaxY = y1;
                }
                edges.add(new Edge(coordinateEncoder.encodeX(x1), coordinateEncoder.encodeY(y1),
                    coordinateEncoder.encodeX(x2), coordinateEncoder.encodeY(y2),
                    coordinateEncoder.encodeY(edgeMinY), coordinateEncoder.encodeY(edgeMaxY)));

                top = Math.max(top, Math.max(y1, y2));
                bottom = Math.min(bottom, Math.min(y1, y2));

                // check first
                if (x1 >= 0 && x1 < posLeft) {
                    posLeft = x1;
                }
                if (x1 >= 0 && x1 > posRight) {
                    posRight = x1;
                }
                if (x1 < 0 && x1 < negLeft) {
                    negLeft = x1;
                }
                if (x1 < 0 && x1 > negRight) {
                    negRight = x1;
                }

                // check second
                if (x2 >= 0 && x2 < posLeft) {
                    posLeft = x2;
                }
                if (x2 >= 0 && x2 > posRight) {
                    posRight = x2;
                }
                if (x2 < 0 && x2 < negLeft) {
                    negLeft = x2;
                }
                if (x2 < 0 && x2 > negRight) {
                    negRight = x2;
                }

                // calculate centroid
                centroidCalculator.addCoordinate(x1, y1);
                if (j == y.get(i).length - 1 && hasArea == false) {
                    centroidCalculator.addCoordinate(x2, y2);
                }
            }
        }
        edges.sort(Edge::compareTo);
        this.extent = new Extent(coordinateEncoder.encodeY(top), coordinateEncoder.encodeY(bottom),
            coordinateEncoder.encodeX(negLeft), coordinateEncoder.encodeX(negRight),
            coordinateEncoder.encodeX(posLeft), coordinateEncoder.encodeX(posRight));
        this.tree = createTree(edges, 0, edges.size() - 1);
    }

    @Override
    public Extent getExtent() {
        return extent;
    }

    @Override
    public ShapeType getShapeType() {
        return numShapes > 1 ? ShapeType.MULTILINESTRING: ShapeType.LINESTRING;
    }

    @Override
    public CentroidCalculator getCentroidCalculator() {
        return centroidCalculator;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        extent.writeTo(out);
        if (tree != null) {
            out.writeBoolean(true);
            tree.writeTo(out, new BytesStreamOutput(), extent);
        } else {
            out.writeBoolean(false);
        }
    }

    private static Edge createTree(List<Edge> edges, int low, int high) {
        if (low > high) {
            return null;
        }
        // add midpoint
        int mid = (low + high) >>> 1;
        Edge newNode = edges.get(mid);
        newNode.size = 1;
        // add children
        newNode.left = createTree(edges, low, mid - 1);
        newNode.right = createTree(edges, mid + 1, high);
        // pull up max values to this node
        // and node count
        if (newNode.left != null) {
            newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
            newNode.size += newNode.left.size;
        }
        if (newNode.right != null) {
            newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
            newNode.size += newNode.right.size;
        }
        return newNode;
    }

    /**
     * Object representing an in-memory edge-tree to be serialized
     */
    static class Edge implements Comparable<Edge> {
        final int x1;
        final int y1;
        final int x2;
        final int y2;
        int minY;
        int maxY;
        int size;
        Edge left;
        Edge right;

        Edge(int x1, int y1, int x2, int y2, int minY, int maxY) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.minY = minY;
            this.maxY = maxY;
        }

        @Override
        public int compareTo(Edge other) {
            int ret = Integer.compare(minY, other.minY);
            if (ret == 0) {
                ret = Integer.compare(maxY, other.maxY);
            }
            return ret;
        }

        private int writeEdgeContent(StreamOutput out, Extent extent) throws IOException {
            long startPosition = out.position();
            out.writeVLong((long) extent.maxY() - maxY);
            out.writeVLong((long) extent.maxY() - minY);
            out.writeVLong((long) extent.maxX() - x1);
            out.writeVLong((long) extent.maxX() - x2);
            out.writeVLong((long) extent.maxY() - y1);
            out.writeVLong((long) extent.maxY() - y2);
            return Math.toIntExact(out.position() - startPosition);
        }

        private void writeTo(StreamOutput out, BytesStreamOutput scratchBuffer, Extent extent) throws IOException {
            writeEdgeContent(out, extent);
            // left node is next node, write offset of right node
            if (left != null) {
                out.writeVInt(left.size(scratchBuffer, extent));
            } else if (right == null){
                out.writeVInt(0);
            } else {
                out.writeVInt(1);
            }
            if (left != null) {
                left.writeTo(out, scratchBuffer, extent);
            }
            if (right != null) {
                right.writeTo(out, scratchBuffer, extent);
            }
        }

        private int size(BytesStreamOutput scratchBuffer, Extent extent) throws IOException {
            int size = writeEdgeContent(scratchBuffer, extent);
            scratchBuffer.reset();
            // left node is next node, write offset of right node
            if (left != null) {
                int leftSize = left.size(scratchBuffer, extent);
                scratchBuffer.reset();
                scratchBuffer.writeVInt(leftSize);
            } else if (right == null){
                scratchBuffer.writeVInt(0);
            } else {
                scratchBuffer.writeVInt(1);
            }
            size += scratchBuffer.size();
            if (left != null) {
                size += left.size(scratchBuffer, extent);
            }
            if (right != null) {
                size += right.size(scratchBuffer, extent);
            }
            return size;
        }
    }
}
