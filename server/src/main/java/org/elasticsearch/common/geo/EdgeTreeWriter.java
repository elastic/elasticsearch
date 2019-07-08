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

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shape edge-tree writer for use in doc-values
 */
public class EdgeTreeWriter extends ShapeTreeWriter {

    /**
     * | minY | maxY | x1 | y1 | x2 | y2 | right_offset |
     */
    static final int EDGE_SIZE_IN_BYTES = 28;

    private final Extent extent;
    private final boolean hasArea;
    private final int numShapes;
    final Edge tree;


    /**
     * @param x        array of the x-coordinate of points.
     * @param y        array of the y-coordinate of points.
     * @param hasArea  true if edge-tree represents a {@link Polygon} and has a non-zero area, false otherwise.
     */
    EdgeTreeWriter(int[] x, int[] y, boolean hasArea) {
        this(Collections.singletonList(x), Collections.singletonList(y), hasArea);
    }

    EdgeTreeWriter(List<int[]> x, List<int[]> y, boolean hasArea) {
        this.numShapes = x.size();
        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < y.size(); i++) {
            for (int j = 1; j < y.get(i).length; j++) {
                int y1 = y.get(i)[j - 1];
                int x1 = x.get(i)[j - 1];
                int y2 = y.get(i)[j];
                int x2 = x.get(i)[j];
                int edgeMinY, edgeMaxY;
                if (y1 < y2) {
                    edgeMinY = y1;
                    edgeMaxY = y2;
                } else {
                    edgeMinY = y2;
                    edgeMaxY = y1;
                }
                edges.add(new Edge(x1, y1, x2, y2, edgeMinY, edgeMaxY));
                minX = Math.min(minX, Math.min(x1, x2));
                minY = Math.min(minY, Math.min(y1, y2));
                maxX = Math.max(maxX, Math.max(x1, x2));
                maxY = Math.max(maxY, Math.max(y1, y2));
            }
        }
        edges.sort(Edge::compareTo);
        this.extent = new Extent(minX, minY, maxX, maxY);
        this.tree = createTree(edges, 0, edges.size() - 1);
        this.hasArea = hasArea;
    }

    @Override
    public Extent getExtent() {
        return extent;
    }

    @Override
    public ShapeType getShapeType() {
        return hasArea ? ShapeType.POLYGON : (numShapes > 1 ? ShapeType.MULTILINESTRING: ShapeType.LINESTRING);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        extent.writeTo(out);
        tree.writeTo(out);
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
    static class Edge implements Comparable<Edge>, Writeable {
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(minY);
            out.writeInt(maxY);
            out.writeInt(x1);
            out.writeInt(y1);
            out.writeInt(x2);
            out.writeInt(y2);
            // left node is next node, write offset of right node
            if (left != null) {
                out.writeInt(left.size * EDGE_SIZE_IN_BYTES);
            } else if (right == null){
                out.writeInt(-1);
            } else {
                out.writeInt(0);
            }
            if (left != null) {
                left.writeTo(out);
            }
            if (right != null) {
                right.writeTo(out);
            }
        }
    }
}
