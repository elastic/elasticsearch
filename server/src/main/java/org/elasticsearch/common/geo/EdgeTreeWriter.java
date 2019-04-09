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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class EdgeTreeWriter {

    /**
     * | minY | maxY | x1 | y1 | x2 | y2 | right_offset |
     */
    static final int EDGE_SIZE_IN_BYTES = 28;

    int minX = Integer.MAX_VALUE;
    int minY = Integer.MAX_VALUE;
    int maxX = Integer.MIN_VALUE;
    int maxY = Integer.MIN_VALUE;
    final Edge tree;

    public EdgeTreeWriter(int[] x, int[] y) {
        Edge edges[] = new Edge[y.length - 1];
        for (int i = 1; i < y.length; i++) {
            int y1 = y[i-1];
            int x1 = x[i-1];
            int y2 = y[i];
            int x2 = x[i];
            int minY, maxY;
            if (y1 < y2) {
                minY = y1;
                maxY = y2;
            } else {
                minY = y2;
                maxY = y1;
            }
            edges[i - 1] = new Edge(x1, y1, x2, y2, minY, maxY);
            this.minX = Math.min(this.minX, Math.min(x1, x2));
            this.minY = Math.min(this.minY, Math.min(y1, y2));
            this.maxX = Math.max(this.maxX, Math.max(x1, x2));
            this.maxY = Math.max(this.maxY, Math.max(y1, y2));
        }
        Arrays.sort(edges);
        this.tree = createTree(edges, 0, edges.length - 1);
    }

    public BytesRef toBytesRef() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput(4 * 4 + EDGE_SIZE_IN_BYTES * tree.size);
        output.writeInt(minX);
        output.writeInt(minY);
        output.writeInt(maxX);
        output.writeInt(maxY);
        writeTree(tree, output);
        output.close();
        return output.bytes().toBytesRef();
    }

    private void writeTree(Edge edge, StreamOutput output) throws IOException {
        if (edge == null) {
            return;
        }
        output.writeInt(edge.minY);
        output.writeInt(edge.maxY);
        output.writeInt(edge.x1);
        output.writeInt(edge.y1);
        output.writeInt(edge.x2);
        output.writeInt(edge.y2);
        // left node is next node, write offset of right node
        if (edge.left != null) {
            output.writeInt(edge.left.size * EDGE_SIZE_IN_BYTES);
        } else if (edge.right == null){
            output.writeInt(-1);
        } else {
            output.writeInt(0);
        }
        writeTree(edge.left, output);
        writeTree(edge.right, output);
    }

    private static Edge createTree(Edge edges[], int low, int high) {
        if (low > high) {
            return null;
        }
        // add midpoint
        int mid = (low + high) >>> 1;
        Edge newNode = edges[mid];
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
    }
}
