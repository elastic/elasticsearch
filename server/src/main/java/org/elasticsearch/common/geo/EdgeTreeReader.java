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
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;

public class EdgeTreeReader {
    final BytesRef bytesRef;
    final ByteBufferStreamInput input;

    public EdgeTreeReader(StreamInput input) throws IOException {
        int treeBytesSize = input.readVInt();
        this.bytesRef = input.readBytesRef(treeBytesSize);
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
    }

    public Extent getExtent() throws IOException {
        input.position(0);
        int thisMinX = input.readInt();
        int thisMinY = input.readInt();
        int thisMaxX = input.readInt();
        int thisMaxY = input.readInt();
        return new Extent(thisMinX, thisMinY, thisMaxX, thisMaxY);
    }

    /**
     * Returns true if the rectangle query and the edge tree's shape overlap
     */
    public boolean containedInOrCrosses(int minX, int minY, int maxX, int maxY) throws IOException {
        Extent extent = new Extent(minX, minY, maxX, maxY);
        return this.containsBottomLeft(extent, true) || this.crosses(extent, true);
    }

    static Optional<Boolean> checkExtent(StreamInput input, Extent extent) throws IOException {
        int thisMinX = input.readInt();
        int thisMinY = input.readInt();
        int thisMaxX = input.readInt();
        int thisMaxY = input.readInt();

        if (thisMinY > extent.maxY || thisMaxX < extent.minX || thisMaxY < extent.minY || thisMinX > extent.maxX) {
            return Optional.of(false); // tree and bbox-query are disjoint
        }

        if (extent.minX <= thisMinX && extent.minY <= thisMinY && extent.maxX >= thisMaxX && extent.maxY >= thisMaxY) {
            return Optional.of(true); // bbox-query fully contains tree's extent.
        }
        return Optional.empty();
    }

    boolean containsBottomLeft(Extent extent, boolean resetInput) throws IOException {
        if (resetInput) {
            input.position(0);
        }

        Optional<Boolean> extentCheck = checkExtent(input, extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        return containsBottomLeft(readRoot(input.position()), extent);
    }

    public boolean crosses(Extent extent, boolean resetInput) throws IOException {
        if (resetInput) {
            input.position(0);
        }

        Optional<Boolean> extentCheck = checkExtent(input, extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        return crosses(readRoot(input.position()), extent);
    }

    public Edge readRoot(int position) throws IOException {
        return readEdge(position);
    }

    private Edge readEdge(int position) throws IOException {
        input.position(position);
        int minY = input.readInt();
        int maxY = input.readInt();
        int x1 = input.readInt();
        int y1 = input.readInt();
        int x2 = input.readInt();
        int y2 = input.readInt();
        int rightOffset = input.readInt();
        return new Edge(input.position(), x1, y1, x2, y2, minY, maxY, rightOffset);
    }


    Edge readLeft(Edge root) throws IOException {
        return readEdge(root.streamOffset);
    }

    Edge readRight(Edge root) throws IOException {
        return readEdge(root.streamOffset + root.rightOffset);
    }

    /**
     * Returns true if the bottom-left point of the rectangle query is contained within the
     * tree's edges.
     */
    private boolean containsBottomLeft(Edge root, Extent extent) throws IOException {
        boolean res = false;
        if (root.maxY >= extent.minY) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from bottom-left of bbox-query to see if it intersects edge
            if (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.minX, extent.minY, Integer.MAX_VALUE, extent.minY)) {
                res = true;
            }

            if (root.rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(readLeft(root), extent);
            }

            if (root.rightOffset > 0 && extent.maxY >= root.minY) { /* no right node if rightOffset == -1 */
                res ^= containsBottomLeft(readRight(root), extent);
            }
        }
        return res;
    }

    /**
     * Returns true if the box crosses any edge in this edge subtree
     * */
    private boolean crosses(Edge root, Extent extent) throws IOException {
        // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
        if (root.maxY >= extent.minY) {

            // does rectangle's edges intersect or reside inside polygon's edge
            if (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                extent.minX, extent.minY, extent.maxX, extent.minY) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX, extent.minY, extent.maxX, extent.maxY) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX, extent.maxY, extent.minX, extent.maxY) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.minX, extent.maxY, extent.minX, extent.minY)) {
                return true;
            }
            /* has left node */
            if (root.rightOffset > 0 && crosses(readLeft(root), extent)) {
                return true;
            }

            /* no right node if rightOffset == -1 */
            if (root.rightOffset > 0 && extent.maxY >= root.minY && crosses(readRight(root), extent)) {
                return true;
            }
        }
        return false;
    }

    static final class Extent {
        final int minX;
        final int minY;
        final int maxX;
        final int maxY;

        Extent(int minX, int minY, int maxX, int maxY) {
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
        }
    }

    private static class Edge {
        int streamOffset;
        int x1;
        int y1;
        int x2;
        int y2;
        int minY;
        int maxY;
        int rightOffset;

        /**
         * Object representing an edge node read from bytes
         *
         * @param streamOffset offset in byte-reference where edge terminates
         * @param x1 x-coordinate of first point in segment
         * @param y1 y-coordinate of first point in segment
         * @param x2 x-coordinate of second point in segment
         * @param y2 y-coordinate of second point in segment
         * @param minY minimum y-coordinate in this edge-node's tree
         * @param maxY maximum y-coordinate in this edge-node's tree
         * @param rightOffset the start offset in the byte-reference of the right edge-node
         */
        Edge(int streamOffset, int x1, int y1, int x2, int y2, int minY, int maxY, int rightOffset) {
            this.streamOffset = streamOffset;
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.minY = minY;
            this.maxY = maxY;
            this.rightOffset = rightOffset;
        }
    }
}
