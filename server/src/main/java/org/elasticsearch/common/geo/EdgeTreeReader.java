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

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;

/**
 * This {@link ShapeTreeReader} understands how to parse polygons
 * serialized with the {@link EdgeTreeWriter}
 */
public class EdgeTreeReader implements ShapeTreeReader {
    private final ByteBufferStreamInput input;
    private final int startPosition;
    private final boolean hasArea;
    private final Extent extent;
    private final Map<Integer, Edge> cachedEdges;

    public EdgeTreeReader(ByteBufferStreamInput input, boolean hasArea) throws IOException {
        this.startPosition = input.position();
        this.input = input;
        this.hasArea = hasArea;
        this.extent = getExtent();
        this.cachedEdges = new LinkedHashMap<>(10, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > 1000;
            }
        };
    }

    public Extent getExtent() throws IOException {
        resetInputPosition();
        return new Extent(input);
    }

    /**
     * Returns true if the rectangle query and the edge tree's shape overlap
     */
    public boolean intersects(Extent extent) throws IOException {
        if (hasArea) {
            return containsBottomLeft(extent) || crosses(extent);
        } else {
            return crosses(extent);
        }
    }

    static Optional<Boolean> checkExtent(Extent treeExtent, Extent extent) throws IOException {
        if (treeExtent.minY() > extent.maxY() || treeExtent.maxX() < extent.minX()
                || treeExtent.maxY() < extent.minY() || treeExtent.minX() > extent.maxX()) {
            return Optional.of(false); // tree and bbox-query are disjoint
        }

        if (extent.minX() <= treeExtent.minX() && extent.minY() <= treeExtent.minY()
                && extent.maxX() >= treeExtent.maxX() && extent.maxY() >= treeExtent.maxY()) {
            return Optional.of(true); // bbox-query fully contains tree's extent.
        }
        return Optional.empty();
    }

    boolean containsBottomLeft(Extent extent) throws IOException {
        resetToRootEdge();
        Optional<Boolean> extentCheck = checkExtent(this.extent, extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        return containsBottomLeft(readRoot(input.position()), extent);
    }

    boolean containsFully(Extent extent) throws IOException {
        resetToRootEdge();
        return containsFully(readRoot(input.position()), extent);
    }

    public boolean crosses(Extent extent) throws IOException {
        resetToRootEdge();
        Optional<Boolean> extentCheck = checkExtent(this.extent, extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        return crosses(readRoot(input.position()), extent);
    }

    private Edge readRoot(int position) throws IOException {
        input.position(position);
        if (input.readBoolean()) {
            return readEdge(input.position());
        }
        return null;
    }

    private Edge readEdge(int position) throws IOException {
        Edge edge = cachedEdges.get(position);
        if (edge == null) {
            input.position(position);
            int maxY = Math.toIntExact(extent.maxY() - input.readVLong());
            int minY = Math.toIntExact(extent.maxY() - input.readVLong());
            int x1 = Math.toIntExact(extent.maxX() - input.readVLong());
            int x2 = Math.toIntExact(extent.maxX() - input.readVLong());
            int y1 = Math.toIntExact(extent.maxY() - input.readVLong());
            int y2 = Math.toIntExact(extent.maxY() - input.readVLong());
            int rightOffset = input.readVInt();
            if (rightOffset == 1) {
                rightOffset = 0;
            } else if (rightOffset == 0) {
                rightOffset = -1;
            }
            edge = new Edge(input.position(), x1, y1, x2, y2, minY, maxY, rightOffset);
            cachedEdges.put(position, edge);
            return edge;
        } else {
            return edge;
        }
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
        if (root.maxY >= extent.minY()) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from bottom-left of bbox-query to see if it intersects edge
            if (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.minX(), extent.minY(), Integer.MAX_VALUE,
                    extent.minY())) {
                res = true;
            }

            if (root.rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(readLeft(root), extent);
            }

            if (root.rightOffset >= 0 && extent.maxY() >= root.minY) { /* no right node if rightOffset == -1 */
                res ^= containsBottomLeft(readRight(root), extent);
            }
        }
        return res;
    }

    /**
     * Returns true if every corner in the rectangle query is contained within the tree's edges.
     */
    private boolean containsFully(Edge root, Extent extent) throws IOException {
        boolean res = false;
        if (root.maxY >= extent.minY()) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from each corner of the extent
            if (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.minX(), extent.minY(),
                    Integer.MAX_VALUE, extent.minY())
                && lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.minX(), extent.maxY(),
                    Integer.MAX_VALUE, extent.maxY())
                && lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.maxX(), extent.minY(),
                    Integer.MAX_VALUE, extent.minY())
                && lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2, extent.maxX(), extent.maxY(),
                    Integer.MAX_VALUE, extent.maxY())
            ) {
                res = true;
            }

            if (root.rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(readLeft(root), extent);
            }

            if (root.rightOffset >= 0 && extent.maxY() >= root.minY) { /* no right node if rightOffset == -1 */
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
        if (root.maxY >= extent.minY()) {

            // does rectangle's edges intersect or reside inside polygon's edge
            if (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                extent.minX(), extent.minY(), extent.maxX(), extent.minY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX(), extent.minY(), extent.maxX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX(), extent.maxY(), extent.minX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.minX(), extent.maxY(), extent.minX(), extent.minY())) {
                return true;
            }

            // does this edge fully reside within the rectangle's area
            if (extent.minX() <= Math.min(root.x1, root.x2) && extent.minY() <= Math.min(root.y1, root.y2)
                && extent.maxX() >= Math.max(root.x1, root.x2) && extent.maxY() >= Math.max(root.y1, root.y2)) {
                return true;
            }

            /* has left node */
            if (root.rightOffset > 0 && crosses(readLeft(root), extent)) {
                return true;
            }

            /* no right node if rightOffset == -1 */
            if (root.rightOffset >= 0 && extent.maxY() >= root.minY && crosses(readRight(root), extent)) {
                return true;
            }
        }
        return false;
    }

    private void resetInputPosition() throws IOException {
        input.position(startPosition);
    }

    private void resetToRootEdge() throws IOException {
        input.position(startPosition + Extent.WRITEABLE_SIZE_IN_BYTES); // skip extent
    }

    private static final class Edge {
        final int streamOffset;
        final int x1;
        final int y1;
        final int x2;
        final int y2;
        final int minY;
        final int maxY;
        final int rightOffset;

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
