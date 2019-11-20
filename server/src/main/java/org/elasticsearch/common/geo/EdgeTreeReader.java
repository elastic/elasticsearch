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
    private static final Optional<Boolean> OPTIONAL_FALSE = Optional.of(false);
    private static final Optional<Boolean> OPTIONAL_TRUE = Optional.of(true);
    private static final Optional<Boolean> OPTIONAL_EMPTY = Optional.empty();

    public EdgeTreeReader(ByteBufferStreamInput input, boolean hasArea) throws IOException {
        this.startPosition = input.position();
        this.input = input;
        this.hasArea = hasArea;
    }

    public Extent getExtent() throws IOException {
        resetInputPosition();
        return new Extent(input);
    }

    /**
     * Returns true if the rectangle query and the edge tree's shape overlap
     */
    @Override
    public GeoRelation relate(Extent extent) throws IOException {
        if (crosses(extent)) {
            return GeoRelation.QUERY_CROSSES;
        } else if (hasArea && containsBottomLeft(extent)){
            return GeoRelation.QUERY_INSIDE;
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    static Optional<Boolean> checkExtent(Extent treeExtent, Extent extent) throws IOException {
        if (treeExtent.minY() > extent.maxY() || treeExtent.maxX() < extent.minX()
                || treeExtent.maxY() < extent.minY() || treeExtent.minX() > extent.maxX()) {
            return OPTIONAL_FALSE; // tree and bbox-query are disjoint
        }

        if (extent.minX() <= treeExtent.minX() && extent.minY() <= treeExtent.minY()
                && extent.maxX() >= treeExtent.maxX() && extent.maxY() >= treeExtent.maxY()) {
            return OPTIONAL_TRUE; // bbox-query fully contains tree's extent.
        }
        return OPTIONAL_EMPTY;
    }

    boolean containsBottomLeft(Extent extent) throws IOException {
        resetInputPosition();

        Optional<Boolean> extentCheck = checkExtent(new Extent(input), extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        return containsBottomLeft(readRoot(input.position()), extent);
    }

    public boolean crosses(Extent extent) throws IOException {
        resetInputPosition();

        Optional<Boolean> extentCheck = checkExtent(new Extent(input), extent);
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
     * Returns true if the box crosses any edge in this edge subtree
     * */
    private boolean crosses(Edge root, Extent extent) throws IOException {
        // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
        if (root.maxY >= extent.minY()) {

            double a1x = root.x1;
            double a1y = root.y1;
            double b1x = root.x2;
            double b1y = root.y2;
            boolean outside = (a1y < extent.minY() && b1y < extent.minY()) ||
                (a1y > extent.maxY() && b1y > extent.maxY()) ||
                (a1x < extent.minX() && b1x < extent.minX()) ||
                (a1x > extent.maxX() && b1x > extent.maxX());

            // does rectangle's edges intersect or reside inside polygon's edge
            if (outside == false && (lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                extent.minX(), extent.minY(), extent.maxX(), extent.minY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX(), extent.minY(), extent.maxX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.maxX(), extent.maxY(), extent.minX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(root.x1, root.y1, root.x2, root.y2,
                    extent.minX(), extent.maxY(), extent.minX(), extent.minY()))) {
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
