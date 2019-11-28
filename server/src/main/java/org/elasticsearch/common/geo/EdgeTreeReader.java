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
    private static final Optional<Boolean> OPTIONAL_FALSE = Optional.of(false);
    private static final Optional<Boolean> OPTIONAL_TRUE = Optional.of(true);
    private static final Optional<Boolean> OPTIONAL_EMPTY = Optional.empty();

    private final ByteBufferStreamInput input;
    private final int startPosition;
    private final boolean hasArea;
    private Extent treeExtent;

    public EdgeTreeReader(ByteBufferStreamInput input, boolean hasArea) throws IOException {
        this.startPosition = input.position();
        this.input = input;
        this.hasArea = hasArea;
        this.treeExtent = null;
    }

    public Extent getExtent() throws IOException {
        if (treeExtent == null) {
            resetInputPosition();
            treeExtent = new Extent(input);
        }
        return treeExtent;
    }

    @Override
    public double getCentroidX() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getCentroidY() {
        throw new UnsupportedOperationException();
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
        Optional<Boolean> extentCheck = checkExtent(getExtent(), extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        resetToRootEdge();
        if (input.readBoolean()) { /* has edges */
            return containsBottomLeft(input.position(), extent);
        }
        return false;
    }

    public boolean crosses(Extent extent) throws IOException {
        resetInputPosition();
        Optional<Boolean> extentCheck = checkExtent(getExtent(), extent);
        if (extentCheck.isPresent()) {
            return extentCheck.get();
        }

        resetToRootEdge();
        if (input.readBoolean()) { /* has edges */
            return crosses(input.position(), extent);
        }
        return false;
    }

    /**
     * Returns true if the bottom-left point of the rectangle query is contained within the
     * tree's edges.
     */
    private boolean containsBottomLeft(int edgePosition, Extent extent) throws IOException {
        // start read edge from bytes
        input.position(edgePosition);
        int maxY = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int minY = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int x1 = Math.toIntExact(treeExtent.maxX() - input.readVLong());
        int x2 = Math.toIntExact(treeExtent.maxX() - input.readVLong());
        int y1 = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int y2 = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int rightOffset = input.readVInt();
        if (rightOffset == 1) {
            rightOffset = 0;
        } else if (rightOffset == 0) {
            rightOffset = -1;
        }
        int streamOffset = input.position();
        // end read edge from bytes

        boolean res = false;
        if (maxY >= extent.minY()) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from bottom-left of bbox-query to see if it intersects edge
            if (lineCrossesLineWithBoundary(x1, y1, x2, y2, extent.minX(), extent.minY(), Integer.MAX_VALUE,
                    extent.minY())) {
                res = true;
            }

            if (rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(streamOffset, extent);
            }

            if (rightOffset >= 0 && extent.maxY() >= minY) { /* no right node if rightOffset == -1 */
                res ^= containsBottomLeft(streamOffset + rightOffset, extent);
            }
        }
        return res;
    }

    /**
     * Returns true if the box crosses any edge in this edge subtree
     * */
    private boolean crosses(int edgePosition, Extent extent) throws IOException {
        // start read edge from bytes
        input.position(edgePosition);
        int maxY = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int minY = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int x1 = Math.toIntExact(treeExtent.maxX() - input.readVLong());
        int x2 = Math.toIntExact(treeExtent.maxX() - input.readVLong());
        int y1 = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int y2 = Math.toIntExact(treeExtent.maxY() - input.readVLong());
        int rightOffset = input.readVInt();
        if (rightOffset == 1) {
            rightOffset = 0;
        } else if (rightOffset == 0) {
            rightOffset = -1;
        }
        int streamOffset = input.position();
        // end read edge from bytes

        // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
        if (maxY >= extent.minY()) {
            boolean outside = (y1 < extent.minY() && y2 < extent.minY()) ||
                (y1 > extent.maxY() && y2 > extent.maxY()) ||
                (x1 < extent.minX() && x2 < extent.minX()) ||
                (x1 > extent.maxX() && x2 > extent.maxX());

            // does rectangle's edges intersect or reside inside polygon's edge
            if (outside == false && (lineCrossesLineWithBoundary(x1, y1, x2, y2,
                extent.minX(), extent.minY(), extent.maxX(), extent.minY()) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    extent.maxX(), extent.minY(), extent.maxX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    extent.maxX(), extent.maxY(), extent.minX(), extent.maxY()) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    extent.minX(), extent.maxY(), extent.minX(), extent.minY()))) {
                return true;
            }

            // does this edge fully reside within the rectangle's area
            if (extent.minX() <= Math.min(x1, x2) && extent.minY() <= Math.min(y1, y2)
                && extent.maxX() >= Math.max(x1, x2) && extent.maxY() >= Math.max(y1, y2)) {
                return true;
            }

            /* has left node */
            if (rightOffset > 0 && crosses(streamOffset, extent)) {
                return true;
            }

            /* no right node if rightOffset == -1 */
            if (rightOffset >= 0 && extent.maxY() >= minY && crosses(streamOffset + rightOffset, extent)) {
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
}
