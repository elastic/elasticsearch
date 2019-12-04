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

import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;

/**
 * This {@link ShapeTreeReader} understands how to parse polygons
 * serialized with the {@link EdgeTreeWriter}
 */
public class EdgeTreeReader implements ShapeTreeReader {
    private final ByteBufferStreamInput input;
    private final int startPosition;
    private final boolean hasArea;

    public EdgeTreeReader(ByteBufferStreamInput input, boolean hasArea) throws IOException {
        this.startPosition = input.position();
        this.input = input;
        this.hasArea = hasArea;
    }

    public Extent getExtent() throws IOException {
        resetInputPosition();
        return new Extent(input);
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
    public GeoRelation relate(int minX, int minY, int maxX, int maxY) throws IOException {
        resetInputPosition();
        // check extent
        int treeMaxY = input.readInt();
        int treeMinY = input.readInt();
        int negLeft = input.readInt();
        int negRight = input.readInt();
        int posLeft = input.readInt();
        int posRight = input.readInt();
        int treeMinX = Math.min(negLeft, posLeft);
        int treeMaxX = Math.max(negRight, posRight);
        if (treeMinY > maxY || treeMaxX < minX || treeMaxY < minY || treeMinX > maxX) {
            return GeoRelation.QUERY_DISJOINT; // tree and bbox-query are disjoint
        }
        if (minX <= treeMinX && minY <= treeMinY && maxX >= treeMaxX && maxY >= treeMaxY) {
            return GeoRelation.QUERY_CROSSES; // bbox-query fully contains tree's
        }

        if (crosses(treeMaxX, treeMaxY, minX, minY, maxX, maxY)) {
            return GeoRelation.QUERY_CROSSES;
        } else if (hasArea && containsBottomLeft(treeMaxX, treeMaxY, minX, minY, maxY)){
            return GeoRelation.QUERY_INSIDE;
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    private boolean containsBottomLeft(int treeMaxX, int treeMaxY, int minX, int minY, int maxY) throws IOException {
        resetToRootEdge();
        if (input.readBoolean()) { /* has edges */
            return containsBottomLeft(input.position(), treeMaxX, treeMaxY, minX, minY, maxY);
        }
        return false;
    }

    private boolean crosses(int treeMaxX, int treeMaxY, int minX, int minY, int maxX, int maxY) throws IOException {
        resetToRootEdge();
        if (input.readBoolean()) { /* has edges */
            return crosses(input.position(), treeMaxX, treeMaxY, minX, minY, maxX, maxY);
        }
        return false;
    }

    /**
     * Returns true if the bottom-left point of the rectangle query is contained within the
     * tree's edges.
     */
    private boolean containsBottomLeft(int edgePosition, int treeMaxX, int treeMaxY,
                                       int minX, int minY, int maxY) throws IOException {
        // start read edge from bytes
        input.position(edgePosition);
        int thisMaxY = Math.toIntExact(treeMaxY - input.readVLong());
        int thisMinY = Math.toIntExact(treeMaxY - input.readVLong());
        int x1 = Math.toIntExact(treeMaxX - input.readVLong());
        int x2 = Math.toIntExact(treeMaxX - input.readVLong());
        int y1 = Math.toIntExact(treeMaxY - input.readVLong());
        int y2 = Math.toIntExact(treeMaxY - input.readVLong());
        int rightOffset = input.readVInt();
        if (rightOffset == 1) {
            rightOffset = 0;
        } else if (rightOffset == 0) {
            rightOffset = -1;
        }
        int streamOffset = input.position();
        // end read edge from bytes

        boolean res = false;
        if (thisMaxY >= minY) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from bottom-left of bbox-query to see if it intersects edge
            if (lineCrossesLineWithBoundary(x1, y1, x2, y2, minX, minY, Integer.MAX_VALUE, minY)) {
                res = true;
            }

            if (rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(streamOffset, treeMaxX, treeMaxY, minX, minY, maxY);
            }

            if (rightOffset >= 0 && maxY >= thisMinY) { /* no right node if rightOffset == -1 */
                res ^= containsBottomLeft(streamOffset + rightOffset, treeMaxX, treeMaxY, minX, minY, maxY);
            }
        }
        return res;
    }

    /**
     * Returns true if the box crosses any edge in this edge subtree
     * */
    private boolean crosses(int edgePosition, int treeMaxX, int treeMaxY, int minX, int minY, int maxX, int maxY) throws IOException {
        // start read edge from bytes
        input.position(edgePosition);
        int thisMaxY = Math.toIntExact(treeMaxY - input.readVLong());
        int thisMinY = Math.toIntExact(treeMaxY - input.readVLong());
        int x1 = Math.toIntExact(treeMaxX - input.readVLong());
        int x2 = Math.toIntExact(treeMaxX - input.readVLong());
        int y1 = Math.toIntExact(treeMaxY - input.readVLong());
        int y2 = Math.toIntExact(treeMaxY - input.readVLong());
        int rightOffset = input.readVInt();
        if (rightOffset == 1) {
            rightOffset = 0;
        } else if (rightOffset == 0) {
            rightOffset = -1;
        }
        int streamOffset = input.position();
        // end read edge from bytes

        // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
        if (thisMaxY >= minY) {
            boolean outside = (y1 < minY && y2 < minY) ||
                (y1 > maxY && y2 > maxY) ||
                (x1 < minX && x2 < minX) ||
                (x1 > maxX && x2 > maxX);

            // does rectangle's edges intersect or reside inside polygon's edge
            if (outside == false && (lineCrossesLineWithBoundary(x1, y1, x2, y2,
                minX, minY, maxX, minY) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    maxX, minY, maxX, maxY) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    maxX, maxY, minX, maxY) ||
                lineCrossesLineWithBoundary(x1, y1, x2, y2,
                    minX, maxY, minX, minY))) {
                return true;
            }

            // does this edge fully reside within the rectangle's area
            if (minX <= Math.min(x1, x2) && minY <= Math.min(y1, y2)
                && maxX >= Math.max(x1, x2) && maxY >= Math.max(y1, y2)) {
                return true;
            }

            /* has left node */
            if (rightOffset > 0 && crosses(streamOffset, treeMaxX, treeMaxY, minX, minY, maxX, maxY)) {
                return true;
            }

            /* no right node if rightOffset == -1 */
            if (rightOffset >= 0 && maxY >= thisMinY && crosses(streamOffset + rightOffset, treeMaxX, treeMaxY, minX, minY, maxX, maxY)) {
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
