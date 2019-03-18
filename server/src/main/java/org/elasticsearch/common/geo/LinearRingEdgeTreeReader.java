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

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.geo.GeoUtils.lineRelateLine;

public class LinearRingEdgeTreeReader {
    final BytesRef bytesRef;

    public LinearRingEdgeTreeReader(BytesRef bytesRef) {
        this.bytesRef = bytesRef;
    }

    /**
     * even partially
     */
    public boolean containedIn(int minX, int minY, int maxX, int maxY) throws IOException {
        StreamInput input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        int[] extent = readExtent(input);
        int thisMinX = extent[0];
        int thisMinY = extent[1];
        int thisMaxX = extent[2];
        int thisMaxY = extent[3];

        if (thisMinY > maxY || thisMaxX < minX || thisMaxY < minY || thisMinX > maxX) {
            return false; // tree and bbox-query are disjoint
        }

        if (minX <= thisMinX && minY <= thisMinY && maxX >= thisMaxX && maxY >= thisMaxY) {
            return true; // bbox-query fully contains tree's extent.
        }

        Edge root = readRoot(input);
        return root.insideOrCrosses(minX, minY, maxX, maxY);
    }

    public int[] readExtent(StreamInput input) throws IOException {
        int minX = input.readInt();
        int minY = input.readInt();
        int maxX = input.readInt();
        int maxY = input.readInt();
        return new int[] { minX, minY, maxX, maxY };
    }

    public Edge readRoot(StreamInput input) throws IOException {
        return Edge.readEdge(input);
    }

    private static class Edge {
        StreamInput input;
        int x1;
        int y1;
        int x2;
        int y2;
        int minY;
        int maxY;
        int rightOffset;

        Edge(StreamInput input, int x1, int y1, int x2, int y2, int minY, int maxY, int rightOffset) {
            this.input = input;
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.minY = minY;
            this.maxY = maxY;
            this.rightOffset = rightOffset;
        }

        Edge readLeft() throws IOException {
            return readEdge(input);
        }

        Edge readRight() throws IOException {
            input.skip(rightOffset);
            return readEdge(input);
        }

        /** Returns true if the box crosses any edge in this edge subtree */
        private boolean insideOrCrosses(int minX, int minY, int maxX, int maxY) throws IOException {
            // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
            if (this.maxY >= minY) {
                // is bbox-query contained within linearRing
                // cast infinite ray to the right from bottom-left and top-right of bbox-query to see if it intersects edge
                if (lineRelateLine(x1, y1, x2, y2, minX, minY, Integer.MAX_VALUE, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                    lineRelateLine(x1, y1, x2, y2, maxX, maxY, Integer.MAX_VALUE, maxY) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
                    return true;
                }

                // does rectangle's edges intersect or reside inside polygon's edge
                if (lineRelateLine(x1, y1, x2, y2, minX, minY, maxX, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                    lineRelateLine(x1, y1, x2, y2, maxX, minY, maxX, maxY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                    lineRelateLine(x1, y1, x2, y2, maxX, maxY, minX, maxY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                    lineRelateLine(x1, y1, x2, y2, minX, maxY, minX, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
                    return true;
                }

                if (rightOffset > 1) { /* has left node */
                    if (readLeft().insideOrCrosses(minX, minY, maxX, maxY)) {
                        return true;
                    }
                }

                if (rightOffset > 0 && maxY >= this.minY) { /* no right node if rightOffset == -1 */
                    if (readRight().insideOrCrosses(minX, minY, maxX, maxY)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static Edge readEdge(StreamInput input) throws IOException {
            int minY = input.readInt();
            int maxY = input.readInt();
            int x1 = input.readInt();
            int y1 = input.readInt();
            int x2 = input.readInt();
            int y2 = input.readInt();
            int rightOffset = input.readInt();
            return new Edge(input, x1, y1, x2, y2, minY, maxY, rightOffset);
        }
    }
}
