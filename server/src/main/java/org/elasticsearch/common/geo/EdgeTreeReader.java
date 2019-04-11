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

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.geo.GeoUtils.lineRelateLine;

public class EdgeTreeReader {
    final BytesRef bytesRef;

    public EdgeTreeReader(BytesRef bytesRef) {
        this.bytesRef = bytesRef;
    }

    public boolean containedInOrCrosses(int minX, int minY, int maxX, int maxY) throws IOException {
        return this.containsBottomLeft(minX, minY, maxX, maxY) || this.crosses(minX, minY, maxX, maxY);
    }

    boolean containsBottomLeft(int minX, int minY, int maxX, int maxY) throws IOException {
        ByteBufferStreamInput input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        int thisMinX = input.readInt();
        int thisMinY = input.readInt();
        int thisMaxX = input.readInt();
        int thisMaxY = input.readInt();

        if (thisMinY > maxY || thisMaxX < minX || thisMaxY < minY || thisMinX > maxX) {
            return false; // tree and bbox-query are disjoint
        }

        if (minX <= thisMinX && minY <= thisMinY && maxX >= thisMaxX && maxY >= thisMaxY) {
            return true; // bbox-query fully contains tree's extent.
        }

        return containsBottomLeft(input, readRoot(input, input.position()), minX, minY, maxX, maxY);
    }

    public boolean crosses(int minX, int minY, int maxX, int maxY) throws IOException {
        ByteBufferStreamInput input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        int thisMinX = input.readInt();
        int thisMinY = input.readInt();
        int thisMaxX = input.readInt();
        int thisMaxY = input.readInt();

        if (thisMinY > maxY || thisMaxX < minX || thisMaxY < minY || thisMinX > maxX) {
            return false; // tree and bbox-query are disjoint
        }

        if (minX <= thisMinX && minY <= thisMinY && maxX >= thisMaxX && maxY >= thisMaxY) {
            return true; // bbox-query fully contains tree's extent.
        }

        return crosses(input, readRoot(input, input.position()), minX, minY, maxX, maxY);
    }

    public Edge readRoot(ByteBufferStreamInput input, int position) throws IOException {
        return readEdge(input, position);
    }

    private static Edge readEdge(ByteBufferStreamInput input, int position) throws IOException {
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


    Edge readLeft(ByteBufferStreamInput input, Edge root) throws IOException {
        return readEdge(input, root.streamOffset);
    }

    Edge readRight(ByteBufferStreamInput input, Edge root) throws IOException {
        return readEdge(input, root.streamOffset + root.rightOffset);
    }

    private boolean containsBottomLeft(ByteBufferStreamInput input, Edge root, int minX, int minY, int maxX, int maxY) throws IOException {
        boolean res = false;
        if (root.maxY >= minY) {
            // is bbox-query contained within linearRing
            // cast infinite ray to the right from bottom-left of bbox-query to see if it intersects edge
            if (lineRelateLine(root.x1, root.y1, root.x2, root.y2,minX, minY,
                    Integer.MAX_VALUE, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
                res = true;
            }

            if (root.rightOffset > 0) { /* has left node */
                res ^= containsBottomLeft(input, readLeft(input, root), minX, minY, maxX, maxY);
            }

            if (root.rightOffset > 0 && maxY >= root.minY) { /* no right node if rightOffset == -1 */
                res ^= containsBottomLeft(input, readRight(input, root), minX, minY, maxX, maxY);
            }
        }
        return res;
    }

    /** Returns true if the box crosses any edge in this edge subtree */
    private boolean crosses(ByteBufferStreamInput input, Edge root, int minX, int minY, int maxX, int maxY) throws IOException {
        boolean res = false;
        // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
        if (root.maxY >= minY) {

            // does rectangle's edges intersect or reside inside polygon's edge
            if (lineRelateLine(root.x1, root.y1, root.x2, root.y2, minX, minY, maxX, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                lineRelateLine(root.x1, root.y1, root.x2, root.y2, maxX, minY, maxX, maxY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                lineRelateLine(root.x1, root.y1, root.x2, root.y2, maxX, maxY, minX, maxY) != PointValues.Relation.CELL_OUTSIDE_QUERY ||
                lineRelateLine(root.x1, root.y1, root.x2, root.y2, minX, maxY, minX, minY) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
                return true;
            }

            if (root.rightOffset > 0) { /* has left node */
                if (crosses(input, readLeft(input, root), minX, minY, maxX, maxY)) {
                    return true;
                }
            }

            if (root.rightOffset > 0 && maxY >= root.minY) { /* no right node if rightOffset == -1 */
                if (crosses(input, readRight(input, root), minX, minY, maxX, maxY)) {
                    return true;
                }
            }
        }
        return false;
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
