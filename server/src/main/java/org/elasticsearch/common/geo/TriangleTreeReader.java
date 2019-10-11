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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A tree reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking bounding box
 * relations against the serialized triangle tree.
 */
public class TriangleTreeReader {

    private final int extentOffset = 8;
    private final ByteBufferStreamInput input;
    private final CoordinateEncoder coordinateEncoder;

    public TriangleTreeReader(BytesRef bytesRef, CoordinateEncoder coordinateEncoder) {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        this.coordinateEncoder = coordinateEncoder;
    }

    /**
     * returns the bounding box of the geometry in the format [minX, maxX, minY, maxY].
     */
    public int[] getExtent() throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        return new int[] {thisMinX, thisMaxX, thisMinY, thisMaxY};
    }

    /**
     * returns the X coordinate of the centroid.
     */
    public double getCentroidX() throws IOException {
        input.position(0);
        return coordinateEncoder.decodeX(input.readInt());
    }

    /**
     * returns the Y coordinate of the centroid.
     */
    public double getCentroidY() throws IOException {
        input.position(4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    /**
     * Compute the intersection with the provided bounding box
     */
    public boolean intersects(int minX, int maxX, int minY, int maxY) throws IOException {
        Rectangle component = new Rectangle(minX, maxX, minY, maxY);
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());

        if (minX <= thisMinX && maxX >= thisMaxX && minY <= thisMinY && maxY >= thisMaxY) {
            return true;
        }
        if ((thisMinX > maxX || thisMaxX < minX || thisMinY > maxY || thisMaxY < minY) == false) {
            byte metadata = input.readByte();
            if ((metadata & 1 << 2) == 1 << 2 && (metadata & 1 << 3) != 1 << 3) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return true;
                }
            } else if ((metadata & 1 << 2) != 1 << 2 && (metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsTriangle(aX, aY, bX, bY, aX, aY)) {
                    return true;
                }
            } else {
                assert (metadata & 1 << 2) == 1 << 2 && (metadata & 1 << 3) == 1 << 3;
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
                    return true;
                }
            }
            if ((metadata & 1 << 0) == 1 << 0) {
                if (relateNode(component, minX, maxX, minY, maxY, true, thisMaxX, thisMaxY)) {
                    return true;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) {
                if (relateNode(component, minX, maxX, minY, maxY, true, thisMaxX, thisMaxY)) {
                    return true;
                }
            }
        }
       return false;
    }

    private  boolean relateNode(Rectangle component, int minX, int maxX, int minY, int maxY, boolean splitX, int parentMax, int parentMaxY) throws IOException {
        int thisMaxX = Math.toIntExact(parentMax - input.readVLong());
        int thisMaxY = Math.toIntExact(parentMaxY - input.readVLong());
        int size =  input.readVInt();
        if (minY <= thisMaxY && minX <= thisMaxX) {
            byte metadata = input.readByte();
            int thisMinX;
            int thisMinY;
            if ((metadata & 1 << 2) == 1 << 2 && (metadata & 1 << 3) != 1 << 3) {
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (component.contains(x, y)) {
                    return true;
                }
                thisMinX = x;
                thisMinY = y;
            } else if ((metadata & 1 << 2) != 1 << 2 && (metadata & 1 << 3) == 1 << 3) {
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsLine(aX, aY, bX, bY)) {
                    return true;
                }
                thisMinX = aX;
                thisMinY = Math.min(aY, bY);
            } else {
                assert (metadata & 1 << 2) == 1 << 2 && (metadata & 1 << 3) == 1 << 3;
                int aX =  Math.toIntExact(thisMaxX - input.readVLong());
                int aY =  Math.toIntExact(thisMaxY - input.readVLong());
                int bX =  Math.toIntExact(thisMaxX - input.readVLong());
                int bY =  Math.toIntExact(thisMaxY - input.readVLong());
                int cX =  Math.toIntExact(thisMaxX - input.readVLong());
                int cY =  Math.toIntExact(thisMaxY - input.readVLong());
                if (component.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
                    return true;
                }
                thisMinX = aX;
                thisMinY = Math.min(Math.min(aY, bY), cY);
            }
            if ((metadata & 1 << 0) == 1 << 0) {
                if (relateNode(component, minX, maxX, minY, maxY, !splitX, thisMaxX, thisMaxY)) {
                    return true;
                }
            }

            if ((metadata & 1 << 1) == 1 << 1) {
                int rightSize = input.readVInt();
                if ((splitX == false && maxY >= thisMinY) || (splitX && maxX >= thisMinX)) {
                    if (relateNode(component, minX, maxX, minY, maxY, !splitX, thisMaxX, thisMaxY)) {
                        return true;
                    }
                } else {
                    input.skip(rightSize);
                }
            }
        } else {
            input.skip(size);
        }

        return false;
    }
}
