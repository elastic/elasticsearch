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
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * This {@link ShapeTreeReader} understands how to parse points
 * serialized with the {@link Point2DWriter}
 */
class Point2DReader implements ShapeTreeReader {
    private final ByteBufferStreamInput input;
    private final int size;
    private final int startPosition;

    Point2DReader(ByteBufferStreamInput input) throws IOException {
        this.input = input;
        this.size = input.readVInt();
        this.startPosition = input.position();
    }

    public Extent getExtent() throws IOException {
        if (size == 2) {
            int x = readX(0);
            int y = readY(0);
            return new Extent(x, y, x, y);
        } else {
            return new Extent(input);
        }
    }

    public boolean intersects(Extent extent) throws IOException {
        Deque<Integer> stack = new ArrayDeque<>();
        stack.push(0);
        stack.push(size - 1);
        stack.push(0);
        while (stack.isEmpty() == false) {
            int axis = stack.pop();
            int right = stack.pop();
            int left = stack.pop();

            if (right - left <= Point2DWriter.LEAF_SIZE) {
                for (int i = left; i <= right; i++) {
                    // TODO serialize to re-usable array instead of serializing in each step
                    int x = readX(i);
                    int y = readY(i);
                    if (x >= extent.minX && x <= extent.maxX && y >= extent.minY && y <= extent.maxY) {
                        return true;
                    }
                }
                continue;
            }

            int middle = (right - left) >> 1;
            int x = readX(middle);
            int y = readY(middle);
            if (x >= extent.minX && x <= extent.maxX && y >= extent.minY && y <= extent.maxY) {
                return true;
            }
            if ((axis == 0 && extent.minX <= x) || (axis == 1 && extent.minY <= y)) {
                stack.push(left);
                stack.push(middle - 1);
                stack.push(1 - axis);
            }
            if ((axis == 0 && extent.maxX >= x) || (axis == 1 && extent.maxY >= y)) {
                stack.push(middle + 1);
                stack.push(right);
                stack.push(1 - axis);
            }
        }

        return false;
    }

    private int readX(int pointIdx) throws IOException {
        input.position(startPosition + 2 * pointIdx * Integer.BYTES);
        return input.readInt();
    }

    private int readY(int pointIdx) throws IOException {
        input.position(startPosition + (2 * pointIdx + 1) * Integer.BYTES);
        return input.readInt();
    }
}
