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

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;

/**
 * points KD-Tree (2D) writer for use in doc-values.
 *
 * This work is influenced by https://github.com/mourner/kdbush (ISC licensed).
 */
public class Point2DWriter extends ShapeTreeWriter {

    private static final int K = 2;
    private final Extent extent;
    private final int[] coords;
    // size of a leaf node where searches are done sequentially.
    static final int LEAF_SIZE = 64;

    Point2DWriter(int[] x, int[] y) {
        assert x.length == y.length;
        int top = Integer.MIN_VALUE;
        int bottom = Integer.MAX_VALUE;
        int negLeft = Integer.MAX_VALUE;
        int negRight = Integer.MIN_VALUE;
        int posLeft = Integer.MAX_VALUE;
        int posRight = Integer.MIN_VALUE;
        coords = new int[x.length * K];
        for (int i = 0; i < x.length; i++) {
            int xi = x[i];
            int yi = y[i];
            top = Math.max(top, yi);
            bottom = Math.min(bottom, yi);
            if (xi >= 0 && xi < posLeft) {
                posLeft = xi;
            }
            if (xi >= 0 && xi > posRight) {
                posRight = xi;
            }
            if (xi < 0 && xi < negLeft) {
                negLeft = xi;
            }
            if (xi < 0 && xi > negRight) {
                negRight = xi;
            }
            coords[2 * i] = xi;
            coords[2 * i + 1] = yi;
        }
        sort(0, x.length - 1, 0);
        this.extent = new Extent(top, bottom, negLeft, negRight, posLeft, posRight);
    }

    Point2DWriter(int x, int y) {
        coords = new int[] {x, y};
        this.extent = Extent.fromPoint(x, y);
    }

    @Override
    public Extent getExtent() {
        return extent;
    }

    @Override
    public ShapeType getShapeType() {
        return ShapeType.MULTIPOINT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        int numPoints = coords.length >> 1;
        out.writeVInt(numPoints);
        if (numPoints > 1) {
            extent.writeTo(out);
        }
        for (int coord : coords) {
            out.writeInt(coord);
        }
    }

    private void sort(int left, int right, int depth) {
        // since the reader will search through points within a leaf,
        // there is no improved performance by sorting these points.
        if (right - left <= LEAF_SIZE) {
            return;
        }

        int middle = (left + right) >> 1;

        select(left, right, middle, depth);

        sort(left, middle - 1, depth + 1);
        sort(middle + 1, right, depth + 1);
    }

    /**
     * A slightly-modified Floyd-Rivest selection algorithm,
     * https://en.wikipedia.org/wiki/Floyd%E2%80%93Rivest_algorithm
     *
     * @param left  the index of the left point
     * @param right the index of the right point
     * @param k     the pivot index
     * @param depth the depth in the kd-tree
     */
    private void select(int left, int right, int k, int depth) {
        int axis = depth % K;
        while (right > left) {
            if (right - left > 600) {
                double n = right - left + 1;
                int i = k - left + 1;
                double z = Math.log(n);
                double s = 0.5 * Math.exp(2 * z / 3);
                double sd = 0.5 * Math.sqrt(z * s * (n - s) / n) * ((i - n / 2) < 0 ? -1 : 1);
                int newLeft = Math.max(left, (int) Math.floor(k - i * s / n + sd));
                int newRight = Math.min(right, (int) Math.floor(k + (n - i) * s / n + sd));
                select(newLeft, newRight, k, depth);
            }
            int t = coords[2 * k + axis];
            int i = left;
            int j = right;

            swapPoint(left, k);
            if (coords[2 * right + axis] > t) {
                swapPoint(left, right);
            }

            while (i < j) {
                swapPoint(i, j);
                i++;
                j--;
                while (coords[2 * i + axis] < t) {
                    i++;
                }
                while (coords[2 * j + axis] > t) {
                    j--;
                }
            }

            if (coords[2 * left + axis] == t) {
                swapPoint(left, j);
            } else {
                j++;
                swapPoint(j, right);
            }

            if (j <= k) {
                left = j + 1;
            }
            if (k <= j) {
                right = j - 1;
            }
        }
    }

    private void swapPoint(int i, int j) {
        swap( 2 * i, 2 * j);
        swap(2 * i + 1, 2 * j + 1);
    }

    private void swap(int i, int j) {
        int tmp = coords[i];
        coords[i] = coords[j];
        coords[j] = tmp;
    }
}
