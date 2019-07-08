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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.Point;
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

    Point2DWriter(MultiPoint multiPoint) {
        int numPoints = multiPoint.size();
        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        coords = new int[numPoints * K];
        int i = 0;
        for (Point point : multiPoint) {
            int x = GeoEncodingUtils.encodeLongitude(point.getLon());
            int y = GeoEncodingUtils.encodeLatitude(point.getLat());
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
            coords[2 * i] = x;
            coords[2 * i + 1] = y;
            i++;
        }
        sort(0, numPoints - 1, 0);
        this.extent = new Extent(minX, minY, maxX, maxY);
    }

    Point2DWriter(Point point) {
        int x = GeoEncodingUtils.encodeLongitude(point.getLon());
        int y = GeoEncodingUtils.encodeLatitude(point.getLat());
        coords = new int[] {x, y};
        this.extent = new Extent(x, y, x, y);
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
