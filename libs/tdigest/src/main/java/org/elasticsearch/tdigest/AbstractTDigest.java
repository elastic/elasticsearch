/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

public abstract class AbstractTDigest extends TDigest {
    /**
     * Same as {@link #weightedAverageSorted(double, double, double, double)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
     */
    static double weightedAverage(double x1, double w1, double x2, double w2) {
        if (x1 <= x2) {
            return weightedAverageSorted(x1, w1, x2, w2);
        } else {
            return weightedAverageSorted(x2, w2, x1, w1);
        }
    }

    /**
     * Compute the weighted average between <code>x1</code> with a weight of
     * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
     * This expects <code>x1</code> to be less than or equal to <code>x2</code>
     * and is guaranteed to return a number in <code>[x1, x2]</code>. An
     * explicit check is required since this isn't guaranteed with floating-point
     * numbers.
     */
    private static double weightedAverageSorted(double x1, double w1, double x2, double w2) {
        assert x1 <= x2;
        final double x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return Math.max(x1, Math.min(x, x2));
    }

    /**
     * Interpolate from a given value given a low and a high reference values
     * @param x value to interpolate from
     * @param x0 low reference value
     * @param x1 high reference value
     * @return interpolated value
     */
    static double interpolate(double x, double x0, double x1) {
        return (x - x0) / (x1 - x0);
    }

    @Override
    public void add(TDigest other) {
        for (Centroid centroid : other.centroids()) {
            add(centroid.mean(), centroid.count());
        }
    }
}
