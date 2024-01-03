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

import java.util.List;
import java.util.function.Function;

/**
 * Reference implementations for cdf and quantile if we have all data sorted.
 */
public class Dist {

    private static double cdf(final double x, final int length, Function<Integer, Double> elementGetter) {
        if (length == 0) {
            // no data to examine
            return Double.NaN;
        }
        if (length == 1) {
            double value = elementGetter.apply(0);
            if (x < value) return 0;
            if (x > value) return 1;
            return 0.5;
        }

        if (Double.compare(x, elementGetter.apply(0)) < 0) {
            return 0;
        }

        if (Double.compare(x, elementGetter.apply(0)) == 0) {
            // we have one or more centroids == x, treat them as one
            // dw will accumulate the weight of all of the centroids at x
            double dw = 0;
            for (int i = 0; i < length && Double.compare(elementGetter.apply(i), x) == 0; i++) {
                dw += 1;
            }
            return dw / 2.0 / length;
        }

        if (x > elementGetter.apply(length - 1)) {
            return 1;
        }
        if (x == elementGetter.apply(length - 1)) {
            double dw = 0;
            for (int i = length - 1; i >= 0 && Double.compare(elementGetter.apply(i), x) == 0; i--) {
                dw += 1;
            }
            return (length - dw / 2.0) / length;
        }

        // initially, we set left width equal to right width
        double left = (elementGetter.apply(1) - elementGetter.apply(0)) / 2;
        double weightSoFar = 0;

        for (int i = 0; i < length - 1; i++) {
            double right = (elementGetter.apply(i + 1) - elementGetter.apply(i)) / 2;
            if (x < elementGetter.apply(i) + right) {
                double value = (weightSoFar + AbstractTDigest.interpolate(x, elementGetter.apply(i) - left, elementGetter.apply(i) + right))
                    / length;
                return Math.max(value, 0.0);
            }
            weightSoFar += 1;
            left = right;
        }

        // for the last element, assume right width is same as left
        int lastOffset = length - 1;
        double right = (elementGetter.apply(lastOffset) - elementGetter.apply(lastOffset - 1)) / 2;
        if (x < elementGetter.apply(lastOffset) + right) {
            return (weightSoFar + AbstractTDigest.interpolate(
                x,
                elementGetter.apply(lastOffset) - right,
                elementGetter.apply(lastOffset) + right
            )) / length;
        }
        return 1;
    }

    public static double cdf(final double x, double[] data) {
        return cdf(x, data.length, (i) -> data[i]);
    }

    public static double cdf(final double x, List<Double> data) {
        return cdf(x, data.size(), data::get);
    }

    private static double quantile(final double q, final int length, Function<Integer, Double> elementGetter) {
        if (length == 0) {
            return Double.NaN;
        }
        double index = q * (length - 1);
        int low_index = (int) Math.floor(index);
        int high_index = low_index + 1;
        double weight = index - low_index;

        if (index <= 0) {
            low_index = 0;
            high_index = 0;
            weight = 0;
        }
        if (index >= length - 1) {
            low_index = length - 1;
            high_index = length - 1;
            weight = 0;
        }
        double low_value = elementGetter.apply(low_index);
        double high_value = elementGetter.apply(high_index);
        return low_value + weight * (high_value - low_value);
    }

    public static double quantile(final double q, double[] data) {
        return quantile(q, data.length, (i) -> data[i]);
    }

    public static double quantile(final double q, List<Double> data) {
        return quantile(q, data.size(), data::get);
    }
}
