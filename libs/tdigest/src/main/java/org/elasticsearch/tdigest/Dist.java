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
        if (Double.compare(x, elementGetter.apply(0)) < 0) {
            return 0;
        }

        double n1 = 0.5;
        int n2 = 0;
        for (int i = 1; i < length; i++) {
            double value = elementGetter.apply(i);
            int compareResult = Double.compare(value, x);
            if (compareResult > 0) {
                if (Double.compare(n2, 0) > 0) {
                    return (n1 + 0.5 * n2) / length;
                }
                double previousValue = elementGetter.apply(i - 1);
                double factor = (x - previousValue) / (value - previousValue);
                return (n1 + factor) / length;
            }
            if (compareResult < 0) {
                n1++;
            } else {
                n2++;
            }
        }
        return (length - 0.5 * n2) / length;
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
