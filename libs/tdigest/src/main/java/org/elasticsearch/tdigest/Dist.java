/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import java.util.Collection;
import java.util.List;

/**
 * Reference implementations for cdf and quantile if we have all data.
 */
public class Dist {
    public static double cdf(final double x, double[] data) {
        return cdf(x, data, 0.5);
    }

    public static double cdf(final double x, double[] data, double w) {
        int n1 = 0;
        int n2 = 0;
        for (Double v : data) {
            n1 += (v < x) ? 1 : 0;
            n2 += (v == x) ? 1 : 0;
        }
        return (n1 + w * n2) / data.length;
    }

    public static double cdf(final double x, Collection<Double> data) {
        return cdf(x, data, 0.5);
    }

    public static double cdf(final double x, Collection<Double> data, double w) {
        int n1 = 0;
        int n2 = 0;
        for (Double v : data) {
            n1 += (v < x) ? 1 : 0;
            n2 += (v == x) ? 1 : 0;
        }
        return (n1 + w * n2) / data.size();
    }

    public static double quantile(final double q, double[] data) {
        int n = data.length;
        if (n == 0) {
            return Double.NaN;
        }
        double index = q * (n - 1);
        if (index < 0) {
            index = 0;
        }
        if (index > n - 1) {
            index = n - 1;
        }
        int low_index = (int) Math.floor(index);
        int high_index = low_index + 1;

        double high_weight = index - low_index;
        double low_weight = 1 - high_weight;
        return low_weight * data[low_index] + high_weight * data[high_index];
    }

    public static double quantile(final double q, List<Double> data) {
        return quantile(q, data.stream().mapToDouble(i -> i).toArray());
    }
}
