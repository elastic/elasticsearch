/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
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
        int low_index = (int) Math.floor(index);
        int high_index = low_index + 1;
        double weight = index - low_index;

        if (index <= 0) {
            low_index = 0;
            high_index = 0;
            weight = 0;
        }
        if (index >= n - 1) {
            low_index = n - 1;
            high_index = n - 1;
            weight = 0;
        }
        return data[low_index] + weight * (data[high_index] - data[low_index]);
    }

    public static double quantile(final double q, List<Double> data) {
        return quantile(q, data.stream().mapToDouble(i -> i).toArray());
    }
}
