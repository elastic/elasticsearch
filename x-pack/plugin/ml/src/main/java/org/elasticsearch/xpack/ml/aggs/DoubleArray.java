/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

import org.elasticsearch.common.Numbers;

public final class DoubleArray {

    private DoubleArray() {}

    /**
     * Returns a NEW {@link double[]} that is the cumulative sum of the passed array
     * @param xs array to cumulatively sum
     * @return new double[]
     */
    public static double[] cumulativeSum(double[] xs) {
        double[] sum = new double[xs.length];
        sum[0] = xs[0];
        for (int i = 1; i < xs.length; i++) {
            sum[i] = sum[i - 1] + xs[i];
        }
        return sum;
    }

    /**
     * Mutably divides each element in `xs` by the value `v`
     * @param xs array to mutate with the divisor
     * @param v The divisor, must not be 0.0, Infinite, or NaN.
     */
    public static void divMut(double[] xs, double v) {
        if (v == 0.0 || Numbers.isValidDouble(v) == false) {
            throw new IllegalArgumentException("unable to divide by [" + v + "] as it results in undefined behavior");
        }
        for (int i = 0; i < xs.length; i++) {
            xs[i] /= v;
        }
    }
}
