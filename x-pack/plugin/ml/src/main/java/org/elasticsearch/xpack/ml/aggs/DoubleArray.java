/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

public final class DoubleArray {

    private DoubleArray() { }

    public static double[] cumulativeSum(double[] xs) {
        double[] sum = new double[xs.length];
        sum[0] = xs[0];
        for (int i = 1; i < xs.length; i++) {
            sum[i] = sum[i - 1] + xs[i];
        }
        return sum;
    }

    public static void divMut(double[] xs, double v) {
        for (int i = 0; i < xs.length; i++) {
            xs[i] /= v;
        }
    }
}
