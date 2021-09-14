/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.common.Numbers;

public final class Statistics {

    private Statistics(){}

    /**
     * Calculates the softMax of the passed values.
     *
     * Any {@link Double#isInfinite()}, {@link Double#NaN}, or `null` values are ignored in calculation and returned as 0.0 in the
     * softMax.
     * @param values Values on which to run SoftMax.
     * @return A new array containing the softmax of the passed values
     */
    public static double[] softMax(double[] values) {
        double expSum = 0.0;
        double max = Double.NEGATIVE_INFINITY;
        for (double val : values) {
            if (isValid(val)) {
                max = Math.max(max, val);
            }
        }
        if (isValid(max) == false) {
            throw new IllegalArgumentException("no valid values present");
        }
        double[] exps = new double[values.length];
        for (int i = 0; i < exps.length; i++) {
            if (isValid(values[i])) {
                double exp = Math.exp(values[i] - max);
                expSum += exp;
                exps[i] = exp;
            } else {
                exps[i] = Double.NaN;
            }
        }
        for (int i = 0; i < exps.length; i++) {
            if (isValid(exps[i])) {
                exps[i] /= expSum;
            } else {
                exps[i] = 0.0;
            }
        }
        return exps;
    }

    public static double sigmoid(double value) {
        return 1/(1 + Math.exp(-value));
    }

    private static boolean isValid(double v) {
        return Numbers.isValidDouble(v);
    }

}
