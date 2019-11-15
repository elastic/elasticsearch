/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import java.util.List;
import java.util.stream.Collectors;

public final class Statistics {

    private Statistics(){}

    /**
     * Calculates the softMax of the passed values.
     *
     * Any {@link Double#isInfinite()}, {@link Double#NaN}, or `null` values are ignored in calculation and returned as 0.0 in the
     * softMax.
     * @param values Values on which to run SoftMax.
     * @return A new list containing the softmax of the passed values
     */
    public static List<Double> softMax(List<Double> values) {
        Double expSum = 0.0;
        Double max = values.stream().filter(v -> isInvalid(v) == false).max(Double::compareTo).orElse(null);
        if (max == null) {
            throw new IllegalArgumentException("no valid values present");
        }
        List<Double> exps = values.stream().map(v -> isInvalid(v) ? Double.NEGATIVE_INFINITY : v - max)
            .collect(Collectors.toList());
        for (int i = 0; i < exps.size(); i++) {
            if (isInvalid(exps.get(i)) == false) {
                Double exp = Math.exp(exps.get(i));
                expSum += exp;
                exps.set(i, exp);
            }
        }
        for (int i = 0; i < exps.size(); i++) {
            if (isInvalid(exps.get(i))) {
                exps.set(i, 0.0);
            } else {
                exps.set(i, exps.get(i)/expSum);
            }
        }
        return exps;
    }

    public static double sigmoid(double value) {
        return 1/(1 + Math.exp(-value));
    }

    public static boolean isInvalid(Double v) {
        return v == null || Double.isInfinite(v) || Double.isNaN(v);
    }

}
