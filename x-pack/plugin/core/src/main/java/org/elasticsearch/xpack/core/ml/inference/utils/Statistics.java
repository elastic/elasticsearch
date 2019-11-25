/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.common.Numbers;

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
        Double max = values.stream().filter(Statistics::isValid).max(Double::compareTo).orElse(null);
        if (max == null) {
            throw new IllegalArgumentException("no valid values present");
        }
        List<Double> exps = values.stream().map(v -> isValid(v) ? v - max : Double.NEGATIVE_INFINITY)
            .collect(Collectors.toList());
        for (int i = 0; i < exps.size(); i++) {
            if (isValid(exps.get(i))) {
                Double exp = Math.exp(exps.get(i));
                expSum += exp;
                exps.set(i, exp);
            }
        }
        for (int i = 0; i < exps.size(); i++) {
            if (isValid(exps.get(i))) {
                exps.set(i, exps.get(i)/expSum);
            } else {
                exps.set(i, 0.0);
            }
        }
        return exps;
    }

    public static double sigmoid(double value) {
        return 1/(1 + Math.exp(-value));
    }

    private static boolean isValid(Double v) {
        return v != null && Numbers.isValidDouble(v);
    }

}
