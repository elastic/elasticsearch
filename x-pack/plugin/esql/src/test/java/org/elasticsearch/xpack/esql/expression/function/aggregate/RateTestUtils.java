/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import java.util.List;

final class RateTestUtils {
    /**
     * Computes the expected increase for the given values and temporality.
     * Values must be in reverse chronological order (newest first), matching the order fed to the aggregator.
     */
    public static double computeExpectedIncrease(List<Object> nonNullValues, RateTests.TemporalityParameter temporality) {
        if (temporality == RateTests.TemporalityParameter.DELTA) {
            double firstDelta = ((Number) nonNullValues.getLast()).doubleValue();
            return nonNullValues.stream().mapToDouble(v -> ((Number) v).doubleValue()).sum() - firstDelta;
        } else {
            double resets = 0.0;
            double last = ((Number) nonNullValues.get(0)).doubleValue();
            double current = last;
            for (int i = 1; i < nonNullValues.size(); i++) {
                double prev = ((Number) nonNullValues.get(i)).doubleValue();
                if (prev > current) {
                    resets += prev;
                }
                current = prev;
            }
            return resets + (last - current);
        }
    }
}
