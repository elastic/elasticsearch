/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Default histogram bucket boundaries matching the APM Java agent's histogram scale: a base-sqrt(2)
 * ladder from 2^-8 up to 2^17 (131072).
 */
final class HistogramBuckets {

    private HistogramBuckets() {}

    // Half-integer exponents of the base-sqrt(2) ladder: 2^(k/2) for k in [-16, 34] gives 2^-8 .. 2^17.
    private static final int MIN_EXPONENT_HALVES = -16;
    private static final int MAX_EXPONENT_HALVES = 34;

    static final List<Double> APM_DEFAULT;
    static final List<Long> APM_DEFAULT_LONGS;

    static {
        List<Double> doubles = new ArrayList<>(MAX_EXPONENT_HALVES - MIN_EXPONENT_HALVES + 1);
        for (int k = MIN_EXPONENT_HALVES; k <= MAX_EXPONENT_HALVES; k++) {
            doubles.add(Math.pow(2.0, k / 2.0));
        }
        APM_DEFAULT = List.copyOf(doubles);

        List<Long> longs = new ArrayList<>(doubles.size());
        for (double boundary : doubles) {
            long rounded = Math.round(boundary);
            if (rounded > 0 && (longs.isEmpty() || longs.get(longs.size() - 1) != rounded)) {
                longs.add(rounded);
            }
        }
        APM_DEFAULT_LONGS = List.copyOf(longs);
    }
}
