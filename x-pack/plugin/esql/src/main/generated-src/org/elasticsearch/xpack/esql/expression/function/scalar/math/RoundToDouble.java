/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

// begin generated imports
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;

import java.util.Arrays;
// end generated imports

/**
 * Implementations of {@link RoundTo} for specific types.
 * <p>
 *   We have specializations for when there are very few rounding points because
 *   those are very fast and quite common.
 * </p>
 * This class is generated. Edit {@code X-RoundTo.java.st} instead.
 */
class RoundToDouble {
    static final RoundTo.Build BUILD = (source, field, points) -> {
        double[] f = points.stream().mapToDouble(p -> ((Number) p).doubleValue()).toArray();
        Arrays.sort(f);
        return switch (f.length) {
            // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
            case 1 -> new RoundToDouble1Evaluator.Factory(source, field, f[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundToDouble2Evaluator.Factory(source, field, f[0], f[1]);
            case 3 -> new RoundToDouble3Evaluator.Factory(source, field, f[0], f[1], f[2]);
            case 4 -> new RoundToDouble4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            case 5, 6, 7, 8, 9, 10 -> new RoundToDoubleLinearSearchEvaluator.Factory(source, field, f);
            default -> new RoundToDoubleBinarySearchEvaluator.Factory(source, field, f);
        };
    };

    /**
     * Search the points array for the match linearly. This is faster for smaller arrays even
     * when finding a position late in the array. Presumably because this is super-SIMD-able.
     */
    @Evaluator(extraName = "LinearSearch")
    static double processLinear(double field, @Fixed(includeInToString = false) double[] points) {
        // points is always longer than 3 or we use one of the specialized methods below
        for (int i = 1; i < points.length; i++) {
            if (field < points[i]) {
                return points[i - 1];
            }
        }
        return points[points.length - 1];
    }

    @Evaluator(extraName = "BinarySearch")
    static double process(double field, @Fixed(includeInToString = false) double[] points) {
        int idx = Arrays.binarySearch(points, field);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    @Evaluator(extraName = "1")
    static double process(double field, @Fixed double p0) {
        return p0;
    }

    @Evaluator(extraName = "2")
    static double process(double field, @Fixed double p0, @Fixed double p1) {
        if (field < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "3")
    static double process(double field, @Fixed double p0, @Fixed double p1, @Fixed double p2) {
        if (field < p1) {
            return p0;
        }
        if (field < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "4")
    static double process(double field, @Fixed double p0, @Fixed double p1, @Fixed double p2, @Fixed double p3) {
        if (field < p1) {
            return p0;
        }
        if (field < p2) {
            return p1;
        }
        if (field < p3) {
            return p2;
        }
        return p3;
    }
}
