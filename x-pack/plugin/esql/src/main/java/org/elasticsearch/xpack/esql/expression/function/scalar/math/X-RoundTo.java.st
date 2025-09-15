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
class RoundTo$Type$ {
    static final RoundTo.Build BUILD = (source, field, points) -> {
        $type$[] f = points.stream().mapTo$Type$(p -> ((Number) p).$type$Value()).toArray();
        Arrays.sort(f);
        return switch (f.length) {
            // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
            case 1 -> new RoundTo$Type$1Evaluator.Factory(source, field, f[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundTo$Type$2Evaluator.Factory(source, field, f[0], f[1]);
            case 3 -> new RoundTo$Type$3Evaluator.Factory(source, field, f[0], f[1], f[2]);
            case 4 -> new RoundTo$Type$4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            case 5, 6, 7, 8, 9, 10 -> new RoundTo$Type$LinearSearchEvaluator.Factory(source, field, f);
            default -> new RoundTo$Type$BinarySearchEvaluator.Factory(source, field, f);
        };
    };

    /**
     * Search the points array for the match linearly. This is faster for smaller arrays even
     * when finding a position late in the array. Presumably because this is super-SIMD-able.
     */
    @Evaluator(extraName = "LinearSearch")
    static $type$ processLinear($type$ field, @Fixed(includeInToString = false) $type$[] points) {
        // points is always longer than 3 or we use one of the specialized methods below
        for (int i = 1; i < points.length; i++) {
            if (field < points[i]) {
                return points[i - 1];
            }
        }
        return points[points.length - 1];
    }

    @Evaluator(extraName = "BinarySearch")
    static $type$ process($type$ field, @Fixed(includeInToString = false) $type$[] points) {
        int idx = Arrays.binarySearch(points, field);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    @Evaluator(extraName = "1")
    static $type$ process($type$ field, @Fixed $type$ p0) {
        return p0;
    }

    @Evaluator(extraName = "2")
    static $type$ process($type$ field, @Fixed $type$ p0, @Fixed $type$ p1) {
        if (field < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "3")
    static $type$ process($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2) {
        if (field < p1) {
            return p0;
        }
        if (field < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "4")
    static $type$ process($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3) {
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
