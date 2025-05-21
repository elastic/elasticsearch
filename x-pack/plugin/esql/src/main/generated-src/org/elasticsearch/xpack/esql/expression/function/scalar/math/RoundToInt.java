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
 * This class is generated. Edit {@code X-RoundTo.java.st} instead.
 */
class RoundToInt {
    static final RoundTo.Build BUILD = (source, field, points) -> {
        int[] f = points.stream().mapToInt(p -> ((Number) p).intValue()).toArray();
        Arrays.sort(f);
        return switch (f.length) {
            // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
            case 1 -> new RoundToInt1Evaluator.Factory(source, field, f[0]);
            case 2 -> new RoundToInt2Evaluator.Factory(source, field, f[0], f[1]);
            case 3 -> new RoundToInt3Evaluator.Factory(source, field, f[0], f[1], f[2]);
            case 4 -> new RoundToInt4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
            default -> new RoundToIntArrayEvaluator.Factory(source, field, f);
        };
    };

    @Evaluator(extraName = "Array")
    static int process(int field, @Fixed(includeInToString = false) int[] points) {
        // points is always longer than 3 or we use one of the specialized methods below
        for (int i = 1; i < points.length; i++) {
            if (field < points[i]) {
                return points[i - 1];
            }
        }
        return points[points.length - 1];
    }

    @Evaluator(extraName = "1")
    static int process(int field, @Fixed int p0) {
        return p0;
    }

    @Evaluator(extraName = "2")
    static int process(int field, @Fixed int p0, @Fixed int p1) {
        if (field < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "3")
    static int process(int field, @Fixed int p0, @Fixed int p1, @Fixed int p2) {
        if (field < p1) {
            return p0;
        }
        if (field < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "4")
    static int process(int field, @Fixed int p0, @Fixed int p1, @Fixed int p2, @Fixed int p3) {
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
