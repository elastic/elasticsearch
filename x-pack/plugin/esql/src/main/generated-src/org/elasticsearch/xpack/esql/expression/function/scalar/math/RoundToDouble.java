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
        return switch (f.length) {
            // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
            case 1 -> new RoundToDouble1Evaluator.Factory(source, field, f[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundToDouble2Evaluator.Factory(source, field, f[0], f[1]);
            case 3 -> new RoundToDouble3Evaluator.Factory(source, field, f[0], f[1], f[2]);
            case 4 -> new RoundToDouble4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
            case 5 -> new RoundToDouble5Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4]);
            case 6 -> new RoundToDouble6Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5]);
            case 7 -> new RoundToDouble7Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6]);
            case 8 -> new RoundToDouble8Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
            case 9 -> new RoundToDouble9Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
            case 10 -> new RoundToDouble10Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            default -> new RoundToDoubleBinarySearchEvaluator.Factory(source, field, f);
        };
    };

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

    /*
     * Manual binary search for 5 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "5")
    static double process(double field, @Fixed double p0, @Fixed double p1, @Fixed double p2, @Fixed double p3, @Fixed double p4) {
        if (field < p2) {
            if (field < p1) {
                return p0;
            }
            return p1;
        }
        if (field < p3) {
            return p2;
        }
        if (field < p4) {
            return p3;
        }
        return p4;
    }

    /*
     * Manual binary search for 6 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "6")
    static double process(
        double field,      // hack to keep the formatter happy.
        @Fixed double p0,  // int is so short this should be on one line but double is not.
        @Fixed double p1,  // That's not compatible with the templates.
        @Fixed double p2,  // So we comment to make the formatter not try to change the line.
        @Fixed double p3,
        @Fixed double p4,
        @Fixed double p5
    ) {
        if (field < p2) {
            if (field < p1) {
                return p0;
            }
            return p1;
        }
        if (field < p4) {
            if (field < p3) {
                return p2;
            }
            return p3;
        }
        if (field < p5) {
            return p4;
        }
        return p5;
    }

    /*
     * Manual binary search for 7 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "7")
    static double process(
        double field,      // hack to keep the formatter happy.
        @Fixed double p0,  // int is so short this should be on one line but double is not.
        @Fixed double p1,  // That's not compatible with the templates.
        @Fixed double p2,  // So we comment to make the formatter not try to change the line.
        @Fixed double p3,
        @Fixed double p4,
        @Fixed double p5,
        @Fixed double p6
    ) {
        if (field < p3) {
            if (field < p1) {
                return p0;
            }
            if (field < p2) {
                return p1;
            }
            return p2;
        }
        if (field < p5) {
            if (field < p4) {
                return p3;
            }
            return p4;
        }
        if (field < p6) {
            return p5;
        }
        return p6;
    }

    /*
     * Manual binary search for 8 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "8")
    static double process(
        double field,
        @Fixed double p0,
        @Fixed double p1,
        @Fixed double p2,
        @Fixed double p3,
        @Fixed double p4,
        @Fixed double p5,
        @Fixed double p6,
        @Fixed double p7
    ) {
        if (field < p3) {
            if (field < p1) {
                return p0;
            }
            if (field < p2) {
                return p1;
            }
            return p2;
        }
        if (field < p5) {
            if (field < p4) {
                return p3;
            }
            return p4;
        }
        if (field < p6) {
            return p5;
        }
        if (field < p7) {
            return p6;
        }
        return p7;
    }

    /*
     * Manual binary search for 9 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "9")
    static double process(
        double field,
        @Fixed double p0,
        @Fixed double p1,
        @Fixed double p2,
        @Fixed double p3,
        @Fixed double p4,
        @Fixed double p5,
        @Fixed double p6,
        @Fixed double p7,
        @Fixed double p8
    ) {
        if (field < p4) {
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
        if (field < p6) {
            if (field < p5) {
                return p4;
            }
            return p5;
        }
        if (field < p7) {
            return p6;
        }
        if (field < p8) {
            return p7;
        }
        return p8;
    }

    /*
     * Manual binary search for 10 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "10")
    static double process(
        double field,
        @Fixed double p0,
        @Fixed double p1,
        @Fixed double p2,
        @Fixed double p3,
        @Fixed double p4,
        @Fixed double p5,
        @Fixed double p6,
        @Fixed double p7,
        @Fixed double p8,
        @Fixed double p9
    ) {
        if (field < p4) {
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
        if (field < p7) {
            if (field < p5) {
                return p4;
            }
            if (field < p6) {
                return p5;
            }
            return p6;
        }
        if (field < p8) {
            return p7;
        }
        if (field < p9) {
            return p8;
        }
        return p9;
    }
}
