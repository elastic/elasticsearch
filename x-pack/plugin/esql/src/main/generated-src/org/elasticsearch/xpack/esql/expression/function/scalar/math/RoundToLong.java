/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;

/**
 * Implementations of {@link RoundTo} for {@code long}.
 * This class is generated. Edit {@code X-RoundTo.java.st} instead.
 */
class RoundToLong {
    static final RoundTo.Build BUILD = (source, field, points, convention) -> {
        // TODO handle 0 points case - currently falls through to floorMany/ceilingMany and crashes
        long[] p = points.stream().mapToLong(n -> n.longValue()).toArray();
        return switch (convention) {
            case Rounding.RoundingConvention.DOWN -> floor(source, field, p);
            case Rounding.RoundingConvention.UP -> ceiling(source, field, p);
        };
    };

    private static ExpressionEvaluator.Factory floor(Source source, ExpressionEvaluator.Factory field, long[] p) {
        return switch (p.length) {
            case 1 -> new RoundToLongConstantEvaluator.Factory(source, field, p[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundToLongFloor2Evaluator.Factory(source, field, p[0], p[1]);
            case 3 -> new RoundToLongFloor3Evaluator.Factory(source, field, p[0], p[1], p[2]);
            case 4 -> new RoundToLongFloor4Evaluator.Factory(source, field, p[0], p[1], p[2], p[3]);
            case 5 -> new RoundToLongFloor5Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4]);
            case 6 -> new RoundToLongFloor6Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5]);
            case 7 -> new RoundToLongFloor7Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6]);
            case 8 -> new RoundToLongFloor8Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]);
            case 9 -> new RoundToLongFloor9Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]);
            case 10 -> new RoundToLongFloor10Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            default -> floorMany(source, field, p);
        };
    }

    private static ExpressionEvaluator.Factory ceiling(Source source, ExpressionEvaluator.Factory field, long[] p) {
        return switch (p.length) {
            case 1 -> new RoundToLongConstantEvaluator.Factory(source, field, p[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundToLongCeiling2Evaluator.Factory(source, field, p[0], p[1]);
            case 3 -> new RoundToLongCeiling3Evaluator.Factory(source, field, p[0], p[1], p[2]);
            case 4 -> new RoundToLongCeiling4Evaluator.Factory(source, field, p[0], p[1], p[2], p[3]);
            case 5 -> new RoundToLongCeiling5Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4]);
            case 6 -> new RoundToLongCeiling6Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5]);
            case 7 -> new RoundToLongCeiling7Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6]);
            case 8 -> new RoundToLongCeiling8Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]);
            case 9 -> new RoundToLongCeiling9Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]);
            case 10 -> new RoundToLongCeiling10Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            default -> ceilingMany(source, field, p);
        };
    }

    private static ExpressionEvaluator.Factory floorMany(Source source, ExpressionEvaluator.Factory field, long[] p) {
        long interval = fixedInterval(p);
        if (interval > 0) {
            return new RoundToLongFloorIntervalEvaluator.Factory(source, field, p[0], p[p.length - 1], interval);
        }
        return new RoundToLongFloorSearchEvaluator.Factory(source, field, p);
    }

    private static ExpressionEvaluator.Factory ceilingMany(Source source, ExpressionEvaluator.Factory field, long[] p) {
        long interval = fixedInterval(p);
        if (interval > 0) {
            return new RoundToLongCeilingIntervalEvaluator.Factory(source, field, p[0], p[p.length - 1], interval);
        }
        return new RoundToLongCeilingSearchEvaluator.Factory(source, field, p);
    }

    private static long fixedInterval(long[] p) {
        long interval = (p[p.length - 1] - p[0]) / (p.length - 1);
        for (int i = 1; i < p.length; i++) {
            if (p[i] - p[i - 1] != interval) {
                return -1;
            }
        }
        return interval;
    }

    @Evaluator(extraName = "Constant")
    static long constant(long v, @Fixed long p) {
        return p;
    }

    @Evaluator(extraName = "FloorSearch")
    static long floorSearch(long v, @Fixed(includeInToString = false) long[] p) {
        int i = Arrays.binarySearch(p, v);
        return p[i >= 0 ? i : Math.max(0, -i - 2)];
    }

    @Evaluator(extraName = "CeilingSearch")
    static long ceilingSearch(long v, @Fixed(includeInToString = false) long[] p) {
        int i = Arrays.binarySearch(p, v);
        return p[i >= 0 ? i : Math.min(p.length - 1, -i - 1)];
    }

    @Evaluator(extraName = "FloorInterval")
    static long floorInterval(long v, @Fixed long lo, @Fixed long hi, @Fixed long interval) {
        if (v < lo) {
            return lo;
        }
        if (v > hi) {
            return hi;
        }
        return v - (v - lo) % interval;
    }

    @Evaluator(extraName = "CeilingInterval")
    static long ceilingInterval(long v, @Fixed long lo, @Fixed long hi, @Fixed long interval) {
        if (v <= lo) {
            return lo;
        }
        if (v > hi) {
            return hi;
        }
        long r = (v - lo) % interval;
        if (r == 0) {
            return v;
        }
        return v + interval - r;
    }

    @Evaluator(extraName = "Floor2")
    static long floor2(long v, @Fixed long p0, @Fixed long p1) {
        if (v < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "Floor3")
    static long floor3(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2) {
        if (v < p1) {
            return p0;
        }
        if (v < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "Floor4")
    static long floor4(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3) {
        if (v < p1) {
            return p0;
        }
        if (v < p2) {
            return p1;
        }
        if (v < p3) {
            return p2;
        }
        return p3;
    }

    /*
     * Manual binary search for 5 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor5")
    static long floor5(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3, @Fixed long p4) {
        if (v < p2) {
            if (v < p1) {
                return p0;
            }
            return p1;
        }
        if (v < p3) {
            return p2;
        }
        if (v < p4) {
            return p3;
        }
        return p4;
    }

    /*
     * Manual binary search for 6 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor6")
    static long floor6(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5
    ) {
        if (v < p2) {
            if (v < p1) {
                return p0;
            }
            return p1;
        }
        if (v < p4) {
            if (v < p3) {
                return p2;
            }
            return p3;
        }
        if (v < p5) {
            return p4;
        }
        return p5;
    }

    /*
     * Manual binary search for 7 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor7")
    static long floor7(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6
    ) {
        if (v < p3) {
            if (v < p1) {
                return p0;
            }
            if (v < p2) {
                return p1;
            }
            return p2;
        }
        if (v < p5) {
            if (v < p4) {
                return p3;
            }
            return p4;
        }
        if (v < p6) {
            return p5;
        }
        return p6;
    }

    /*
     * Manual binary search for 8 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor8")
    static long floor8(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7
    ) {
        if (v < p3) {
            if (v < p1) {
                return p0;
            }
            if (v < p2) {
                return p1;
            }
            return p2;
        }
        if (v < p5) {
            if (v < p4) {
                return p3;
            }
            return p4;
        }
        if (v < p6) {
            return p5;
        }
        if (v < p7) {
            return p6;
        }
        return p7;
    }

    /*
     * Manual binary search for 9 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor9")
    static long floor9(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8
    ) {
        if (v < p4) {
            if (v < p1) {
                return p0;
            }
            if (v < p2) {
                return p1;
            }
            if (v < p3) {
                return p2;
            }
            return p3;
        }
        if (v < p6) {
            if (v < p5) {
                return p4;
            }
            return p5;
        }
        if (v < p7) {
            return p6;
        }
        if (v < p8) {
            return p7;
        }
        return p8;
    }

    /*
     * Manual binary search for 10 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Floor10")
    static long floor10(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8,
        @Fixed long p9
    ) {
        if (v < p4) {
            if (v < p1) {
                return p0;
            }
            if (v < p2) {
                return p1;
            }
            if (v < p3) {
                return p2;
            }
            return p3;
        }
        if (v < p7) {
            if (v < p5) {
                return p4;
            }
            if (v < p6) {
                return p5;
            }
            return p6;
        }
        if (v < p8) {
            return p7;
        }
        if (v < p9) {
            return p8;
        }
        return p9;
    }

    @Evaluator(extraName = "Ceiling2")
    static long ceiling2(long v, @Fixed long p0, @Fixed long p1) {
        if (v <= p0) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "Ceiling3")
    static long ceiling3(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2) {
        if (v <= p0) {
            return p0;
        }
        if (v <= p1) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "Ceiling4")
    static long ceiling4(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3) {
        if (v <= p0) {
            return p0;
        }
        if (v <= p1) {
            return p1;
        }
        if (v <= p2) {
            return p2;
        }
        return p3;
    }

    /*
     * Manual binary search for 5 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling5")
    static long ceiling5(long v, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3, @Fixed long p4) {
        if (v <= p1) {
            if (v <= p0) {
                return p0;
            }
            return p1;
        }
        if (v <= p2) {
            return p2;
        }
        if (v <= p3) {
            return p3;
        }
        return p4;
    }

    /*
     * Manual binary search for 6 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling6")
    static long ceiling6(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5
    ) {
        if (v <= p1) {
            if (v <= p0) {
                return p0;
            }
            return p1;
        }
        if (v <= p3) {
            if (v <= p2) {
                return p2;
            }
            return p3;
        }
        if (v <= p4) {
            return p4;
        }
        return p5;
    }

    /*
     * Manual binary search for 7 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling7")
    static long ceiling7(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6
    ) {
        if (v <= p2) {
            if (v <= p0) {
                return p0;
            }
            if (v <= p1) {
                return p1;
            }
            return p2;
        }
        if (v <= p4) {
            if (v <= p3) {
                return p3;
            }
            return p4;
        }
        if (v <= p5) {
            return p5;
        }
        return p6;
    }

    /*
     * Manual binary search for 8 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling8")
    static long ceiling8(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7
    ) {
        if (v <= p2) {
            if (v <= p0) {
                return p0;
            }
            if (v <= p1) {
                return p1;
            }
            return p2;
        }
        if (v <= p4) {
            if (v <= p3) {
                return p3;
            }
            return p4;
        }
        if (v <= p5) {
            return p5;
        }
        if (v <= p6) {
            return p6;
        }
        return p7;
    }

    /*
     * Manual binary search for 9 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling9")
    static long ceiling9(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8
    ) {
        if (v <= p3) {
            if (v <= p0) {
                return p0;
            }
            if (v <= p1) {
                return p1;
            }
            if (v <= p2) {
                return p2;
            }
            return p3;
        }
        if (v <= p5) {
            if (v <= p4) {
                return p4;
            }
            return p5;
        }
        if (v <= p6) {
            return p6;
        }
        if (v <= p7) {
            return p7;
        }
        return p8;
    }

    /*
     * Manual binary search for 10 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "Ceiling10")
    static long ceiling10(
        long v,          // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8,
        @Fixed long p9
    ) {
        if (v <= p3) {
            if (v <= p0) {
                return p0;
            }
            if (v <= p1) {
                return p1;
            }
            if (v <= p2) {
                return p2;
            }
            return p3;
        }
        if (v <= p6) {
            if (v <= p4) {
                return p4;
            }
            if (v <= p5) {
                return p5;
            }
            return p6;
        }
        if (v <= p7) {
            return p7;
        }
        if (v <= p8) {
            return p8;
        }
        return p9;
    }
}
