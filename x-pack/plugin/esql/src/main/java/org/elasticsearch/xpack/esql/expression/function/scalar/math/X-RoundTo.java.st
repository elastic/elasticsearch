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
 * Implementations of {@link RoundTo} for {@code $type$}.
 * This class is generated. Edit {@code X-RoundTo.java.st} instead.
 */
class RoundTo$Type$ {
    static final RoundTo.Build BUILD = (source, field, points, convention) -> {
        // TODO handle 0 points case - currently falls through to floorMany/ceilingMany and crashes
        $type$[] p = points.stream().mapTo$Type$(n -> n.$type$Value()).toArray();
        return switch (convention) {
            case Rounding.RoundingConvention.DOWN -> floor(source, field, p);
            case Rounding.RoundingConvention.UP -> ceiling(source, field, p);
        };
    };

    private static ExpressionEvaluator.Factory floor(Source source, ExpressionEvaluator.Factory field, $type$[] p) {
        return switch (p.length) {
            case 1 -> new RoundTo$Type$ConstantEvaluator.Factory(source, field, p[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundTo$Type$Floor2Evaluator.Factory(source, field, p[0], p[1]);
            case 3 -> new RoundTo$Type$Floor3Evaluator.Factory(source, field, p[0], p[1], p[2]);
            case 4 -> new RoundTo$Type$Floor4Evaluator.Factory(source, field, p[0], p[1], p[2], p[3]);
            case 5 -> new RoundTo$Type$Floor5Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4]);
            case 6 -> new RoundTo$Type$Floor6Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5]);
            case 7 -> new RoundTo$Type$Floor7Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6]);
            case 8 -> new RoundTo$Type$Floor8Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]);
            case 9 -> new RoundTo$Type$Floor9Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]);
            case 10 -> new RoundTo$Type$Floor10Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]);
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            default -> floorMany(source, field, p);
        };
    }

    private static ExpressionEvaluator.Factory ceiling(Source source, ExpressionEvaluator.Factory field, $type$[] p) {
        return switch (p.length) {
            case 1 -> new RoundTo$Type$ConstantEvaluator.Factory(source, field, p[0]);
            /*
             * These hand-unrolled implementations are even faster than the linear scan implementations.
             */
            case 2 -> new RoundTo$Type$Ceiling2Evaluator.Factory(source, field, p[0], p[1]);
            case 3 -> new RoundTo$Type$Ceiling3Evaluator.Factory(source, field, p[0], p[1], p[2]);
            case 4 -> new RoundTo$Type$Ceiling4Evaluator.Factory(source, field, p[0], p[1], p[2], p[3]);
            case 5 -> new RoundTo$Type$Ceiling5Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4]);
            case 6 -> new RoundTo$Type$Ceiling6Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5]);
            case 7 -> new RoundTo$Type$Ceiling7Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6]);
            case 8 -> new RoundTo$Type$Ceiling8Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]);
            case 9 -> new RoundTo$Type$Ceiling9Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]);
$if(double)$
            case 10 -> new RoundTo$Type$Ceiling10Evaluator.Factory(
                source,
                field,
                p[0],
                p[1],
                p[2],
                p[3],
                p[4],
                p[5],
                p[6],
                p[7],
                p[8],
                p[9]
            );
$else$
            case 10 -> new RoundTo$Type$Ceiling10Evaluator.Factory(source, field, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]);
$endif$
            /*
             * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
             * on 2025-05-22.
             */
            default -> ceilingMany(source, field, p);
        };
    }

    private static ExpressionEvaluator.Factory floorMany(Source source, ExpressionEvaluator.Factory field, $type$[] p) {
$if(long||int)$
        $type$ interval = fixedInterval(p);
        if (interval > 0) {
            return new RoundTo$Type$FloorIntervalEvaluator.Factory(source, field, p[0], p[p.length - 1], interval);
        }
$endif$
        return new RoundTo$Type$FloorSearchEvaluator.Factory(source, field, p);
    }

    private static ExpressionEvaluator.Factory ceilingMany(Source source, ExpressionEvaluator.Factory field, $type$[] p) {
$if(long||int)$
        $type$ interval = fixedInterval(p);
        if (interval > 0) {
            return new RoundTo$Type$CeilingIntervalEvaluator.Factory(source, field, p[0], p[p.length - 1], interval);
        }
$endif$
        return new RoundTo$Type$CeilingSearchEvaluator.Factory(source, field, p);
    }

$if(long||int)$
    private static $type$ fixedInterval($type$[] p) {
        $type$ interval = (p[p.length - 1] - p[0]) / (p.length - 1);
        for (int i = 1; i < p.length; i++) {
            if (p[i] - p[i - 1] != interval) {
                return -1;
            }
        }
        return interval;
    }
$endif$

    @Evaluator(extraName = "Constant")
    static $type$ constant($type$ v, @Fixed $type$ p) {
        return p;
    }

    @Evaluator(extraName = "FloorSearch")
    static $type$ floorSearch($type$ v, @Fixed(includeInToString = false) $type$[] p) {
        int i = Arrays.binarySearch(p, v);
        return p[i >= 0 ? i : Math.max(0, -i - 2)];
    }

    @Evaluator(extraName = "CeilingSearch")
    static $type$ ceilingSearch($type$ v, @Fixed(includeInToString = false) $type$[] p) {
        int i = Arrays.binarySearch(p, v);
        return p[i >= 0 ? i : Math.min(p.length - 1, -i - 1)];
    }

$if(long||int)$
    @Evaluator(extraName = "FloorInterval")
    static $type$ floorInterval($type$ v, @Fixed $type$ lo, @Fixed $type$ hi, @Fixed $type$ interval) {
        if (v < lo) {
            return lo;
        }
        if (v > hi) {
            return hi;
        }
        return v - (v - lo) % interval;
    }

    @Evaluator(extraName = "CeilingInterval")
    static $type$ ceilingInterval($type$ v, @Fixed $type$ lo, @Fixed $type$ hi, @Fixed $type$ interval) {
        if (v <= lo) {
            return lo;
        }
        if (v > hi) {
            return hi;
        }
        $type$ r = (v - lo) % interval;
        if (r == 0) {
            return v;
        }
        return v + interval - r;
    }
$endif$

    @Evaluator(extraName = "Floor2")
    static $type$ floor2($type$ v, @Fixed $type$ p0, @Fixed $type$ p1) {
        if (v < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "Floor3")
    static $type$ floor3($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2) {
        if (v < p1) {
            return p0;
        }
        if (v < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "Floor4")
    static $type$ floor4($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3) {
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
    static $type$ floor5($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3, @Fixed $type$ p4) {
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
    static $type$ floor6(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5
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
    static $type$ floor7(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6
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
    static $type$ floor8(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7
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
    static $type$ floor9(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8
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
    static $type$ floor10(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8,
        @Fixed $type$ p9
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
    static $type$ ceiling2($type$ v, @Fixed $type$ p0, @Fixed $type$ p1) {
        if (v <= p0) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "Ceiling3")
    static $type$ ceiling3($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2) {
        if (v <= p0) {
            return p0;
        }
        if (v <= p1) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "Ceiling4")
    static $type$ ceiling4($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3) {
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
    static $type$ ceiling5($type$ v, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3, @Fixed $type$ p4) {
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
    static $type$ ceiling6(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5
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
    static $type$ ceiling7(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6
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
    static $type$ ceiling8(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7
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
    static $type$ ceiling9(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8
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
    static $type$ ceiling10(
        $type$ v,          // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8,
        @Fixed $type$ p9
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
