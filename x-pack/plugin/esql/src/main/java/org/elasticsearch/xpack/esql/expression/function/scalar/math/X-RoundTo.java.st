/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

// begin generated imports
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.tree.Source;

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
    interface Build {
        ExpressionEvaluator.Factory build(
            Source source,
            ExpressionEvaluator.Factory field,
            $type$[] points,
            Rounding.RoundingConvention convention
        );
    }

    static final Build BUILD = (source, field, f, roundingConvention) -> {
        if (Rounding.RoundingConvention.DOWN.equals(roundingConvention)) {
            return switch (f.length) {
                // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
                case 1 -> new RoundTo$Type$1Evaluator.Factory(source, field, f[0]);
                /*
                 * These hand-unrolled implementations are even faster than the linear scan implementations.
                 */
                case 2 -> new RoundTo$Type$2Evaluator.Factory(source, field, f[0], f[1]);
                case 3 -> new RoundTo$Type$3Evaluator.Factory(source, field, f[0], f[1], f[2]);
                case 4 -> new RoundTo$Type$4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
                case 5 -> new RoundTo$Type$5Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4]);
                case 6 -> new RoundTo$Type$6Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5]);
                case 7 -> new RoundTo$Type$7Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6]);
                case 8 -> new RoundTo$Type$8Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
                case 9 -> new RoundTo$Type$9Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
                case 10 -> new RoundTo$Type$10Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9]);
                /*
                 * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
                 * on 2025-05-22.
                 */
                default -> {
$if(long||int)$
                    $type$ interval = extractFixedInterval(f);
                    if (interval > 0) {
                        yield new RoundTo$Type$FixedIntervalEvaluator.Factory(source, field, f[0], f[f.length - 1], interval);
                    }
$endif$
                    yield new RoundTo$Type$BinarySearchEvaluator.Factory(source, field, f);
                }
            };
        } else if (Rounding.RoundingConvention.UP.equals(roundingConvention)) {
            return switch (f.length) {
                // N=1: both conventions return the single point
                case 1 -> new RoundTo$Type$1Evaluator.Factory(source, field, f[0]);
                case 2 -> new RoundTo$Type$2UpEvaluator.Factory(source, field, f[0], f[1]);
                case 3 -> new RoundTo$Type$3UpEvaluator.Factory(source, field, f[0], f[1], f[2]);
                case 4 -> new RoundTo$Type$4UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
                case 5 -> new RoundTo$Type$5UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4]);
                case 6 -> new RoundTo$Type$6UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5]);
                case 7 -> new RoundTo$Type$7UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6]);
                case 8 -> new RoundTo$Type$8UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
                case 9 -> new RoundTo$Type$9UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
$if(double)$
                case 10 -> new RoundTo$Type$10UpEvaluator.Factory(
                    source,
                    field,
                    f[0],
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    f[7],
                    f[8],
                    f[9]
                );
$else$
                case 10 -> new RoundTo$Type$10UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9]);
$endif$
                default -> {
$if(long||int)$
                    $type$ interval = extractFixedInterval(f);
                    if (interval > 0) {
                        yield new RoundTo$Type$FixedIntervalUpEvaluator.Factory(source, field, f[0], f[f.length - 1], interval);
                    }
$endif$
                    yield new RoundTo$Type$BinarySearchUpEvaluator.Factory(source, field, f);
                }
            };
        } else {
            throw new IllegalArgumentException("Unknown rounding convention: [ " + roundingConvention + " ]");
        }
    };

    @Evaluator(extraName = "BinarySearch")
    static $type$ process($type$ field, @Fixed(includeInToString = false) $type$[] points) {
        int idx = Arrays.binarySearch(points, field);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    @Evaluator(extraName = "BinarySearchUp")
    static $type$ processUp($type$ field, @Fixed(includeInToString = false) $type$[] points) {
        int idx = Arrays.binarySearch(points, field);
        if (idx >= 0) {
            return points[idx];
        }
        int insertionPoint = -idx - 1;
        return points[Math.min(insertionPoint, points.length - 1)];
    }

$if(long||int)$
    private static $type$ extractFixedInterval($type$[] points) {
        $type$ interval = (points[points.length - 1] - points[0]) / (points.length - 1);
        for (int i = 1; i < points.length; i++) {
            if (points[i] - points[i - 1] != interval) {
                return -1;
            }
        }
        return interval;
    }

    @Evaluator(extraName = "FixedInterval")
    static $type$ processFixed($type$ field, @Fixed $type$ first, @Fixed $type$ last, @Fixed $type$ interval) {
        if (field < first) {
            return first; // almost never true in practice; predictor skips this
        }
        if (field > last) {
            return last; // almost never true in practice; predictor skips this
        }
        return field - (field - first) % interval;
    }

    @Evaluator(extraName = "FixedIntervalUp")
    static $type$ processFixedUp($type$ field, @Fixed $type$ first, @Fixed $type$ last, @Fixed $type$ interval) {
        if (field <= first) {
            return first;
        }
        if (field > last) {
            return last;
        }
        $type$ rem = (field - first) % interval;
        return rem == 0 ? field : field + (interval - rem);
    }
$endif$

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

    /*
     * Manual binary search for 5 rounding points, it is faster than linear search or array style binary search.
     */
    @Evaluator(extraName = "5")
    static $type$ process($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3, @Fixed $type$ p4) {
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
    static $type$ process(
        $type$ field,      // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5
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
    static $type$ process(
        $type$ field,      // hack to keep the formatter happy.
        @Fixed $type$ p0,  // int is so short this should be on one line but double is not.
        @Fixed $type$ p1,  // That's not compatible with the templates.
        @Fixed $type$ p2,  // So we comment to make the formatter not try to change the line.
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6
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
    static $type$ process(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7
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
    static $type$ process(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8
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
    static $type$ process(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8,
        @Fixed $type$ p9
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

    // -------------------------------------------------------------------------
    // ROUND UP
    // -------------------------------------------------------------------------

    @Evaluator(extraName = "2Up")
    static $type$ processUp($type$ field, @Fixed $type$ p0, @Fixed $type$ p1) {
        if (field <= p0) return p0;
        return p1;
    }

    @Evaluator(extraName = "3Up")
    static $type$ processUp($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        return p2;
    }

    @Evaluator(extraName = "4Up")
    static $type$ processUp($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        return p3;
    }

    @Evaluator(extraName = "5Up")
    static $type$ processUp($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3, @Fixed $type$ p4) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        return p4;
    }

$if(double)$
    @Evaluator(extraName = "6Up")
    static $type$ processUp(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5
    ) {
$else$
    @Evaluator(extraName = "6Up")
    static $type$ processUp($type$ field, @Fixed $type$ p0, @Fixed $type$ p1, @Fixed $type$ p2, @Fixed $type$ p3, @Fixed $type$ p4, @Fixed $type$ p5) {
$endif$
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        return p5;
    }

    @Evaluator(extraName = "7Up")
    static $type$ processUp(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6
    ) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        if (field <= p5) return p5;
        return p6;
    }

    @Evaluator(extraName = "8Up")
    static $type$ processUp(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7
    ) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        if (field <= p5) return p5;
        if (field <= p6) return p6;
        return p7;
    }

    @Evaluator(extraName = "9Up")
    static $type$ processUp(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8
    ) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        if (field <= p5) return p5;
        if (field <= p6) return p6;
        if (field <= p7) return p7;
        return p8;
    }

    @Evaluator(extraName = "10Up")
    static $type$ processUp(
        $type$ field,
        @Fixed $type$ p0,
        @Fixed $type$ p1,
        @Fixed $type$ p2,
        @Fixed $type$ p3,
        @Fixed $type$ p4,
        @Fixed $type$ p5,
        @Fixed $type$ p6,
        @Fixed $type$ p7,
        @Fixed $type$ p8,
        @Fixed $type$ p9
    ) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        if (field <= p5) return p5;
        if (field <= p6) return p6;
        if (field <= p7) return p7;
        if (field <= p8) return p8;
        return p9;
    }
}
