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
class RoundToLong {
    interface Build {
        ExpressionEvaluator.Factory build(
            Source source,
            ExpressionEvaluator.Factory field,
            long[] points,
            Rounding.RoundingConvention convention
        );
    }

    static final Build BUILD = (source, field, f, roundingConvention) -> {
        if (Rounding.RoundingConvention.DOWN.equals(roundingConvention)) {
            return switch (f.length) {
                // TODO should be a consistent way to do the 0 version - is CASE(MV_COUNT(f) == 1, f[0])
                case 1 -> new RoundToLong1Evaluator.Factory(source, field, f[0]);
                /*
                 * These hand-unrolled implementations are even faster than the linear scan implementations.
                 */
                case 2 -> new RoundToLong2Evaluator.Factory(source, field, f[0], f[1]);
                case 3 -> new RoundToLong3Evaluator.Factory(source, field, f[0], f[1], f[2]);
                case 4 -> new RoundToLong4Evaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
                case 5 -> new RoundToLong5Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4]);
                case 6 -> new RoundToLong6Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5]);
                case 7 -> new RoundToLong7Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6]);
                case 8 -> new RoundToLong8Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
                case 9 -> new RoundToLong9Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
                case 10 -> new RoundToLong10Evaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9]);
                /*
                 * Break point of 10 experimentally derived on Nik's laptop (13th Gen Intel(R) Core(TM) i7-1370P)
                 * on 2025-05-22.
                 */
                default -> {
                    long interval = extractFixedInterval(f);
                    if (interval > 0) {
                        yield new RoundToLongFixedIntervalEvaluator.Factory(source, field, f[0], f[f.length - 1], interval);
                    }
                    yield new RoundToLongBinarySearchEvaluator.Factory(source, field, f);
                }
            };
        } else if (Rounding.RoundingConvention.UP.equals(roundingConvention)) {
            return switch (f.length) {
                // N=1: both conventions return the single point
                case 1 -> new RoundToLong1Evaluator.Factory(source, field, f[0]);
                case 2 -> new RoundToLong2UpEvaluator.Factory(source, field, f[0], f[1]);
                case 3 -> new RoundToLong3UpEvaluator.Factory(source, field, f[0], f[1], f[2]);
                case 4 -> new RoundToLong4UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3]);
                case 5 -> new RoundToLong5UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4]);
                case 6 -> new RoundToLong6UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5]);
                case 7 -> new RoundToLong7UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6]);
                case 8 -> new RoundToLong8UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
                case 9 -> new RoundToLong9UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
                case 10 -> new RoundToLong10UpEvaluator.Factory(source, field, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9]);
                default -> {
                    long interval = extractFixedInterval(f);
                    if (interval > 0) {
                        yield new RoundToLongFixedIntervalUpEvaluator.Factory(source, field, f[0], f[f.length - 1], interval);
                    }
                    yield new RoundToLongBinarySearchUpEvaluator.Factory(source, field, f);
                }
            };
        } else {
            throw new IllegalArgumentException("Unknown rounding convention: [ " + roundingConvention + " ]");
        }
    };

    @Evaluator(extraName = "BinarySearch")
    static long process(long field, @Fixed(includeInToString = false) long[] points) {
        int idx = Arrays.binarySearch(points, field);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    @Evaluator(extraName = "BinarySearchUp")
    static long processUp(long field, @Fixed(includeInToString = false) long[] points) {
        int idx = Arrays.binarySearch(points, field);
        if (idx >= 0) {
            return points[idx];
        }
        int insertionPoint = -idx - 1;
        return points[Math.min(insertionPoint, points.length - 1)];
    }

    private static long extractFixedInterval(long[] points) {
        long interval = (points[points.length - 1] - points[0]) / (points.length - 1);
        for (int i = 1; i < points.length; i++) {
            if (points[i] - points[i - 1] != interval) {
                return -1;
            }
        }
        return interval;
    }

    @Evaluator(extraName = "FixedInterval")
    static long processFixed(long field, @Fixed long first, @Fixed long last, @Fixed long interval) {
        if (field < first) {
            return first; // almost never true in practice; predictor skips this
        }
        if (field > last) {
            return last; // almost never true in practice; predictor skips this
        }
        return field - (field - first) % interval;
    }

    @Evaluator(extraName = "FixedIntervalUp")
    static long processFixedUp(long field, @Fixed long first, @Fixed long last, @Fixed long interval) {
        if (field <= first) {
            return first;
        }
        if (field > last) {
            return last;
        }
        long rem = (field - first) % interval;
        return rem == 0 ? field : field + (interval - rem);
    }

    @Evaluator(extraName = "1")
    static long process(long field, @Fixed long p0) {
        return p0;
    }

    @Evaluator(extraName = "2")
    static long process(long field, @Fixed long p0, @Fixed long p1) {
        if (field < p1) {
            return p0;
        }
        return p1;
    }

    @Evaluator(extraName = "3")
    static long process(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2) {
        if (field < p1) {
            return p0;
        }
        if (field < p2) {
            return p1;
        }
        return p2;
    }

    @Evaluator(extraName = "4")
    static long process(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3) {
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
    static long process(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3, @Fixed long p4) {
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
    static long process(
        long field,      // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5
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
    static long process(
        long field,      // hack to keep the formatter happy.
        @Fixed long p0,  // int is so short this should be on one line but double is not.
        @Fixed long p1,  // That's not compatible with the templates.
        @Fixed long p2,  // So we comment to make the formatter not try to change the line.
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6
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
    static long process(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7
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
    static long process(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8
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
    static long process(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8,
        @Fixed long p9
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
    static long processUp(long field, @Fixed long p0, @Fixed long p1) {
        if (field <= p0) return p0;
        return p1;
    }

    @Evaluator(extraName = "3Up")
    static long processUp(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        return p2;
    }

    @Evaluator(extraName = "4Up")
    static long processUp(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        return p3;
    }

    @Evaluator(extraName = "5Up")
    static long processUp(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3, @Fixed long p4) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        return p4;
    }

    @Evaluator(extraName = "6Up")
    static long processUp(long field, @Fixed long p0, @Fixed long p1, @Fixed long p2, @Fixed long p3, @Fixed long p4, @Fixed long p5) {
        if (field <= p0) return p0;
        if (field <= p1) return p1;
        if (field <= p2) return p2;
        if (field <= p3) return p3;
        if (field <= p4) return p4;
        return p5;
    }

    @Evaluator(extraName = "7Up")
    static long processUp(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6
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
    static long processUp(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7
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
    static long processUp(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8
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
    static long processUp(
        long field,
        @Fixed long p0,
        @Fixed long p1,
        @Fixed long p2,
        @Fixed long p3,
        @Fixed long p4,
        @Fixed long p5,
        @Fixed long p6,
        @Fixed long p7,
        @Fixed long p8,
        @Fixed long p9
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
