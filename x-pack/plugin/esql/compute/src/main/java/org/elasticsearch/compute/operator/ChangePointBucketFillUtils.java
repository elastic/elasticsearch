/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Utilities for computing which bucket keys to synthesize before {@link ChangePointOperator}.
 */
public final class ChangePointBucketFillUtils {

    /**
     * Minimum number of values required by {@link org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector},
     * equal to {@code (2 * MINIMUM_BUCKETS) + 2} in that class ({@code MINIMUM_BUCKETS} is 10).
     */
    public static final int MINIMUM_BUCKET_COUNT = 22;

    private ChangePointBucketFillUtils() {}

    public static int countBucketsInRange(Rounding.Prepared rounding, long minDate, long maxDate) {
        int count = 0;
        long current = rounding.round(minDate);
        while (current < maxDate) {
            count++;
            current = rounding.nextRoundingValue(current);
        }
        return count;
    }

    /**
     * Returns the sorted bucket keys that should be present. Interior gaps between consecutive real keys
     * are always filled. When fewer than {@link #MINIMUM_BUCKET_COUNT} real bucket keys are present and
     * interior gap fill alone does not reach the minimum, buckets are added alternately to the left and
     * right of the real-key cluster. When one side reaches the timerange edge, extension continues on the
     * other side only until the minimum is reached or both sides are exhausted. Real bucket keys are
     * never removed.
     */
    public static List<Long> computeFilledKeys(NavigableSet<Long> existingInRange, Rounding.Prepared rounding, long minDate, long maxDate) {
        if (existingInRange.isEmpty()) {
            return List.of();
        }

        TreeSet<Long> result = new TreeSet<>(existingInRange);

        List<Long> sorted = new ArrayList<>(existingInRange);
        for (int i = 0; i < sorted.size() - 1; i++) {
            long t = sorted.get(i);
            long next = sorted.get(i + 1);
            while (true) {
                t = rounding.nextRoundingValue(t);
                if (t >= next) {
                    break;
                }
                if (t >= minDate && t < maxDate) {
                    result.add(t);
                }
            }
        }

        if (existingInRange.size() < MINIMUM_BUCKET_COUNT && result.size() < MINIMUM_BUCKET_COUNT) {
            extendAroundCluster(result, rounding, minDate, maxDate);
        }

        return new ArrayList<>(result);
    }

    public static boolean needsFill(NavigableSet<Long> existingInRange, Rounding.Prepared rounding, long minDate, long maxDate) {
        return computeFilledKeys(existingInRange, rounding, minDate, maxDate).size() != existingInRange.size();
    }

    private static void extendAroundCluster(TreeSet<Long> result, Rounding.Prepared rounding, long minDate, long maxDate) {
        long left = result.first();
        long right = result.last();
        boolean preferRight = true;

        while (result.size() < MINIMUM_BUCKET_COUNT) {
            boolean extended = false;
            if (preferRight) {
                extended = tryExtendRight(result, rounding, maxDate, right);
                if (extended) {
                    right = result.last();
                }
            } else {
                extended = tryExtendLeft(result, rounding, minDate, left);
                if (extended) {
                    left = result.first();
                }
            }

            if (extended == false) {
                if (preferRight) {
                    extended = tryExtendLeft(result, rounding, minDate, left);
                    if (extended) {
                        left = result.first();
                    }
                } else {
                    extended = tryExtendRight(result, rounding, maxDate, right);
                    if (extended) {
                        right = result.last();
                    }
                }
            }

            if (extended == false) {
                break;
            }
            preferRight = preferRight == false;
        }
    }

    private static boolean tryExtendRight(TreeSet<Long> result, Rounding.Prepared rounding, long maxDate, long right) {
        long next = rounding.nextRoundingValue(right);
        if (next < maxDate) {
            result.add(next);
            return true;
        }
        return false;
    }

    private static boolean tryExtendLeft(TreeSet<Long> result, Rounding.Prepared rounding, long minDate, long left) {
        long previous = previousRoundingValue(rounding, left, minDate);
        if (previous >= minDate && previous < left) {
            result.add(previous);
            return true;
        }
        return false;
    }

    private static long previousRoundingValue(Rounding.Prepared rounding, long key, long minDate) {
        long t = rounding.round(minDate);
        long previous = t;
        while (t < key) {
            previous = t;
            t = rounding.nextRoundingValue(t);
        }
        return previous < key ? previous : Long.MIN_VALUE;
    }
}
