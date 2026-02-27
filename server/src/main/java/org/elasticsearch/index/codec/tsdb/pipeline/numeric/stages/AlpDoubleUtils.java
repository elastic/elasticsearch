/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;

final class AlpDoubleUtils {

    private AlpDoubleUtils() {}

    static final int MAX_EXPONENT = 18;
    // NOTE: Fixed threshold for cache validation only — triggers a re-search of (e,f)
    // candidates when the cached pair produces too many exceptions for the new block.
    static final int CACHE_VALIDATION_THRESHOLD = 5;

    // NOTE: Average per-exception metadata cost: VInt position (~2 bytes) + raw Long (8 bytes).
    static final int DOUBLE_EXCEPTION_COST = 10;
    static final double[] POWERS_OF_TEN = new double[MAX_EXPONENT + 1];
    static final double[] NEG_POWERS_OF_TEN = new double[MAX_EXPONENT + 1];
    static final int SAMPLE_SIZE = 16;
    static final int MAX_NON_IMPROVE_STREAK = 8;
    static final double PRECISION_TOLERANCE = 1e-9;

    // NOTE: Bias rounding constant for the add/sub trick (Kahan's method).
    // Adding the bias to a finite double forces IEEE 754 rounding to integer
    // in the mantissa, avoiding the overhead of Math.round() in hot loops.
    // Safe only when abs(x) < FAST_ROUND_MAX_DOUBLE; beyond that, fall back to Math.round().
    static final double ROUNDING_BIAS_DOUBLE = 6755399441055744.0; // 2^52 + 2^51
    static final double FAST_ROUND_MAX_DOUBLE = (double) (1L << 52);

    static long fastRound(double x) {
        if (x >= 0) {
            return (long) (x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE;
        }
        return -((long) (-x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE);
    }

    // NOTE: Guarded fast round for ALP encode paths. Uses bias trick when magnitude
    // is safe, otherwise falls back to Math.round(). NaN/Inf values are never passed
    // here because ALP encode loops operate on finite sortable-long-encoded values.
    static long alpRound(double x) {
        if (x > -FAST_ROUND_MAX_DOUBLE && x < FAST_ROUND_MAX_DOUBLE) {
            return fastRound(x);
        }
        return Math.round(x);
    }

    static final int CAND_POOL_SIZE = 32;
    static final int TOP_K = 5;
    static final int PRE_SELECT_SAMPLE = 64;

    static {
        for (int i = 0; i <= MAX_EXPONENT; i++) {
            POWERS_OF_TEN[i] = Math.pow(10, i);
            NEG_POWERS_OF_TEN[i] = Math.pow(10, -i);
        }
    }

    static int estimatePrecision(double value, int maxExponent) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final double fractional = Math.abs(value) - Math.floor(Math.abs(value));
        if (fractional == 0.0) {
            return 0;
        }
        for (int p = 1; p <= maxExponent; p++) {
            if (Math.abs(fractional * POWERS_OF_TEN[p] - alpRound(fractional * POWERS_OF_TEN[p])) < PRECISION_TOLERANCE) {
                return p;
            }
        }
        return maxExponent;
    }

    static int countExceptions(final long[] values, int valueCount, int e, int f) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
        int exceptions = 0;
        for (int i = 0; i < valueCount; i++) {
            final double original = NumericUtils.sortableLongToDouble(values[i]);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            if (Double.doubleToRawLongBits(original) != Double.doubleToRawLongBits(decoded)) {
                exceptions++;
            }
        }
        return exceptions;
    }

    // NOTE: Returns how many bits ALP saves per value (maxOriginalBits - maxMantissaBits).
    // Uses sign-magnitude conversion so negative values do not inflate the bit count to 64.
    static int computeBitSavings(final long[] values, int valueCount, int e, int f) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        int maxOriginalBits = 0;
        int maxMantissaBits = 0;
        for (int i = 0; i < valueCount; i++) {
            final double original = NumericUtils.sortableLongToDouble(values[i]);
            final long mantissa = alpRound(original * mulFactor);
            final long origMag = values[i] ^ (values[i] >> 63);
            final long mantMag = mantissa ^ (mantissa >> 63);
            maxOriginalBits = Math.max(maxOriginalBits, Long.SIZE - Long.numberOfLeadingZeros(origMag));
            maxMantissaBits = Math.max(maxMantissaBits, Long.SIZE - Long.numberOfLeadingZeros(mantMag));
        }
        return Math.max(0, maxOriginalBits - maxMantissaBits);
    }

    // NOTE: Dynamic exception threshold — the more bits ALP saves per value, the more
    // exceptions we can tolerate before the metadata overhead erodes the savings. A 2×
    // safety margin ensures we only accept exceptions when the net benefit is meaningful,
    // avoiding CPU waste on marginal compression gains.
    static int maxExceptionPercent(int bitsSaved, int exceptionCost) {
        if (bitsSaved <= 0) return 0;
        return (bitsSaved * 100) / (8 * exceptionCost * 2);
    }

    // NOTE: Fused ALP encode transform: single pass over the block that converts
    // sortable-longs to integer mantissas in-place and collects exceptions.
    static int alpTransformBlock(final long[] values, int valueCount, int e, int f, final int[] excPositions, final long[] excValues) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];

        int excCount = 0;
        for (int i = 0; i < valueCount; i++) {
            final double original = NumericUtils.sortableLongToDouble(values[i]);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;

            if (Double.doubleToRawLongBits(original) == Double.doubleToRawLongBits(decoded)) {
                values[i] = encoded;
            } else {
                excPositions[excCount] = i;
                excValues[excCount] = values[i];
                excCount++;
                // NOTE: replace the exception slot with a nearby non-exception value to keep
                // bit-packed mantissa width low. Previous value is chosen for local
                // smoothness and favor delta-encoding; first exception uses 0 since there is no predecessor.
                values[i] = (i > 0) ? values[i - 1] : 0;
            }
        }
        return excCount;
    }

    // NOTE: Returns packed (e << 16) | f for a single double value.
    // Tries f=0 first (most common), then small f values for better coverage.
    // Returns 0 (e=0, f=0) if no precision matches or value is special.
    static int bestEFForSingleDouble(double value, int maxExponent) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final int p = estimatePrecision(value, maxExponent);
        for (int e = p; e <= maxExponent; e++) {
            for (int f = 0; f <= e; f++) {
                final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
                final long encoded = alpRound(value * mulFactor);
                final double decoded = encoded * POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
                if (Double.doubleToRawLongBits(value) == Double.doubleToRawLongBits(decoded)) {
                    return (e << 16) | f;
                }
            }
        }
        return 0;
    }

    // NOTE: Top-K (e,f) pre-selection for doubles. Samples the block, evaluates
    // top-K candidates on the full block. When sampling yields no candidates,
    // falls back to precision-estimation-based candidate generation.
    static int findBestEFDoubleTopK(
        final long[] values,
        int valueCount,
        int maxExponent,
        final int[] efOut,
        final int[] candE,
        final int[] candF,
        final int[] candCount
    ) {
        for (int i = 0; i < CAND_POOL_SIZE; i++) {
            candCount[i] = 0;
        }
        int poolUsed = 0;

        // NOTE: Fixed-stride deterministic sample (ALP paper S3.2).
        final int step = Math.max(1, valueCount / PRE_SELECT_SAMPLE);
        for (int i = 0; i < valueCount; i += step) {
            final double value = NumericUtils.sortableLongToDouble(values[i]);
            final int packed = bestEFForSingleDouble(value, maxExponent);
            final int e = packed >>> 16;
            final int f = packed & 0xFFFF;

            poolUsed = insertIntoPool(candE, candF, candCount, poolUsed, e, f);
        }

        // NOTE: Precision-estimation fallback when sampling yields no candidates.
        if (poolUsed == 0) {
            if (valueCount == 0) {
                efOut[0] = -1;
                efOut[1] = -1;
                return valueCount;
            }
            int minP = MAX_EXPONENT;
            int maxP = 0;
            final int precStep = Math.max(1, valueCount / SAMPLE_SIZE);
            for (int i = 0; i < valueCount; i += precStep) {
                final int p = estimatePrecision(NumericUtils.sortableLongToDouble(values[i]), maxExponent);
                minP = Math.min(minP, p);
                maxP = Math.max(maxP, p);
            }
            for (int e = minP; e <= maxExponent; e++) {
                for (int f = 0; f <= e; f++) {
                    poolUsed = insertIntoPool(candE, candF, candCount, poolUsed, e, f);
                }
            }
        }

        return evaluateTopKDouble(values, valueCount, efOut, candE, candF, candCount, poolUsed);
    }

    private static int insertIntoPool(final int[] candE, final int[] candF, final int[] candCount, int poolUsed, int e, int f) {
        for (int j = 0; j < poolUsed; j++) {
            if (candE[j] == e && candF[j] == f) {
                candCount[j]++;
                return poolUsed;
            }
        }
        if (poolUsed < CAND_POOL_SIZE) {
            candE[poolUsed] = e;
            candF[poolUsed] = f;
            candCount[poolUsed] = 1;
            return poolUsed + 1;
        }
        // NOTE: Pool full: evict the least-frequent entry so later samples
        // can displace stale candidates that appeared only in early positions.
        int minIdx = 0;
        int minCount = candCount[0];
        for (int j = 1; j < CAND_POOL_SIZE; j++) {
            if (candCount[j] < minCount) {
                minCount = candCount[j];
                minIdx = j;
            }
        }
        candE[minIdx] = e;
        candF[minIdx] = f;
        candCount[minIdx] = 1;
        return poolUsed;
    }

    private static int evaluateTopKDouble(
        final long[] values,
        int valueCount,
        final int[] efOut,
        final int[] candE,
        final int[] candF,
        final int[] candCount,
        int poolUsed
    ) {
        int bestE = 0;
        int bestF = 0;
        int bestExceptions = valueCount;

        for (int k = 0; k < TOP_K && k < poolUsed; k++) {
            int maxIdx = -1;
            int maxCount = -1;
            for (int j = 0; j < poolUsed; j++) {
                if (candCount[j] > maxCount) {
                    maxCount = candCount[j];
                    maxIdx = j;
                }
            }
            if (maxIdx < 0 || maxCount <= 0) break;

            final int e = candE[maxIdx];
            final int f = candF[maxIdx];
            candCount[maxIdx] = -candCount[maxIdx]; // mark as selected

            final int exceptions = countExceptions(values, valueCount, e, f);
            if (exceptions < bestExceptions) {
                bestExceptions = exceptions;
                bestE = e;
                bestF = f;
                if (exceptions == 0) break;
            }
        }

        boolean identityEvaluated = false;
        for (int j = 0; j < poolUsed; j++) {
            if (candE[j] == 0 && candF[j] == 0 && candCount[j] < 0) {
                identityEvaluated = true;
                break;
            }
        }
        if (identityEvaluated == false) {
            final int exceptions = countExceptions(values, valueCount, 0, 0);
            if (exceptions < bestExceptions) {
                bestExceptions = exceptions;
                bestE = 0;
                bestF = 0;
            }
        }

        for (int j = 0; j < poolUsed; j++) {
            if (candCount[j] < 0) {
                candCount[j] = -candCount[j];
            }
        }

        efOut[0] = bestE;
        efOut[1] = bestF;
        return bestExceptions;
    }
}
