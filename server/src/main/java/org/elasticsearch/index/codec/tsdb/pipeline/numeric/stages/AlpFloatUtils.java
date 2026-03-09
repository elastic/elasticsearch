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

final class AlpFloatUtils {

    private AlpFloatUtils() {}

    static final int MAX_EXPONENT = 10;
    // NOTE: Fixed threshold for cache validation only — triggers a re-search of (e,f)
    // candidates when the cached pair produces too many exceptions for the new block.
    static final int CACHE_VALIDATION_THRESHOLD = 5;

    // NOTE: Average per-exception metadata cost: VInt position (~2 bytes) + ZInt value (~4 bytes).
    static final int FLOAT_EXCEPTION_COST = 6;
    static final float[] POWERS_OF_TEN_FLOAT = new float[MAX_EXPONENT + 1];
    static final float[] NEG_POWERS_OF_TEN_FLOAT = new float[MAX_EXPONENT + 1];
    static final int SAMPLE_SIZE = 16;
    static final int MAX_NON_IMPROVE_STREAK = 8;
    static final float PRECISION_TOLERANCE = 1e-4f;

    // NOTE: Bias rounding constant for float (Kahan's method).
    // 2^23 + 2^22 — matches float's 23-bit mantissa.
    static final float ROUNDING_BIAS_FLOAT = 12582912.0f;
    static final float FAST_ROUND_MAX_FLOAT = (float) (1 << 23);

    static final int CAND_POOL_SIZE = 32;
    static final int TOP_K = 5;
    static final int PRE_SELECT_SAMPLE = 64;

    static {
        for (int i = 0; i <= MAX_EXPONENT; i++) {
            POWERS_OF_TEN_FLOAT[i] = (float) Math.pow(10, i);
            NEG_POWERS_OF_TEN_FLOAT[i] = (float) Math.pow(10, -i);
        }
    }

    // NOTE: Static helpers so JIT can inline. No NaN/Inf guard needed;
    // ALP encode paths only process finite sortable-encoded values.
    static int fastRound(float x) {
        if (x >= 0) {
            return (int) (x + ROUNDING_BIAS_FLOAT) - (int) ROUNDING_BIAS_FLOAT;
        }
        return -((int) (-x + ROUNDING_BIAS_FLOAT) - (int) ROUNDING_BIAS_FLOAT);
    }

    // NOTE: Guarded fast round for ALP encode paths. Uses bias trick when magnitude
    // is safe, otherwise falls back to Math.round().
    static int alpRound(float x) {
        if (x > -FAST_ROUND_MAX_FLOAT && x < FAST_ROUND_MAX_FLOAT) {
            return fastRound(x);
        }
        return Math.round(x);
    }

    static int estimatePrecisionFloat(float value, int maxExponent) {
        if (Float.isNaN(value) || Float.isInfinite(value) || value == 0.0f) {
            return 0;
        }
        final float fractional = Math.abs(value) - (float) Math.floor(Math.abs(value));
        if (fractional == 0.0f) {
            return 0;
        }
        for (int p = 1; p <= maxExponent; p++) {
            if (Math.abs(fractional * POWERS_OF_TEN_FLOAT[p] - alpRound(fractional * POWERS_OF_TEN_FLOAT[p])) < PRECISION_TOLERANCE) {
                return p;
            }
        }
        return maxExponent;
    }

    static int countExceptionsFloat(final long[] values, int valueCount, int e, int f) {
        final float mulFactor = POWERS_OF_TEN_FLOAT[e] * NEG_POWERS_OF_TEN_FLOAT[f];
        final float decodeMul = POWERS_OF_TEN_FLOAT[f] * NEG_POWERS_OF_TEN_FLOAT[e];
        int exceptions = 0;
        for (int i = 0; i < valueCount; i++) {
            final float original = NumericUtils.sortableIntToFloat((int) values[i]);
            final int encoded = alpRound(original * mulFactor);
            final float decoded = encoded * decodeMul;
            if (Float.floatToRawIntBits(original) != Float.floatToRawIntBits(decoded)) {
                exceptions++;
            }
        }
        return exceptions;
    }

    // NOTE: Returns how many bits ALP saves per value (maxOriginalBits - maxMantissaBits).
    // Uses sign-magnitude conversion so negative values do not inflate the bit count to 32.
    static int computeBitSavings(final long[] values, int valueCount, int e, int f) {
        final float mulFactor = POWERS_OF_TEN_FLOAT[e] * NEG_POWERS_OF_TEN_FLOAT[f];
        int maxOriginalBits = 0;
        int maxMantissaBits = 0;
        for (int i = 0; i < valueCount; i++) {
            final float original = NumericUtils.sortableIntToFloat((int) values[i]);
            final int mantissa = alpRound(original * mulFactor);
            final int origMag = (int) values[i] ^ ((int) values[i] >> 31);
            final int mantMag = mantissa ^ (mantissa >> 31);
            maxOriginalBits = Math.max(maxOriginalBits, Integer.SIZE - Integer.numberOfLeadingZeros(origMag));
            maxMantissaBits = Math.max(maxMantissaBits, Integer.SIZE - Integer.numberOfLeadingZeros(mantMag));
        }
        return Math.max(0, maxOriginalBits - maxMantissaBits);
    }

    // NOTE: Dynamic exception threshold — mirrors AlpDoubleUtils.maxExceptionPercent.
    // See that method for the full rationale.
    static int maxExceptionPercent(int bitsSaved, int exceptionCost) {
        if (bitsSaved <= 0) return 0;
        return (bitsSaved * 100) / (8 * exceptionCost * 2);
    }

    // NOTE: Fused ALP encode transform: single pass over the block that converts
    // sortable-longs to integer mantissas in-place and collects exceptions.
    static int alpTransformBlock(final long[] values, int valueCount, int e, int f, final int[] excPositions, final int[] excValues) {
        final float mulFactor = POWERS_OF_TEN_FLOAT[e] * NEG_POWERS_OF_TEN_FLOAT[f];
        final float decodeMul = POWERS_OF_TEN_FLOAT[f] * NEG_POWERS_OF_TEN_FLOAT[e];

        int excCount = 0;
        for (int i = 0; i < valueCount; i++) {
            final float original = NumericUtils.sortableIntToFloat((int) values[i]);
            final int encoded = alpRound(original * mulFactor);
            final float decoded = encoded * decodeMul;

            if (Float.floatToRawIntBits(original) == Float.floatToRawIntBits(decoded)) {
                values[i] = encoded;
            } else {
                excPositions[excCount] = i;
                excValues[excCount] = (int) values[i];
                excCount++;
                // NOTE: replace the exception slot with a nearby non-exception value to keep
                // bit-packed mantissa width low. Previous value is chosen for local
                // smoothness and favor delta-encoding; first exception uses 0 since there is no predecessor.
                values[i] = (i > 0) ? values[i - 1] : 0;
            }
        }
        return excCount;
    }

    // NOTE: Returns packed (e << 16) | f for a single float value.
    static int bestEFForSingleFloat(float value, int maxExponent) {
        if (Float.isNaN(value) || Float.isInfinite(value) || value == 0.0f) {
            return 0;
        }
        final int p = estimatePrecisionFloat(value, maxExponent);
        for (int e = p; e <= maxExponent; e++) {
            for (int f = 0; f <= e; f++) {
                final float mulFactor = POWERS_OF_TEN_FLOAT[e] * NEG_POWERS_OF_TEN_FLOAT[f];
                final int encoded = alpRound(value * mulFactor);
                final float decoded = encoded * POWERS_OF_TEN_FLOAT[f] * NEG_POWERS_OF_TEN_FLOAT[e];
                if (Float.floatToRawIntBits(value) == Float.floatToRawIntBits(decoded)) {
                    return (e << 16) | f;
                }
            }
        }
        return 0;
    }

    // NOTE: Top-K (e,f) pre-selection for floats. Samples the block, evaluates
    // top-K candidates on the full block. When sampling yields no candidates,
    // falls back to precision-estimation-based candidate generation.
    static int findBestEFFloatTopK(
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
            final float value = NumericUtils.sortableIntToFloat((int) values[i]);
            final int packed = bestEFForSingleFloat(value, maxExponent);
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
                final int p = estimatePrecisionFloat(NumericUtils.sortableIntToFloat((int) values[i]), maxExponent);
                minP = Math.min(minP, p);
                maxP = Math.max(maxP, p);
            }
            for (int e = minP; e <= maxExponent; e++) {
                for (int f = 0; f <= e; f++) {
                    poolUsed = insertIntoPool(candE, candF, candCount, poolUsed, e, f);
                }
            }
        }

        return evaluateTopKFloat(values, valueCount, efOut, candE, candF, candCount, poolUsed);
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
        // NOTE: Pool full: evict the least-frequent entry.
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

    private static int evaluateTopKFloat(
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
            candCount[maxIdx] = -candCount[maxIdx];

            final int exceptions = countExceptionsFloat(values, valueCount, e, f);
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
            final int exceptions = countExceptionsFloat(values, valueCount, 0, 0);
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
