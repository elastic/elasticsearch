/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.util.Arrays;

/**
 * Algorithmic primitives for ALP (Adaptive Lossless floating-Point) double encoding. The caller
 * owns all scratch state so this class never allocates on the hot path.
 */
final class AlpDoubleUtils {

    private AlpDoubleUtils() {}

    static final int MAX_EXPONENT = 18;
    static final int CACHE_VALIDATION_THRESHOLD = 5;
    static final int EXCEPTION_POSITION_VINT_BYTES = 2;
    static final int DOUBLE_EXCEPTION_COST = EXCEPTION_POSITION_VINT_BYTES + Long.BYTES;

    private static final int CONSECUTIVE_WORSE_EXIT = 2;

    /**
     * Spread threshold (in sortable-long deltas) above which it is worth running the ALP search.
     * Below the threshold {@code delta > offset > bitPack} fits residuals into at most 5 bits per
     * value, which is the break-even point against ALP's per-block overhead.
     */
    static final long DELTA_SPREAD_THRESHOLD = 16L;

    static final int CAND_POOL_SIZE = (MAX_EXPONENT + 1) * (MAX_EXPONENT + 2) / 2;

    private static int candidateKey(int e, int f) {
        return e * (e + 1) / 2 + f;
    }

    private static final int[] IDX_TO_E = new int[CAND_POOL_SIZE];
    private static final int[] IDX_TO_F = new int[CAND_POOL_SIZE];

    static {
        int idx = 0;
        for (int e = 0; e <= MAX_EXPONENT; e++) {
            for (int f = 0; f <= e; f++) {
                IDX_TO_E[idx] = e;
                IDX_TO_F[idx] = f;
                idx++;
            }
        }
    }

    static final int TOP_K = 5;
    static final int PRE_SELECT_SAMPLE = 64;
    static final int SAMPLE_SIZE = 16;
    static final double PRECISION_TOLERANCE = 1e-9;
    static final double ROUNDING_BIAS_DOUBLE = 6755399441055744.0;
    static final double FAST_ROUND_MAX_DOUBLE = (double) (1L << 52);

    static final double[] POWERS_OF_TEN = new double[MAX_EXPONENT + 1];
    static final double[] NEG_POWERS_OF_TEN = new double[MAX_EXPONENT + 1];

    static {
        for (int i = 0; i <= MAX_EXPONENT; i++) {
            POWERS_OF_TEN[i] = Math.pow(10, i);
            NEG_POWERS_OF_TEN[i] = Math.pow(10, -i);
        }
    }

    /**
     * Returns {@code true} when consecutive sortable-long deltas have a non-zero base stride and a
     * spread no larger than {@link #DELTA_SPREAD_THRESHOLD}. Constant blocks (stride 0) return
     * {@code false} to keep ALP in play.
     */
    static boolean hasNearConstantStride(final long[] values, int valueCount) {
        if (valueCount < 3) {
            return false;
        }
        final long firstStride = values[1] - values[0];
        if (firstStride == 0) {
            return false;
        }
        long min = firstStride;
        long max = firstStride;
        for (int i = 2; i < valueCount; i++) {
            final long stride = values[i] - values[i - 1];
            min = Math.min(min, stride);
            max = Math.max(max, stride);
        }
        final long spread = max - min;
        if (spread < 0) {
            return false;
        }
        return spread <= DELTA_SPREAD_THRESHOLD;
    }

    // Branchless rounding via the (x + bias) - bias trick. Caller must guarantee |x| < FAST_ROUND_MAX_DOUBLE.
    static long fastRound(double x) {
        return (long) (x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE;
    }

    // Inlined NumericUtils.sortableLongToDoubleBits.
    static long sortableToDoubleBits(long sortable) {
        return sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL);
    }

    static long alpRound(double x) {
        if (x > -FAST_ROUND_MAX_DOUBLE && x < FAST_ROUND_MAX_DOUBLE) {
            return fastRound(x);
        }
        return Math.round(x);
    }

    static int estimatePrecision(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final double fractional = Math.abs(value) - Math.floor(Math.abs(value));
        if (fractional == 0.0) {
            return 0;
        }
        for (int p = 1; p <= MAX_EXPONENT; p++) {
            final double scaled = fractional * POWERS_OF_TEN[p];
            if (Math.abs(scaled - alpRound(scaled)) < PRECISION_TOLERANCE) {
                return p;
            }
        }
        return MAX_EXPONENT;
    }

    static int countExceptions(final long[] values, int valueCount, int e, int f, int maxAllowed) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
        int exceptions = 0;
        for (int i = 0; i < valueCount; i++) {
            final long originalBits = sortableToDoubleBits(values[i]);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            if (originalBits != Double.doubleToRawLongBits(decoded)) {
                exceptions++;
                if (exceptions > maxAllowed) {
                    return exceptions;
                }
            }
        }
        return exceptions;
    }

    static int computeBitSavings(final long[] values, int valueCount, int e, int f) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        int maxOriginalBits = 0;
        int maxMantissaBits = 0;
        for (int i = 0; i < valueCount; i++) {
            final long sortable = values[i];
            final double original = Double.longBitsToDouble(sortableToDoubleBits(sortable));
            final long mantissa = alpRound(original * mulFactor);
            final long origMag = sortable ^ (sortable >> 63);
            final long mantMag = mantissa ^ (mantissa >> 63);
            maxOriginalBits = Math.max(maxOriginalBits, Long.SIZE - Long.numberOfLeadingZeros(origMag));
            maxMantissaBits = Math.max(maxMantissaBits, Long.SIZE - Long.numberOfLeadingZeros(mantMag));
        }
        return Math.max(0, maxOriginalBits - maxMantissaBits);
    }

    static int bitsForRange(long range) {
        if (range <= 0) {
            return 1;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(range);
    }

    static int vintBitCount(int value) {
        if (value < 1 << 7) {
            return Byte.SIZE;
        }
        if (value < 1 << 14) {
            return 2 * Byte.SIZE;
        }
        if (value < 1 << 21) {
            return 3 * Byte.SIZE;
        }
        if (value < 1 << 28) {
            return 4 * Byte.SIZE;
        }
        return 5 * Byte.SIZE;
    }

    /**
     * Per-block bit-cost estimate for {@code (e, f)}: range-coded mantissa width times the
     * non-exception count plus actual per-exception storage cost. Returned packed: cost-bits
     * in the upper 48 bits, exception count in the lower 16 bits.
     */
    static long estimateBlockBits(final long[] values, int valueCount, int e, int f) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
        long minMantissa = Long.MAX_VALUE;
        long maxMantissa = Long.MIN_VALUE;
        int excCount = 0;
        long excPositionBits = 0;
        for (int i = 0; i < valueCount; i++) {
            final long originalBits = sortableToDoubleBits(values[i]);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            if (originalBits == Double.doubleToRawLongBits(decoded)) {
                if (encoded < minMantissa) {
                    minMantissa = encoded;
                }
                if (encoded > maxMantissa) {
                    maxMantissa = encoded;
                }
            } else {
                excCount++;
                excPositionBits += vintBitCount(i);
            }
        }
        final int nonExc = valueCount - excCount;
        final int mantissaBits = (nonExc > 0) ? bitsForRange(maxMantissa - minMantissa) : 0;
        final long valueExcBits = (long) Long.BYTES * Byte.SIZE * excCount;
        final long costBits = (long) mantissaBits * nonExc + valueExcBits + excPositionBits;
        return (costBits << 16) | (excCount & 0xFFFFL);
    }

    static int maxExceptions(int bitsSaved, int valueCount, int exceptionCost) {
        if (bitsSaved <= 0) {
            return 0;
        }
        final long savedBits = (long) bitsSaved * valueCount;
        final long perExceptionBits = (long) exceptionCost * 8 * 2;
        return (int) (savedBits / perExceptionBits);
    }

    /**
     * Single-pass transform: replaces each value with its integer mantissa and records exceptions.
     * When {@code nearConstStrideOut} is non-null and {@code valueCount >= 3}, also writes the
     * {@link #hasNearConstantStride} result into slot 0 as a side-channel for the cache fast path.
     *
     * @return the number of exceptions collected
     */
    static int alpTransformBlock(
        final long[] values,
        int valueCount,
        int e,
        int f,
        final int[] excPositions,
        final long[] excValues,
        final boolean[] nearConstStrideOut
    ) {
        assert valueCount <= excPositions.length : "valueCount must not exceed exception scratch length";
        if (valueCount == 0) {
            return 0;
        }
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
        final boolean observe = nearConstStrideOut != null && valueCount >= 3;

        // Always write the rounded mantissa even for exceptions: the decoder patches them back from
        // metadata, and using the mantissa preserves the block's natural shape for downstream stages.
        final long sortable0 = values[0];
        int excCount = 0;
        {
            final long originalBits = sortableToDoubleBits(sortable0);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            values[0] = encoded;
            if (originalBits != Double.doubleToRawLongBits(decoded)) {
                excPositions[excCount] = 0;
                excValues[excCount] = sortable0;
                excCount++;
            }
        }

        long prevSortable = sortable0;
        long firstStride = 0;
        long minStride = 0;
        long maxStride = 0;
        if (observe) {
            firstStride = values[1] - sortable0;
            minStride = firstStride;
            maxStride = firstStride;
        }

        for (int i = 1; i < valueCount; i++) {
            final long sortable = values[i];
            if (observe) {
                final long stride = sortable - prevSortable;
                if (stride < minStride) {
                    minStride = stride;
                }
                if (stride > maxStride) {
                    maxStride = stride;
                }
                prevSortable = sortable;
            }

            final long originalBits = sortableToDoubleBits(sortable);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            values[i] = encoded;
            if (originalBits != Double.doubleToRawLongBits(decoded)) {
                excPositions[excCount] = i;
                excValues[excCount] = sortable;
                excCount++;
            }
        }

        if (observe) {
            final long spread = maxStride - minStride;
            nearConstStrideOut[0] = firstStride != 0 && spread >= 0 && spread <= DELTA_SPREAD_THRESHOLD;
        }
        return excCount;
    }

    static int bestEFForValue(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final int p = estimatePrecision(value);
        final long valueBits = Double.doubleToRawLongBits(value);
        for (int e = p; e <= MAX_EXPONENT; e++) {
            final long encoded = alpRound(value * POWERS_OF_TEN[e]);
            final double decoded = encoded * NEG_POWERS_OF_TEN[e];
            if (valueBits == Double.doubleToRawLongBits(decoded)) {
                return e << 16;
            }
        }
        for (int e = Math.max(p, 1); e <= MAX_EXPONENT; e++) {
            for (int f = 1; f <= e; f++) {
                final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
                final long encoded = alpRound(value * mulFactor);
                final double decoded = encoded * POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
                if (valueBits == Double.doubleToRawLongBits(decoded)) {
                    return (e << 16) | f;
                }
            }
        }
        return 0;
    }

    /**
     * Top-K {@code (e, f)} selection. Samples the block at a fixed stride, tallies per-pair
     * frequencies in {@code candCounts}, and evaluates the {@link #TOP_K} most frequent pairs
     * against the full block. Falls back to a precision-bounded enumeration when no sample
     * yields a candidate.
     */
    static int findBestEFForBlock(final long[] values, int valueCount, final int[] efOut, final int[] candCounts) {
        if (valueCount == 0) {
            efOut[0] = -1;
            efOut[1] = -1;
            return 0;
        }

        Arrays.fill(candCounts, 0);

        boolean anyCandidate = false;
        final int step = Math.max(1, valueCount / PRE_SELECT_SAMPLE);
        for (int i = 0; i < valueCount; i += step) {
            final double value = Double.longBitsToDouble(sortableToDoubleBits(values[i]));
            final int packed = bestEFForValue(value);
            if (packed != 0) {
                candCounts[candidateKey(packed >>> 16, packed & 0xFFFF)]++;
                anyCandidate = true;
            }
        }

        if (anyCandidate == false) {
            int minP = MAX_EXPONENT;
            final int precStep = Math.max(1, valueCount / SAMPLE_SIZE);
            for (int i = 0; i < valueCount; i += precStep) {
                final double value = Double.longBitsToDouble(sortableToDoubleBits(values[i]));
                minP = Math.min(minP, estimatePrecision(value));
            }
            for (int e = minP; e <= MAX_EXPONENT; e++) {
                for (int f = 0; f <= e; f++) {
                    candCounts[candidateKey(e, f)] = 1;
                }
            }
        }

        return evaluateTopK(values, valueCount, efOut, candCounts);
    }

    private static int evaluateTopK(final long[] values, int valueCount, final int[] efOut, final int[] candCounts) {
        int bestE = 0;
        int bestF = 0;
        int bestExceptions = valueCount;
        long bestCost = Long.MAX_VALUE;
        int consecutiveWorse = 0;

        for (int k = 0; k < TOP_K; k++) {
            int maxIdx = -1;
            int maxCount = 0;
            for (int idx = 0; idx < CAND_POOL_SIZE; idx++) {
                if (candCounts[idx] > maxCount) {
                    maxCount = candCounts[idx];
                    maxIdx = idx;
                }
            }
            if (maxIdx < 0) {
                break;
            }

            final int e = IDX_TO_E[maxIdx];
            final int f = IDX_TO_F[maxIdx];
            // Negate to mark as evaluated; reset happens via Arrays.fill on the next call.
            candCounts[maxIdx] = -candCounts[maxIdx];

            final long packed = estimateBlockBits(values, valueCount, e, f);
            final long cost = packed >>> 16;
            final int exceptions = (int) (packed & 0xFFFFL);

            if (cost < bestCost) {
                bestCost = cost;
                bestExceptions = exceptions;
                bestE = e;
                bestF = f;
                consecutiveWorse = 0;
            } else {
                consecutiveWorse++;
                if (consecutiveWorse >= CONSECUTIVE_WORSE_EXIT) {
                    break;
                }
            }
        }

        if (candCounts[candidateKey(0, 0)] >= 0) {
            final long packed = estimateBlockBits(values, valueCount, 0, 0);
            final long cost = packed >>> 16;
            final int exceptions = (int) (packed & 0xFFFFL);
            if (cost < bestCost) {
                bestExceptions = exceptions;
                bestE = 0;
                bestF = 0;
            }
        }

        efOut[0] = bestE;
        efOut[1] = bestF;
        return bestExceptions;
    }
}
