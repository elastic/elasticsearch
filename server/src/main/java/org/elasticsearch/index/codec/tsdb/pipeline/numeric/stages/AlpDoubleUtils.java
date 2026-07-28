/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

/**
 * Algorithmic primitives for the ALP (Adaptive Lossless floating-Point) double encoding.
 *
 * <p>The transform encodes each double {@code v} as the integer mantissa
 * {@code round(v * 10^e * 10^-f)} for a per-block exponent pair {@code (e, f)} chosen so
 * that {@code (mantissa * 10^f * 10^-e)} round-trips back to {@code v} bit-for-bit. Values
 * for which the round-trip fails are recorded as exceptions and reinjected on decode.
 *
 * <p>The stage-level policy (per-block cache, skip heuristics, metadata layout) lives in
 * {@link AlpDoubleTransformStage}; this class exposes only the building blocks. The
 * search strategy is a fixed-stride sample feeding a per-pair candidate pool over the
 * {@code f <= e} triangular space, then the top-K candidates are evaluated against the
 * full block using a bit-cost objective (mantissa range bits per non-exception value
 * plus the actual VInt-position-aware cost per exception). Replaces the naive
 * {@code O(N * E * F)} sweep with {@code O(K * N)} steady-state work.
 *
 * <p>All scratch state (candidate pool, exception arrays, output slots) is owned by the
 * caller, so this class never allocates on the hot path.
 */
final class AlpDoubleUtils {

    private AlpDoubleUtils() {}

    /** Largest decimal exponent considered by the {@code (e, f)} search. */
    static final int MAX_EXPONENT = 18;

    /** Maximum exception count, in percent of the block, tolerated for a cached pair. */
    static final int CACHE_VALIDATION_THRESHOLD = 5;

    /** Typical per-exception position VInt width (1-5 bytes possible; 2 is typical for blockSize up to 16K). */
    static final int EXCEPTION_POSITION_VINT_BYTES = 2;

    /**
     * Approximate per-exception metadata cost in bytes: the position VInt
     * ({@link #EXCEPTION_POSITION_VINT_BYTES}) plus the original sortable-long stored
     * verbatim ({@link Long#BYTES}).
     */
    static final int DOUBLE_EXCEPTION_COST = EXCEPTION_POSITION_VINT_BYTES + Long.BYTES;

    /** Number of consecutive non-improving candidates after which the top-K loop bails. */
    private static final int CONSECUTIVE_WORSE_EXIT = 2;

    /**
     * Spread threshold (in sortable-long deltas) above which it is worth running the
     * ALP search. Below the threshold {@code delta > offset > bitPack} fits residuals
     * into at most {@code ceil(log2(17)) = 5} bits per value, which is the break-even
     * point against ALP: any further per-value width reduction ALP might achieve is
     * dominated by its per-block metadata cost (one byte each for {@code e} and
     * {@code f}, a VInt for the exception count, plus {@link #DOUBLE_EXCEPTION_COST}
     * bytes for every exception slot). Above the threshold the integer baseline jumps
     * to 6 or more bits per value, which gives ALP enough per-value headroom to recover
     * its overhead net.
     */
    static final long DELTA_SPREAD_THRESHOLD = 16L;

    /**
     * Triangular candidate pool size for the {@code f <= e} search space:
     * {@code (MAX_EXPONENT + 1) * (MAX_EXPONENT + 2) / 2}. Layout is internal to this class.
     */
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

    /** Number of candidates evaluated against the full block during top-K selection. */
    static final int TOP_K = 5;

    /** Stride for the pre-selection sample passed through {@link #bestEFForValue}. */
    static final int PRE_SELECT_SAMPLE = 64;

    /** Sample size for the precision estimate that bounds the fallback search. */
    static final int SAMPLE_SIZE = 16;

    /** Tolerance for the fractional precision detector. */
    static final double PRECISION_TOLERANCE = 1e-9;

    /** Magic constant for the add/subtract rounding trick, {@code 2^52 + 2^51}. */
    static final double ROUNDING_BIAS_DOUBLE = 6755399441055744.0;

    /** Upper bound on {@code |x|} for which the bias trick produces correct rounding. */
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
     * Returns {@code true} when consecutive sortable-long deltas have a non-zero base
     * stride and a spread no larger than {@link #DELTA_SPREAD_THRESHOLD}. This shape
     * characterises doubles that are not literally constant but advance in near-uniform
     * small increments (a slow-drift gauge staying inside a single IEEE 754 exponent),
     * so the downstream integer pipeline {@code delta > offset > gcd > bitPack} reduces
     * the residuals to a tight range that fits in 5 bits per value or fewer, which is at
     * or below ALP's bit-width floor.
     *
     * <p>Constant blocks (stride 0) return {@code false} to keep ALP in play, because
     * ALP wins on them: it picks {@code (e, f)} that collapses the repeated double
     * down to a small integer mantissa, so the downstream bit-pack stores a tiny
     * first value (1-2 bytes) plus three bytes of ALP metadata for roughly 7 bytes
     * total per block. The integer pipeline would have to store the full sortable-long
     * representation of the repeated double (8 bytes for the first value alone) plus
     * its own metadata for roughly 12 bytes.
     *
     * <p>Callers use this as the gate to skip ALP entirely on blocks where the integer
     * pipeline is already near-optimal.
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

    /**
     * Branchless rounding using the {@code (x + bias) - bias} trick (paper section 3.1).
     * Adding {@code bias = 2^52 + 2^51} pushes {@code x} into the magnitude range where
     * every representable double is an integer; IEEE 754 ties-to-even then rounds as part
     * of fitting the sum into the 52-bit mantissa, and subtracting the bias recovers the
     * rounded integer. The same expression works for negative {@code x} because the bias
     * dominates {@code |x|} in magnitude, so the addition still lands inside the
     * integer-only range. Caller must guarantee {@code |x| < FAST_ROUND_MAX_DOUBLE} and
     * that {@code x} is finite; {@link #alpRound} layers the magnitude guard.
     */
    static long fastRound(double x) {
        return (long) (x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE;
    }

    /**
     * Inlined NumericUtils.sortableLongToDoubleBits: flips the lower 63 bits when the
     * sign bit is set so the resulting long preserves the natural order of the source
     * double. Kept on this class so every ALP hot loop calls a single short static
     * method that the JIT inlines unconditionally.
     */
    static long sortableToDoubleBits(long sortable) {
        return sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL);
    }

    /** Guarded round: {@link #fastRound} inside {@link #FAST_ROUND_MAX_DOUBLE}, {@link Math#round} otherwise. */
    static long alpRound(double x) {
        if (x > -FAST_ROUND_MAX_DOUBLE && x < FAST_ROUND_MAX_DOUBLE) {
            return fastRound(x);
        }
        return Math.round(x);
    }

    /**
     * Returns the smallest exponent {@code p in [0, MAX_EXPONENT]} for which the
     * fractional part of {@code value} scaled by {@code 10^p} rounds to an integer
     * within {@link #PRECISION_TOLERANCE}. Returns {@code 0} for non-finite values and
     * exact integers; returns {@link #MAX_EXPONENT} when no smaller exponent fits.
     */
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

    /**
     * Counts how many values in the block fail to round-trip through ALP with the given
     * {@code (e, f)}, bailing as soon as the count exceeds {@code maxAllowed}. The
     * returned value is then exact when it is {@code <= maxAllowed} and otherwise any
     * value greater than {@code maxAllowed}, which is all the caller needs to decide
     * whether to accept the pair.
     */
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

    /**
     * Returns the bit-width reduction ALP achieves on the block, computed as
     * {@code max sign-magnitude bits of values - max sign-magnitude bits of mantissas}.
     * Negative results clamp to zero. Sign-magnitude is used so that a single negative
     * value does not inflate the result to {@code Long.SIZE}.
     */
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

    /**
     * Bit width needed to range-code a non-negative span: {@code ceil(log2(range + 1))},
     * clamped to a minimum of one so a constant block still costs one bit per value.
     */
    static int bitsForRange(long range) {
        if (range <= 0) {
            return 1;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(range);
    }

    /**
     * Number of bits a non-negative VInt occupies on the metadata stream, in
     * {@link Byte#SIZE} multiples. Used by the per-block bit-cost estimate to charge
     * exceptions their actual position-encoding cost instead of a constant.
     */
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
     * non-exception count plus the actual per-exception storage cost (8-byte raw value
     * plus the VInt position cost computed per exception position). Drives candidate
     * selection in {@link #findBestEFForBlock}: among the top-K most frequent pairs from
     * the sample, the one with the smallest cost wins, not the one with the fewest
     * exceptions, so a candidate with a few extra exceptions but much narrower mantissas
     * is correctly preferred when the trade favours it.
     *
     * <p>Returned packed with the exception count to avoid scratch allocation on the
     * hot path: cost-bits in the upper 48 bits, exception count in the lower 16 bits.
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

    /**
     * Maximum tolerated exception count for a block of {@code valueCount} values given the
     * per-value bit-width saving and per-exception byte cost. The threshold is the
     * break-even point where the block-wide saving from ALP's narrower mantissa stream
     * (bitsSaved * valueCount bits) is exceeded by the per-exception metadata cost
     * (exceptionCost bytes each), with a {@code 2x} safety margin so marginal wins do not
     * trigger.
     */
    static int maxExceptions(int bitsSaved, int valueCount, int exceptionCost) {
        if (bitsSaved <= 0) {
            return 0;
        }
        final long savedBits = (long) bitsSaved * valueCount;
        final long perExceptionBits = (long) exceptionCost * 8 * 2;
        return (int) (savedBits / perExceptionBits);
    }

    /**
     * Single-pass transform: replaces each successfully encoded value with its integer
     * mantissa and records exceptions into {@code excPositions} and {@code excValues}.
     * Exception slots are filled with the previous value (or zero at position zero) to
     * keep the bit-width of the mantissa stream low.
     *
     * <p>When {@code als} is non-null and {@code valueCount >= 3}, the
     * same pass also observes the sortable-long stride statistics and writes the
     * {@link #hasNearConstantStride} decision into slot 0. The cache fast path in
     * {@code AlpDoubleTransformStage} uses this signal as one of the ALP-skip
     * conditions: when the flag is set, the caller restores values from snapshot,
     * invalidates the cache, and lets the block flow through to the integer pipeline
     * unchanged. Callers that do not need the observation pass {@code null} so the
     * stride accumulators and the post-loop decision are elided.
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

        // NOTE: always write the rounded mantissa, even for exceptions. The decoder
        // patches exception positions back to the original value from metadata, so
        // the in-array value only matters for the downstream stages. Keeping the
        // rounded mantissa preserves the block's natural shape (monotonicity, tight
        // delta range) instead of injecting copies of the previous value or a zero
        // at position 0.
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

    /**
     * Returns the smallest {@code (e, f)} that lets {@code value} round-trip through
     * ALP, packed as {@code (e << 16) | f}. Returns {@code 0} (identity) for non-finite
     * values, zero, or values that find no match within {@link #MAX_EXPONENT}.
     */
    static int bestEFForValue(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final int p = estimatePrecision(value);
        final long valueBits = Double.doubleToRawLongBits(value);
        // f=0 is the common winner; specialize so the hot loop drops the multiplications by 1.0.
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
     * Top-K {@code (e, f)} selection (paper section 3.2). Samples the block at a fixed
     * stride, tallies per-pair frequencies in {@code candCounts}, and evaluates the
     * {@link #TOP_K} most frequent pairs against the full block. Falls back to a
     * precision-bounded enumeration when no sample yields a candidate.
     *
     * @return the exception count for the chosen {@code (e, f)}, written to
     *         {@code efOut[0]} and {@code efOut[1]}
     */
    static int findBestEFForBlock(final long[] values, int valueCount, final int[] efOut, final int[] candCounts) {
        java.util.Arrays.fill(candCounts, 0);

        boolean anyCandidate = false;
        final int step = Math.max(1, valueCount / PRE_SELECT_SAMPLE);
        for (int i = 0; i < valueCount; i += step) {
            final double value = Double.longBitsToDouble(sortableToDoubleBits(values[i]));
            final int packed = bestEFForValue(value);
            final int e = packed >>> 16;
            final int f = packed & 0xFFFF;
            candCounts[candidateKey(e, f)]++;
            anyCandidate = true;
        }

        if (anyCandidate == false) {
            if (valueCount == 0) {
                efOut[0] = -1;
                efOut[1] = -1;
                return valueCount;
            }
            int minP = MAX_EXPONENT;
            int maxP = 0;
            final int precStep = Math.max(1, valueCount / SAMPLE_SIZE);
            for (int i = 0; i < valueCount; i += precStep) {
                final double value = Double.longBitsToDouble(sortableToDoubleBits(values[i]));
                final int p = estimatePrecision(value);
                minP = Math.min(minP, p);
                maxP = Math.max(maxP, p);
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
            // Negate to mark this candidate as evaluated. The next call resets the table
            // via Arrays.fill so we do not need to restore the sign.
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

        // Always consider identity (0, 0) if it was not already evaluated in the top-K loop.
        if (candCounts[candidateKey(0, 0)] >= 0) {
            final long packed = estimateBlockBits(values, valueCount, 0, 0);
            final long cost = packed >>> 16;
            final int exceptions = (int) (packed & 0xFFFFL);
            if (cost < bestCost) {
                bestCost = cost;
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
