/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

// NOTE: NumericUtils.sortableLongToDouble is inlined as the two-op
// (bits ^ ((bits >> 63) & 0x7FFF...)) + longBitsToDouble idiom inside the
// per-value hot loops below. It avoids the cross-module method call so the JIT
// can keep the entire per-value sequence in registers and auto-vectorise.

/**
 * Algorithmic primitives for the ALP (Adaptive Lossless floating-Point) double encoding.
 *
 * <p>The transform encodes each double {@code v} as the integer mantissa
 * {@code round(v * 10^e * 10^-f)} for a per-block exponent pair {@code (e, f)} chosen so
 * that {@code (mantissa * 10^f * 10^-e)} round-trips back to {@code v} bit-for-bit. Values
 * for which the round-trip fails are recorded as exceptions and reinjected on decode.
 *
 * <p>The stage-level policy that orchestrates these primitives (per-block cache, skip
 * heuristics, metadata layout) lives in {@link AlpDoubleTransformStage}. This class
 * exposes only the building blocks: branchless rounding, exception counting,
 * single-pass transform, and the top-K {@code (e, f)} search.
 *
 * <h2>Selection cost</h2>
 * <p>The ALP paper (Afroozeh et al., SIGMOD 2023, section 3.2) identifies the per-block
 * {@code (e, f)} search as the dominant cost of ALP encoding. The naive lower bound is
 * {@code O(N * E * F)} per block, where {@code N} is the block size, {@code E} is the
 * exponent range (up to {@link #MAX_EXPONENT}, so 19), and {@code F} is the factor range
 * (up to {@code E}). For {@code N = 1024} and a full sweep this is roughly
 * {@code 1024 * 19 * 10 ~= 195k round-trip checks per block}, each one a multiply, a
 * fast round, a multiply back, and a bit-pattern compare. Because a segment can contain
 * thousands of blocks, this search is run thousands of times per field at flush time,
 * which is why the paper invests heavily in cutting it.
 *
 * <h2>Selection strategy</h2>
 * <ol>
 *   <li><b>Top-K pre-selection from a fixed-stride sample.</b> {@link
 *     #findBestEFDoubleTopK} samples at most {@link #PRE_SELECT_SAMPLE} values at a fixed
 *     stride (paper section 3.2), runs the full per-value search
 *     ({@link #bestEFForSingleDouble}) on each sample, and accumulates the resulting
 *     pairs in a {@link #CAND_POOL_SIZE}-slot pool keyed by {@code (e, f)} frequency.
 *     The {@link #TOP_K} most frequent candidates are evaluated against the full block.
 *     This replaces the {@code O(N * E * F)} sweep with one bounded per-sample search
 *     plus {@code K} block-wide evaluations ({@code O(K * N) <= O(5 * N)}).
 *   <li><b>Identity fast path.</b> The pool eviction policy is LFU rather than LRU so
 *     candidates that never recur drop out, but identity {@code (0, 0)} is always
 *     evaluated even when no sample produces it. Integer-valued doubles and any block
 *     where ALP does not help short-circuit through identity.
 *   <li><b>Precision-bounded fallback.</b> If no sample yields a candidate (e.g. the
 *     block is all zeros or all special values), a second sample of size
 *     {@link #SAMPLE_SIZE} estimates the precision range {@code [minP, maxExponent]}
 *     and only that subset of {@code (e, f)} pairs is evaluated, instead of the full
 *     {@code [0, MAX_EXPONENT]} square.
 * </ol>
 *
 * <h2>Block-size effect</h2>
 * <p>The stride is set to {@code max(1, valueCount / PRE_SELECT_SAMPLE)}, so the sample
 * size is constant in {@code N} and the per-candidate evaluation grows linearly.
 * Larger blocks therefore amortize the search over more values. Smaller blocks pay the
 * search more often per field, which is the main reason TSDB favours larger block sizes
 * for double metrics.
 *
 * <h2>Allocation</h2>
 * <p>All scratch state (candidate pool, exception arrays, output slots for the best
 * {@code (e, f)}) is owned by the caller so this class never allocates on the encode
 * hot path.
 */
final class AlpDoubleUtils {

    private AlpDoubleUtils() {}

    /** Largest decimal exponent considered by the {@code (e, f)} search. */
    static final int MAX_EXPONENT = 18;

    /** Maximum exception count, in percent of the block, tolerated for a cached pair. */
    static final int CACHE_VALIDATION_THRESHOLD = 5;

    /** Approximate per-exception cost in bytes: VInt position plus raw long value. */
    static final int DOUBLE_EXCEPTION_COST = 10;

    /**
     * Spread threshold (in sortable-long deltas) above which the integer baseline loses
     * its near-optimal compression. Below the threshold {@code delta > offset > bitPack}
     * fits residuals into at most {@code ceil(log2(17))=5} bits per value and ALP cannot
     * improve on that floor.
     */
    static final long DELTA_SPREAD_THRESHOLD = 16L;

    /**
     * Size of the direct-indexed candidate pool. {@code (e, f)} is encoded as
     * {@code e * CAND_STRIDE + f} so the lookup is one array indexing, no
     * hashing, no linear scan. Covers every legal pair with {@code e in [0, MAX_EXPONENT]}
     * and {@code f in [0, e]} (and an unused upper triangle, which the search never visits).
     */
    static final int CAND_STRIDE = MAX_EXPONENT + 1;
    static final int CAND_POOL_SIZE = CAND_STRIDE * CAND_STRIDE;

    static int candidateKey(int e, int f) {
        return e * CAND_STRIDE + f;
    }

    /** Number of candidates evaluated against the full block during top-K selection. */
    static final int TOP_K = 5;

    /** Stride for the pre-selection sample passed through {@link #bestEFForSingleDouble}. */
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
     * Branchless rounding using the {@code (x + bias) - bias} trick (paper section 3.1).
     *
     * <p>Adding {@code bias = 2^52 + 2^51} pushes {@code x} into the magnitude where
     * every representable double is an integer; IEEE 754 round-to-nearest-even then
     * rounds {@code x} as part of fitting the sum into the 52-bit mantissa. Subtracting
     * the bias recovers the rounded integer as an exact double, which casts cheaply to
     * {@code long}. This replaces the sign branch and NaN/Infinity special cases of
     * {@link Math#round} with two adds and a narrowing, the form modern compilers
     * vectorize.
     *
     * <p>Caveats: rounding is ties-to-even rather than ties-up, which ALP tolerates
     * because encode and decode use the same rounding; the caller must guarantee
     * {@code |x| < FAST_ROUND_MAX_DOUBLE}, and NaN/Infinity are gated out upstream.
     * {@link #alpRound} layers the magnitude guard on top of this primitive.
     */
    static long fastRound(double x) {
        if (x >= 0) {
            return (long) (x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE;
        }
        return -((long) (-x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE);
    }

    /**
     * Guarded round used by every ALP hot loop. Uses {@link #fastRound} when the
     * magnitude is safe and falls back to {@link Math#round} otherwise. NaN and infinity
     * are not handled here; callers gate them out before reaching this method.
     */
    static long alpRound(double x) {
        if (x > -FAST_ROUND_MAX_DOUBLE && x < FAST_ROUND_MAX_DOUBLE) {
            return fastRound(x);
        }
        return Math.round(x);
    }

    /**
     * Returns the smallest exponent {@code p in [0, maxExponent]} for which the
     * fractional part of {@code value} scaled by {@code 10^p} rounds to an integer
     * within {@link #PRECISION_TOLERANCE}. Returns {@code 0} for non-finite values and
     * exact integers; returns {@code maxExponent} when no smaller exponent fits.
     */
    static int estimatePrecision(double value, int maxExponent) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final double fractional = Math.abs(value) - Math.floor(Math.abs(value));
        if (fractional == 0.0) {
            return 0;
        }
        for (int p = 1; p <= maxExponent; p++) {
            final double scaled = fractional * POWERS_OF_TEN[p];
            if (Math.abs(scaled - alpRound(scaled)) < PRECISION_TOLERANCE) {
                return p;
            }
        }
        return maxExponent;
    }

    /**
     * Counts how many values in the block fail to round-trip through ALP with the given
     * {@code (e, f)}. Used both to evaluate candidates during selection and to validate
     * the cached pair from the previous block.
     */
    static int countExceptions(final long[] values, int valueCount, int e, int f) {
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];
        int exceptions = 0;
        for (int i = 0; i < valueCount; i++) {
            final long sortable = values[i];
            final long originalBits = sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;
            if (originalBits != Double.doubleToRawLongBits(decoded)) {
                exceptions++;
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
            final long originalBits = sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL);
            final double original = Double.longBitsToDouble(originalBits);
            final long mantissa = alpRound(original * mulFactor);
            final long origMag = sortable ^ (sortable >> 63);
            final long mantMag = mantissa ^ (mantissa >> 63);
            maxOriginalBits = Math.max(maxOriginalBits, Long.SIZE - Long.numberOfLeadingZeros(origMag));
            maxMantissaBits = Math.max(maxMantissaBits, Long.SIZE - Long.numberOfLeadingZeros(mantMag));
        }
        return Math.max(0, maxOriginalBits - maxMantissaBits);
    }

    /**
     * Maximum tolerated exception fraction, in percent, for the given per-value bit
     * saving and per-exception byte cost. The {@code 2x} safety margin biases the
     * decision against marginal wins where metadata overhead can erode the saving.
     */
    static int maxExceptionPercent(int bitsSaved, int exceptionCost) {
        if (bitsSaved <= 0) {
            return 0;
        }
        return (bitsSaved * 100) / (8 * exceptionCost * 2);
    }

    /**
     * Single-pass transform: replaces each successfully encoded value with its integer
     * mantissa and records exceptions into {@code excPositions} and {@code excValues}.
     * Exception slots are filled with the previous value (or zero at position zero) to
     * keep the bit-width of the mantissa stream low.
     *
     * @return the number of exceptions collected
     */
    static int alpTransformBlock(final long[] values, int valueCount, int e, int f, final int[] excPositions, final long[] excValues) {
        assert valueCount <= excPositions.length : "valueCount must not exceed exception scratch length";
        final double mulFactor = POWERS_OF_TEN[e] * NEG_POWERS_OF_TEN[f];
        final double decodeMul = POWERS_OF_TEN[f] * NEG_POWERS_OF_TEN[e];

        int excCount = 0;
        for (int i = 0; i < valueCount; i++) {
            final long sortable = values[i];
            final long originalBits = sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL);
            final double original = Double.longBitsToDouble(originalBits);
            final long encoded = alpRound(original * mulFactor);
            final double decoded = encoded * decodeMul;

            if (originalBits == Double.doubleToRawLongBits(decoded)) {
                values[i] = encoded;
            } else {
                excPositions[excCount] = i;
                excValues[excCount] = sortable;
                excCount++;
                values[i] = (i > 0) ? values[i - 1] : 0;
            }
        }
        return excCount;
    }

    /**
     * Returns the smallest {@code (e, f)} that lets {@code value} round-trip through
     * ALP, packed as {@code (e << 16) | f}. Returns {@code 0} (identity) for non-finite
     * values, zero, or values that find no match within {@link #MAX_EXPONENT}.
     */
    static int bestEFForSingleDouble(double value, int maxExponent) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value == 0.0) {
            return 0;
        }
        final int p = estimatePrecision(value, maxExponent);
        final long valueBits = Double.doubleToRawLongBits(value);
        // NOTE: f=0 is the common winner for decimal-friendly values; specialize so the
        // hot path skips the redundant multiplications by POWERS_OF_TEN[0]=1.0.
        for (int e = p; e <= maxExponent; e++) {
            final long encoded = alpRound(value * POWERS_OF_TEN[e]);
            final double decoded = encoded * NEG_POWERS_OF_TEN[e];
            if (valueBits == Double.doubleToRawLongBits(decoded)) {
                return e << 16;
            }
        }
        for (int e = Math.max(p, 1); e <= maxExponent; e++) {
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
     * stride, builds a candidate pool keyed by per-sample {@link #bestEFForSingleDouble},
     * and evaluates the most frequent {@link #TOP_K} pairs against the full block. Falls
     * back to a precision-bounded enumeration when no sample yields a candidate.
     *
     * <p>See the class-level javadoc for the rationale: this method replaces a full
     * {@code O(N * E * F)} block sweep with one bounded per-sample search plus
     * {@link #TOP_K} {@code O(N)} candidate evaluations. Together with the cached
     * {@code (e, f)} on the caller side it keeps the steady-state cost at one linear
     * pass per block.
     *
     * <p>{@code candCounts} is a direct-indexed pool sized to {@link #CAND_POOL_SIZE}
     * where {@code candCounts[candidateKey(e, f)]} holds the sample frequency of the
     * pair {@code (e, f)}. The lookup is one array index per sample; there is no
     * linear scan or eviction logic. The mark-as-evaluated trick uses sign negation
     * during top-K selection and is cleared by the {@code Arrays.fill} at the start
     * of the next call, so no un-negate pass is needed.
     *
     * @return the exception count for the chosen {@code (e, f)}, written to
     *         {@code efOut[0]} and {@code efOut[1]}
     */
    static int findBestEFDoubleTopK(final long[] values, int valueCount, int maxExponent, final int[] efOut, final int[] candCounts) {
        java.util.Arrays.fill(candCounts, 0);

        boolean anyCandidate = false;
        final int step = Math.max(1, valueCount / PRE_SELECT_SAMPLE);
        for (int i = 0; i < valueCount; i += step) {
            final long sortable = values[i];
            final double value = Double.longBitsToDouble(sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL));
            final int packed = bestEFForSingleDouble(value, maxExponent);
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
                final long sortable = values[i];
                final double value = Double.longBitsToDouble(sortable ^ ((sortable >> 63) & 0x7FFFFFFFFFFFFFFFL));
                final int p = estimatePrecision(value, maxExponent);
                minP = Math.min(minP, p);
                maxP = Math.max(maxP, p);
            }
            for (int e = minP; e <= maxExponent; e++) {
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

            final int e = maxIdx / CAND_STRIDE;
            final int f = maxIdx % CAND_STRIDE;
            // Negate to mark this candidate as evaluated. The next call resets the table
            // via Arrays.fill so we do not need to restore the sign.
            candCounts[maxIdx] = -candCounts[maxIdx];

            final int exceptions = countExceptions(values, valueCount, e, f);
            if (exceptions < bestExceptions) {
                bestExceptions = exceptions;
                bestE = e;
                bestF = f;
                if (exceptions == 0) {
                    break;
                }
            }
        }

        // Always consider identity (0, 0) if it was not already evaluated in the top-K loop.
        if (candCounts[candidateKey(0, 0)] >= 0) {
            final int exceptions = countExceptions(values, valueCount, 0, 0);
            if (exceptions < bestExceptions) {
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
