/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.util.LSBRadixSorter;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

/**
 * Spherical scalar quantizer for ASH. Quantizes each dimension of the
 * projected (latent) vector into {@code bitsPerDim} bits using an optimal greedy
 * level selection that maximizes inner product preservation on the unit sphere.
 * <p>
 * For 1-bit, this degenerates to sign quantization (each dimension becomes +/- 0.5).
 * For 2-bit, a specialized sweep selects between magnitudes 0.5 and 1.5.
 * For higher bit widths, a general event-based scan assigns optimal levels.
 */
final class AshSphericalScalarQuantizer {

    private final int bitsPerDim;

    /**
     * Result of quantization for a single vector.
     *
     * @param centeredCode code centered around zero, length nDims
     * @param codeNorm L2 norm of the code vector
     */
    record SingleQuantizeResult(float[] centeredCode, float codeNorm) {}

    /**
     * Result of batch quantization.
     *
     * @param centeredCodes codes centered around zero, row-major matrix (n x nDims)
     * @param codeNorms L2 norm of each code vector, length n
     */
    record QuantizeResult(float[] centeredCodes, float[] codeNorms) {}

    /**
     * Creates a spherical scalar quantizer with the given bit width.
     *
     * @param bitsPerDim number of bits per projected dimension (must be >= 1)
     * @throws IllegalArgumentException if bitsPerDim is less than 1
     */
    AshSphericalScalarQuantizer(int bitsPerDim) {
        if (bitsPerDim < 1) {
            throw new IllegalArgumentException("bitsPerDim must be >= 1");
        }
        this.bitsPerDim = bitsPerDim;
    }

    int bitsPerDimension() {
        return bitsPerDim;
    }

    /**
     * Quantizes {@code n} vectors of {@code nDims} components each.
     *
     * @param x the vectors to quantize, row-major (n x nDims)
     * @param n number of vectors
     * @param nDims components per vector
     */
    QuantizeResult encode(float[] x, int n, int nDims) {
        float[] centeredCodes = new float[n * nDims];
        float[] codeNorms = new float[n];

        for (int i = 0; i < n; i++) {
            int base = i * nDims;
            codeNorms[i] = quantizeExact(x, base, centeredCodes, base, nDims);
        }
        return new QuantizeResult(centeredCodes, codeNorms);
    }

    SingleQuantizeResult encodeOne(float[] xLatent) {
        int nDims = xLatent.length;
        float[] out = new float[nDims];
        float norm = quantizeExact(xLatent, 0, out, 0, nDims);
        return new SingleQuantizeResult(out, norm);
    }

    /**
     * Greedy optimal quantization for a single vector of {@code d} components read from
     * {@code z[zOffset..]}. Returns the norm of the centered code and writes the code to
     * {@code out[outOffset..]}. The offsets let a caller quantize one row of a flat
     * row-major matrix in place.
     */
    private float quantizeExact(float[] z, int zOffset, float[] out, int outOffset, int d) {
        assert assertAllFinite(z);  // all vector values must be finite for the maths to work

        int numAbsLevels = 1 << (bitsPerDim - 1);
        int nSteps = numAbsLevels - 1;

        return switch (nSteps) {
            case 0 -> quantizeExact1Bit(z, zOffset, out, outOffset, d);
            case 1 -> quantizeExact2Bit(z, zOffset, out, outOffset, d);
            default -> quantizeExactGeneral(z, zOffset, out, outOffset, d, numAbsLevels, nSteps);
        };
    }

    private static boolean assertAllFinite(float[] value) {
        for (float v : value) {
            assert Float.isFinite(v) : "value must be finite";
        }
        return true;
    }

    /**
     * 1-bit quantization: each dimension is assigned magnitude 0.5 with the sign of the input.
     * The norm is always sqrt(0.25 * d) = 0.5 * sqrt(d).
     */
    static float quantizeExact1Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        for (int j = 0; j < d; j++) {
            out[outOffset + j] = Math.copySign(0.5f, z[zOffset + j]);
        }
        return (float) Math.sqrt(0.25 * d);
    }

    /**
     * Thread-local reusable radix sorter, to reduce buffer allocations when handling lots of vectors of the same dimension.
     */
    private static final ThreadLocal<LSBRadixSorter> SORTER = ThreadLocal.withInitial(LSBRadixSorter::new);

    /**
     * Specialized fast path for 2-bit quantization (nSteps=1).
     * <p>
     * Each dimension is either at level 0.5 or 1.5. The optimal assignment sorts magnitude values
     * ascending, then iterates in descending order to find the cutoff that maximises
     * cumDot / sqrt(cumNormSq), where upgrading dimension j adds |z_j| to cumDot and 2.0 to cumNormSq.
     * The selected set is recovered via a threshold on |z_j| rather than by tracking indices.
     */
    static float quantizeExact2Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        // Base level: all dims at 0.5 -> cumDot = sum(0.5 * |z_j|), cumNormSq = 0.25 * d
        int[] absZF = new int[d];
        double dot = 0;
        for (int j = 0; j < d; j++) {
            float abs = Math.abs(z[zOffset + j]);
            absZF[j] = Float.floatToRawIntBits(abs);
            dot = Math.fma(0.5, abs, dot);
        }

        // Sorted ascending; the iteration is then done backwards
        // sort as ints, as all values are positive and finite
        // and that eliminates some float-specific handling of NaN/Inf values
        // this also allows us to use radix sort, which is faster and less branch-y than Arrays.sort
        SORTER.get().sort(31, absZF, d);

        double normSq = 0.25 * d;
        double bestValue = dot / Math.sqrt(normSq);

        // iterate dims in |z| descending order
        int bestK = 0; // number of dimensions to upgrade to level 1.5
        for (int k = 0; k < d; k++) {
            int i = d - 1 - k;
            float abs = Float.intBitsToFloat(absZF[i]);
            dot += abs;  // upgrading from 0.5 to 1.5 adds 1.0 * |z_dim|
            normSq += 2.0;   // 1.5^2 - 0.5^2 = 2.0

            // Handle ties: skip evaluation if next dim has the same |z|
            if (i > 0 && absZF[i] == absZF[i - 1]) {
                continue;
            }

            double value = dot / Math.sqrt(normSq);
            if (value > bestValue) {
                bestValue = value;
                bestK = k + 1;
            }
        }

        if (bestK == 0) {
            // every dim is 0.5
            for (int j = 0; j < d; j++) {
                out[outOffset + j] = Math.copySign(0.5f, z[zOffset + j]);
            }
        } else {
            float threshold = Float.intBitsToFloat(absZF[d - bestK]);
            for (int j = 0; j < d; j++) {
                // The tie rule above only ever sets bestK at the end of a run of equal magnitudes, so
                // selecting every dimension at or above the smallest upgraded magnitude picks out exactly
                // bestK of them
                // need to recalculate abs(z[..]) here, as the absZ array order has changed
                float v = z[zOffset + j];
                out[outOffset + j] = Math.copySign(Math.abs(v) >= threshold ? 1.5f : 0.5f, v);
            }
        }

        // vector is now (d - bestK) x 0.5, and bestK x 1.5 (squares = 0.25, 2.25)
        return (float) Math.sqrt(0.25 * (d - bestK) + 2.25 * bestK);
    }

    /**
     * General quantization path for bitsPerDim > 2 (nSteps > 1).
     */
    static float quantizeExactGeneral(float[] z, int zOffset, float[] out, int outOffset, int d, int numAbsLevels, int nSteps) {

        // Extract signs and absolute values
        float[] signs = new float[d];
        float[] absZ = new float[d];
        for (int j = 0; j < d; j++) {
            signs[j] = Math.copySign(1.0f, z[zOffset + j]);
            absZ[j] = Math.abs(z[zOffset + j]);
        }

        // Base level: all at 0.5
        double currentDot = 0;
        for (int j = 0; j < d; j++) {
            currentDot = Math.fma(0.5, absZ[j], currentDot);
        }
        double currentNormSq = 0.25 * d;

        // Find best magnitude for each dimension via greedy event scanning
        int[] bestIdx = new int[d]; // number of level increments beyond base

        if (numAbsLevels > 1) {
            int k = nSteps * d;

            // Build events: for each (step, dim), critical time = step / absZ[dim]
            // Sort events by critical time and greedily pick the best stopping point
            double[] eventTimes = new double[k];
            int[] eventDims = new int[k];
            int[] eventLevels = new int[k];

            int eventCount = 0;
            for (int step = 1; step <= nSteps; step++) {
                for (int j = 0; j < d; j++) {
                    if (absZ[j] > 0) {
                        eventTimes[eventCount] = (double) step / absZ[j];
                        eventDims[eventCount] = j;
                        eventLevels[eventCount] = step;
                        eventCount++;
                    }
                }
            }

            // Sort events by time
            int[] order = new int[eventCount];
            Arrays.setAll(order, IntUnaryOperator.identity());
            IndirectSorter.sortAscendingByDouble(order, eventTimes, eventCount);

            // Sweep through events, tracking cumulative dot product and norm
            double dot = currentDot;
            double normSq = currentNormSq;
            double bestValue = dot / Math.sqrt(normSq);
            int bestStopIdx = -1; // -1 means stop at base

            for (int idx = 0; idx < eventCount; idx++) {
                int oi = order[idx];

                dot += absZ[eventDims[oi]];
                normSq = Math.fma(2f, eventLevels[oi], normSq);

                // Handle ties: skip if next event has same time
                if (idx + 1 < eventCount) {
                    int nextOi = order[idx + 1];
                    if (eventTimes[oi] == eventTimes[nextOi]) {
                        continue;
                    }
                }

                double value = dot / Math.sqrt(normSq);
                if (value > bestValue) {
                    bestValue = value;
                    bestStopIdx = idx;
                }
            }

            // Reconstruct bestIdx from the events up to bestStopIdx
            if (bestStopIdx >= 0) {
                Arrays.fill(bestIdx, 0);
                for (int idx = 0; idx <= bestStopIdx; idx++) {
                    int oi = order[idx];
                    bestIdx[eventDims[oi]]++;
                }
            }
        }

        // Final conversion: centered code = sign * (0.5 + bestIdx)
        for (int j = 0; j < d; j++) {
            float mag = 0.5f + bestIdx[j];
            out[outOffset + j] = signs[j] * mag;
        }
        float norm = ESVectorUtil.dotProduct(out, outOffset, out, outOffset, d);
        return (float) Math.sqrt(norm);
    }
}
