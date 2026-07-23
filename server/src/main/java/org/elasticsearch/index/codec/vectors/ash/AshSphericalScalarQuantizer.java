/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import java.util.Arrays;

/**
 * Multi-bit spherical scalar quantizer for ASH. Quantizes each dimension of the
 * projected (latent) vector into {@code bitsPerDim} bits using an optimal greedy
 * level selection that maximizes inner product preservation on the unit sphere.
 * <p>
 * This is a port of the Python reference implementation's {@code SphericalScalarQuantizer}.
 */
public final class AshSphericalScalarQuantizer implements AshDimQuantizer {

    private final int bitsPerDim;

    /**
     * Creates a spherical scalar quantizer with the given bit width.
     *
     * @param bitsPerDim number of bits per projected dimension (must be >= 2)
     * @throws IllegalArgumentException if bitsPerDim is less than 2
     */
    public AshSphericalScalarQuantizer(int bitsPerDim) {
        if (bitsPerDim < 2) {
            throw new IllegalArgumentException("bitsPerDim must be >= 2 for spherical scalar quantizer; use BinaryQuantizer for 1-bit");
        }
        this.bitsPerDim = bitsPerDim;
    }

    @Override
    public int bitsPerDimension() {
        return bitsPerDim;
    }

    @Override
    public QuantizeResult encode(float[][] x) {
        int n = x.length;
        if (n == 0) {
            return new QuantizeResult(new float[0][0], new float[0]);
        }
        int nDims = x[0].length;
        float[][] centeredCodes = new float[n][nDims];
        float[] codeNorms = new float[n];

        for (int i = 0; i < n; i++) {
            float norm = quantizeExact(x[i], centeredCodes[i], nDims);
            codeNorms[i] = norm;
        }
        return new QuantizeResult(centeredCodes, codeNorms);
    }

    @Override
    public SingleQuantizeResult encodeOne(float[] xLatent) {
        int nDims = xLatent.length;
        float[] out = new float[nDims];
        float norm = quantizeExact(xLatent, out, nDims);
        return new SingleQuantizeResult(out, norm);
    }

    /**
     * Greedy optimal quantization for a single vector.
     * Returns the norm of the centered code and writes the code to {@code out}.
     */
    private float quantizeExact(float[] z, float[] out, int d) {
        int numAbsLevels = 1 << (bitsPerDim - 1);
        int nSteps = numAbsLevels - 1;

        if (nSteps == 1) {
            return quantizeExact2Bit(z, out, d);
        }
        return quantizeExactGeneral(z, out, d, numAbsLevels, nSteps);
    }

    /**
     * Specialized fast path for 2-bit quantization (nSteps=1).
     * <p>
     * Each dimension is either at level 0.5 or 1.5. The optimal assignment sorts dimensions
     * by |z_j| descending and sweeps to find the cutoff that maximizes
     * cumDot / sqrt(cumNormSq), where upgrading dimension j adds |z_j| to cumDot and 2.0 to cumNormSq.
     * <p>
     * This avoids allocating event arrays, level tracking, and tie-breaking logic --
     * producing identical results to the general path but roughly 3x faster.
     */
    private static float quantizeExact2Bit(float[] z, float[] out, int d) {
        // Sort dimension indices by |z| descending. We sort absZ in-place using an index array
        // so we can sweep in order of decreasing magnitude.
        int[] order = new int[d];
        float[] absZ = new float[d];
        for (int j = 0; j < d; j++) {
            order[j] = j;
            absZ[j] = Math.abs(z[j]);
        }
        // Insertion sort descending by absZ -- d is typically 384, well within insertion sort's sweet spot
        for (int i = 1; i < d; i++) {
            int key = order[i];
            float keyVal = absZ[key];
            int j = i - 1;
            while (j >= 0 && absZ[order[j]] < keyVal) {
                order[j + 1] = order[j];
                j--;
            }
            order[j + 1] = key;
        }

        // Base level: all dims at 0.5 -> cumDot = sum(0.5 * |z_j|), cumNormSq = 0.25 * d
        double cumDot = 0;
        for (int j = 0; j < d; j++) {
            cumDot += 0.5 * absZ[j];
        }
        double cumNormSq = 0.25 * d;
        double bestValue = cumDot / Math.sqrt(cumNormSq);
        int bestK = 0; // number of dimensions to upgrade to level 1.5

        // Sweep: upgrade dims in descending |z| order
        double sweepDot = cumDot;
        double sweepNormSq = cumNormSq;
        for (int k = 0; k < d; k++) {
            int dim = order[k];
            sweepDot += absZ[dim];     // upgrading from 0.5 to 1.5 adds 1.0 * |z_dim|
            sweepNormSq += 2.0;        // 1.5^2 - 0.5^2 = 2.0

            // Handle ties: skip evaluation if next dim has the same |z|
            if (k + 1 < d && absZ[order[k]] == absZ[order[k + 1]]) {
                continue;
            }

            double value = sweepDot / Math.sqrt(sweepNormSq);
            if (value > bestValue) {
                bestValue = value;
                bestK = k + 1;
            }
        }

        // Write output: upgraded dims get magnitude 1.5, others stay at 0.5
        double normSq = 0;
        // Start with all at 0.5
        for (int j = 0; j < d; j++) {
            float sign = z[j] >= 0 ? 1.0f : -1.0f;
            out[j] = sign * 0.5f;
        }
        // Upgrade the top bestK dimensions to 1.5
        for (int k = 0; k < bestK; k++) {
            int dim = order[k];
            float sign = z[dim] >= 0 ? 1.0f : -1.0f;
            out[dim] = sign * 1.5f;
        }
        // Compute norm
        for (int j = 0; j < d; j++) {
            normSq += (double) out[j] * out[j];
        }
        return (float) Math.sqrt(normSq);
    }

    /**
     * General quantization path for bitsPerDim > 2 (nSteps > 1).
     */
    private float quantizeExactGeneral(float[] z, float[] out, int d, int numAbsLevels, int nSteps) {

        // Extract signs and absolute values
        float[] signs = new float[d];
        float[] absZ = new float[d];
        for (int j = 0; j < d; j++) {
            signs[j] = z[j] >= 0 ? 1.0f : -1.0f;
            absZ[j] = Math.abs(z[j]);
        }

        // Base level: all at 0.5
        double currentDot = 0;
        for (int j = 0; j < d; j++) {
            currentDot += 0.5 * absZ[j];
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
            int[] order = argsort(eventTimes, eventCount);

            // Sweep through events, tracking cumulative dot product and norm
            double cumDot = currentDot;
            double cumNormSq = currentNormSq;
            double bestValue = cumDot / Math.sqrt(cumNormSq);
            int bestStopIdx = -1; // -1 means stop at base

            int[] dimLevelCount = new int[d]; // track how many levels each dim has been incremented

            for (int idx = 0; idx < eventCount; idx++) {
                int oi = order[idx];
                int dim = eventDims[oi];
                int level = eventLevels[oi];

                cumDot += absZ[dim];
                cumNormSq += 2.0 * level;
                dimLevelCount[dim]++;

                // Handle ties: skip if next event has same time
                if (idx + 1 < eventCount) {
                    int nextOi = order[idx + 1];
                    if (eventTimes[oi] == eventTimes[nextOi]) {
                        continue;
                    }
                }

                double value = cumDot / Math.sqrt(cumNormSq);
                if (value > bestValue) {
                    bestValue = value;
                    bestStopIdx = idx;
                }
            }

            // Reconstruct bestIdx from the events up to bestStopIdx
            if (bestStopIdx >= 0) {
                Arrays.fill(dimLevelCount, 0);
                for (int idx = 0; idx <= bestStopIdx; idx++) {
                    int oi = order[idx];
                    dimLevelCount[eventDims[oi]]++;
                }
                System.arraycopy(dimLevelCount, 0, bestIdx, 0, d);
            }
        }

        // Final conversion: centered code = sign * (0.5 + bestIdx)
        double normSq = 0;
        for (int j = 0; j < d; j++) {
            float mag = 0.5f + bestIdx[j];
            out[j] = signs[j] * mag;
            normSq += (double) out[j] * out[j];
        }
        return (float) Math.sqrt(normSq);
    }

    /**
     * Returns indices that sort the first {@code count} elements of {@code values} in ascending order.
     * Uses a primitive int[] indirect sort to avoid Integer boxing and comparator dispatch overhead.
     */
    private static int[] argsort(double[] values, int count) {
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = i;
        }
        // Indirect insertion sort -- count is typically small (e.g. 384 for 2-bit, 768d).
        // Insertion sort avoids object allocation and is cache-friendly for small n.
        // For larger counts (>1000), a primitive indirect mergesort would be better,
        // but in practice bitsPerDim <= 4 and nDims <= 768 gives count <= 2304.
        for (int i = 1; i < count; i++) {
            int key = indices[i];
            double keyVal = values[key];
            int j = i - 1;
            while (j >= 0 && values[indices[j]] > keyVal) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
        return indices;
    }
}
