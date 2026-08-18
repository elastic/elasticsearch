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
     * @param centeredCodes codes centered around zero, row-major (n x nDims)
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
        int nSteps = (1 << (bitsPerDim - 1)) - 1;

        return switch (nSteps) {
            case 0 -> quantizeExact1Bit(z, zOffset, out, outOffset, d);
            case 1 -> quantizeExact2Bit(z, zOffset, out, outOffset, d);
            default -> quantizeExactGeneral(z, zOffset, out, outOffset, d, nSteps);
        };
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
     * Specialized fast path for 2-bit quantization (nSteps=1).
     * <p>
     * Each dimension is either at level 0.5 or 1.5. The optimal assignment sorts magnitude values
     * ascending, then iterates in descending order to find the cutoff that maximises
     * cumDot / sqrt(cumNormSq), where upgrading dimension j adds |z_j| to cumDot and 2.0 to cumNormSq.
     * The selected set is recovered via a threshold on |z_j| rather than by tracking indices.
     */
    static float quantizeExact2Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        // Base level: all dims at 0.5 -> cumDot = sum(0.5 * |z_j|), cumNormSq = 0.25 * d
        float[] absZ = new float[d];
        double dot = 0;
        for (int j = 0; j < d; j++) {
            absZ[j] = Math.abs(z[zOffset + j]);
            dot = Math.fma(0.5, absZ[j], dot);
        }

        // Sorted ascending; the iteration is then done backwards
        Arrays.sort(absZ);

        double normSq = 0.25 * d;
        double bestValue = dot / Math.sqrt(normSq);

        // iterate dims in |z| descending order
        int bestK = 0; // number of dimensions to upgrade to level 1.5
        for (int k = 0; k < d; k++) {
            int i = d - 1 - k;
            dot += absZ[i];  // upgrading from 0.5 to 1.5 adds 1.0 * |z_dim|
            normSq += 2.0;   // 1.5^2 - 0.5^2 = 2.0

            // Handle ties: skip evaluation if next dim has the same |z|
            if (i > 0 && absZ[i] == absZ[i - 1]) {
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
            float threshold = absZ[d - bestK];
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
     * <p>
     * Every candidate assignment has the form {@code 0.5 + min(nSteps, floor(t * |z_j|))} for some
     * threshold {@code t}, so the optimum is found by sweeping the thresholds at which a dimension
     * gains a level: the critical time of the pair (step, dim) is {@code step / |z_j|}. Those times
     * ascend with step for a fixed dimension, so once the magnitudes are sorted the
     * {@code nSteps * d} events form {@code nSteps} runs that are each already ordered, and the
     * sweep is a merge of the runs rather than a sort of all the events. The selected set is
     * recovered from the threshold alone, which is why only magnitudes need sorting and not the
     * dimension indices alongside them.
     */
    static float quantizeExactGeneral(float[] z, int zOffset, float[] out, int outOffset, int d, int nSteps) {
        // Base level: all dims at 0.5 -> dot = sum(0.5 * |z_j|), normSq = 0.25 * d
        float[] absZ = new float[d];
        double baseDot = 0;
        for (int j = 0; j < d; j++) {
            float a = Math.abs(z[zOffset + j]);
            absZ[j] = a;
            baseDot = Math.fma(0.5, a, baseDot);
        }

        // Sorted ascending; every run walks it backwards, so zero magnitudes -- which gain no level
        // at any threshold -- sit past the end of each run
        Arrays.sort(absZ);
        int firstNonZero = 0;
        while (firstNonZero < d && absZ[firstNonZero] == 0) {
            firstNonZero++;
        }

        double bestDot = baseDot;
        double bestNormSq = 0.25 * d;
        // The winning event fixes the threshold at bestStep / bestMag; step 0 means no event
        // improved on the base level
        int bestStep = 0;
        double bestMag = 0;

        if (firstNonZero < d) {
            // Run s (1..nSteps) holds the events of step s, next one at position heads[s - 1]. All
            // runs start at the largest magnitude, where the critical time s / |z| ascends with s,
            // so the runs in step order already satisfy the heap invariant.
            int[] heads = new int[nSteps];
            double[] headMags = new double[nSteps];
            int[] heap = new int[nSteps];
            Arrays.fill(heads, d - 1);
            Arrays.fill(headMags, absZ[d - 1]);
            Arrays.setAll(heap, IntUnaryOperator.identity());

            double dot = baseDot;
            double normSq = bestNormSq;
            int events = nSteps * (d - firstNonZero);
            for (int e = 0; e < events; e++) {
                int run = heap[0];
                int step = run + 1;
                double mag = headMags[run];

                dot += mag;
                normSq += 2 * step;

                int head = --heads[run];
                // An exhausted run carries magnitude 0, i.e. an infinite critical time, so it sinks
                // to the bottom of the heap and is never selected again
                headMags[run] = head < firstNonZero ? 0 : absZ[head];
                siftDown(heap, headMags, nSteps);

                // Handle ties: skip evaluation if the next event is at the same critical time
                int next = heap[0];
                if (step * headMags[next] == (next + 1) * mag) {
                    continue;
                }

                // dot / sqrt(normSq) > bestDot / sqrt(bestNormSq), cross-multiplied to avoid a
                // divide and a square root per event
                if (dot * dot * bestNormSq > bestDot * bestDot * normSq) {
                    bestDot = dot;
                    bestNormSq = normSq;
                    bestStep = step;
                    bestMag = mag;
                }
            }
        }

        if (bestStep == 0) {
            return quantizeExact1Bit(z, zOffset, out, outOffset, d);
        }

        // Every event up to the winning one was consumed, so dimension j holds each step s whose
        // critical time s / |z_j| is at or below the threshold bestStep / bestMag. The tie rule
        // above only ever settles on the last event of a run of equal critical times, so the
        // threshold picks out exactly the consumed events.
        for (int j = 0; j < d; j++) {
            float v = z[zOffset + j];
            // Exact: bestStep occupies 7 bits at most and the magnitude 24
            double scaled = bestStep * (double) Math.abs(v);
            int levels = (int) Math.min(scaled / bestMag, nSteps);
            // That division is the only inexact step, so nudge the level it landed on. Both
            // comparisons are exact, and each loop runs at most once.
            while (levels < nSteps && (levels + 1) * bestMag <= scaled) {
                levels++;
            }
            while (levels > 0 && levels * bestMag > scaled) {
                levels--;
            }
            out[outOffset + j] = Math.copySign(0.5f + levels, v);
        }

        // Each event contributed 2 * step to normSq, and summing that over steps 1..m gives
        // exactly (0.5 + m)^2 - 0.25, so the tracked value is the code's squared norm
        return (float) Math.sqrt(bestNormSq);
    }

    /**
     * Restores the heap invariant at the root, ordering runs by the critical time of their next
     * event.
     */
    private static void siftDown(int[] heap, double[] headMags, int size) {
        int i = 0;
        while (true) {
            int child = 2 * i + 1;
            if (child >= size) {
                return;
            }
            int right = child + 1;
            if (right < size && earlier(heap[right], heap[child], headMags)) {
                child = right;
            }
            if (earlier(heap[child], heap[i], headMags) == false) {
                return;
            }
            int tmp = heap[i];
            heap[i] = heap[child];
            heap[child] = tmp;
            i = child;
        }
    }

    /**
     * Whether run {@code r1}'s next event precedes run {@code r2}'s, i.e.
     * {@code (r1 + 1) / mag(r1) < (r2 + 1) / mag(r2)}, cross-multiplied so that the comparison --
     * and the tie detection it feeds -- stays exact. A run carrying magnitude 0 is exhausted and
     * never precedes a live run.
     */
    private static boolean earlier(int r1, int r2, double[] headMags) {
        return (r1 + 1) * headMags[r2] < (r2 + 1) * headMags[r1];
    }
}
