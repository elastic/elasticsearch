/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.LSBRadixSorter;
import org.elasticsearch.simdvec.internal.vectorization.PanamaAshSphericalScalarQuantizer;

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
public sealed class AshSphericalScalarQuantizer permits PanamaAshSphericalScalarQuantizer {

    private final int bitsPerDim;

    /**
     * Result of quantization for a single vector.
     *
     * @param centeredCode code centered around zero, length nDims
     * @param codeNorm L2 norm of the code vector
     */
    public record SingleQuantizeResult(float[] centeredCode, float codeNorm) {}

    /**
     * Result of batch quantization.
     *
     * @param centeredCodes codes centered around zero, row-major matrix (n x nDims)
     * @param codeNorms L2 norm of each code vector, length n
     */
    public record QuantizeResult(float[] centeredCodes, float[] codeNorms) {}

    /**
     * Creates a spherical scalar quantizer with the given bit width.
     *
     * @param bitsPerDim number of bits per projected dimension (must be >= 1)
     * @throws IllegalArgumentException if bitsPerDim is less than 1
     */
    protected AshSphericalScalarQuantizer(int bitsPerDim) {
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
     * @param x the vectors to quantize, row-major matrix (n x nDims)
     * @param n number of vectors
     * @param nDims components per vector
     */
    public QuantizeResult encode(float[] x, int n, int nDims) {
        float[] centeredCodes = new float[n * nDims];
        float[] codeNorms = new float[n];

        for (int i = 0; i < n; i++) {
            int base = i * nDims;
            codeNorms[i] = quantizeExact(x, base, centeredCodes, base, nDims);
        }
        return new QuantizeResult(centeredCodes, codeNorms);
    }

    public SingleQuantizeResult encodeOne(float[] xLatent) {
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
    public float quantizeExact(float[] z, int zOffset, float[] out, int outOffset, int d) {
        assert assertAllFinite(z);  // all vector values must be finite for the maths to work

        int nSteps = (1 << (bitsPerDim - 1)) - 1;

        return switch (nSteps) {
            case 0 -> quantizeExact1Bit(z, zOffset, out, outOffset, d);
            case 1 -> quantizeExact2Bit(z, zOffset, out, outOffset, d);
            default -> quantizeExactGeneral(z, zOffset, out, outOffset, d, nSteps);
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
    protected float quantizeExact1Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        for (int j = 0; j < d; j++) {
            out[outOffset + j] = Math.copySign(0.5f, z[zOffset + j]);
        }
        return (float) Math.sqrt(0.25 * d);
    }

    /**
     * Thread-local reusable radix sorter, to reduce buffer allocations when handling lots of vectors of the same dimension.
     */
    private static final ThreadLocal<LSBRadixSorter> SORTER = ThreadLocal.withInitial(LSBRadixSorter::new);

    protected double calculateBaseLevel(float[] z, int zOffset, int[] absZF) {
        // Base level: all dims at 0.5 -> cumDot = sum(0.5 * |z_j|), cumNormSq = 0.25 * d
        // use doubles here, as small differences between steps can be significant
        double dot = 0;
        for (int j = 0; j < absZF.length; j++) {
            float abs = Math.abs(z[zOffset + j]);
            absZF[j] = Float.floatToRawIntBits(abs);
            dot = Math.fma(0.5, abs, dot);
        }
        return dot;
    }

    protected void set2BitOutput(float threshold, float[] z, int zOffset, float[] out, int outOffset, int d) {
        for (int j = 0; j < d; j++) {
            // The tie rule only ever sets bestK at the end of a run of equal magnitudes, so
            // selecting every dimension at or above the smallest upgraded magnitude picks out exactly
            // bestK of them
            // need to recalculate abs(z[..]) here, as the absZ array order has changed
            float v = z[zOffset + j];
            out[outOffset + j] = Math.copySign(Math.abs(v) >= threshold ? 1.5f : 0.5f, v);
        }
    }

    /**
     * Specialized fast path for 2-bit quantization (nSteps=1).
     * <p>
     * Each dimension is either at level 0.5 or 1.5. The optimal assignment sorts magnitude values
     * ascending, then iterates in descending order to find the cutoff that maximises
     * cumDot / sqrt(cumNormSq), where upgrading dimension j adds |z_j| to cumDot and 2.0 to cumNormSq.
     * The selected set is recovered via a threshold on |z_j| rather than by tracking indices.
     */
    protected float quantizeExact2Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        int[] absZF = new int[d];
        double dot = calculateBaseLevel(z, zOffset, absZF);

        // Sorted ascending; the iteration is then done backwards
        // sort as ints, as all values are positive and finite
        // and that eliminates some float-specific handling of NaN/Inf values
        // this also allows us to use radix sort, which is faster and less branch-y than Arrays.sort
        SORTER.get().sort(31, absZF, d);

        double normSq = 0.25 * d;
        double bestDot = dot;
        double bestNormSq = normSq;

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

            // dot / sqrt(normSq) > bestDot / sqrt(bestNormSq), cross-multiplied to avoid a divide
            // and a square root per dimension
            if (dot * dot * bestNormSq > bestDot * bestDot * normSq) {
                bestDot = dot;
                bestNormSq = normSq;
                bestK = k + 1;
            }
        }

        if (bestK == 0) {
            // bah humbug, didn't find anything, so everything is 0.5
            return quantizeExact1Bit(z, zOffset, out, outOffset, d);
        }

        float threshold = Float.intBitsToFloat(absZF[d - bestK]);
        set2BitOutput(threshold, z, zOffset, out, outOffset, d);

        // vector is now (d - bestK) x 0.5, and bestK x 1.5,
        // which is what the iteration accumulated into bestNormSq
        // it started out as d x 0.5^2, and was adjusted for every dim switched to 1.5
        return (float) Math.sqrt(bestNormSq);
    }

    protected void setGeneralOutput(float[] z, int zOffset, float[] out, int outOffset, int d, int nSteps, int bestStep, double bestMag) {
        // Every event up to the winning one was consumed, so dimension j holds each step s whose
        // critical time s / |z_j| is at or below the threshold bestStep / bestMag. The tie rule
        // only ever settles on the last event of a run of equal critical times, so the
        // threshold picks out exactly the consumed events.
        for (int j = 0; j < d; j++) {
            float v = z[zOffset + j];
            double scaled = bestStep * (double) Math.abs(v);
            int levels = (int) Math.min(scaled / bestMag, nSteps);
            // That division is the only inexact step, and it is correctly rounded, so the truncated
            // quotient sits either side of the exact level by at most one. Both comparisons below
            // are exact, so a single correction in one direction settles it.
            if (levels < nSteps && (levels + 1) * bestMag <= scaled) {
                levels++;
            } else if (levels > 0 && levels * bestMag > scaled) {
                levels--;
            }
            out[outOffset + j] = Math.copySign(0.5f + levels, v);
        }
    }

    /**
     * General quantization path for bitsPerDim > 2 (nSteps > 1).
     * <p>
     * Every candidate assignment has the form {@code 0.5 + min(nSteps, floor(t * |z_j|))} for some
     * threshold {@code t}, so the optimum is found by sweeping the thresholds at which a dimension
     * gains a level: the critical time of the pair (step, dim) is {@code step / |z_j|}. Those times
     * ascend with step for a fixed dimension, so once the magnitudes are sorted the
     * {@code nSteps * d} events form {@code nSteps} runs that are each already ordered, and the
     * sweep is a merge of the runs. The selected set is recovered from the threshold alone,
     * which is why only magnitudes need sorting and not the dimension indices alongside them.
     */
    protected float quantizeExactGeneral(float[] z, int zOffset, float[] out, int outOffset, int d, int nSteps) {
        int[] absZF = new int[d];
        double baseDot = calculateBaseLevel(z, zOffset, absZF);

        // Sorted ascending; the iteration is then done backwards
        // sort as ints - see use in 2bit method
        SORTER.get().sort(31, absZF, d);
        int firstNonZero = 0;
        while (firstNonZero < d && absZF[firstNonZero] == 0) {
            firstNonZero++;
        }

        double bestDot = baseDot;
        double bestNormSq = 0.25 * d;
        // The winning event fixes the threshold at bestStep / bestMag; step 0 means no event
        // improved on the base level
        int bestStep = 0;
        double bestMag = 0;

        if (firstNonZero < d) {
            // Run s (1..nSteps) holds the events of step s, next one at position heads[s - 1].
            // All runs start at the largest magnitude, where the critical time s / |z| ascends with s,
            // so the runs in step order already satisfy the heap invariant.
            double dot = baseDot;
            double normSq = bestNormSq;
            RunHeap heap = new RunHeap(nSteps, d, Float.intBitsToFloat(absZF[d - 1]));
            int events = nSteps * (d - firstNonZero);

            for (int e = 0; e < events; e++) {
                int run = heap.heap[0];
                int step = run + 1;
                double mag = heap.headMags[run];

                dot += mag;
                normSq += 2 * step;

                int head = --heap.heads[run];
                // An exhausted run carries magnitude 0, i.e. an infinite critical time,
                // so it sinks to the bottom of the heap and is never selected again
                heap.headMags[run] = head < firstNonZero ? 0 : Float.intBitsToFloat(absZF[head]);
                heap.siftDown();

                // Handle ties: skip evaluation if the next event is at the same critical time
                int next = heap.heap[0];
                if (step * heap.headMags[next] == (next + 1) * mag) {
                    continue;
                }

                // dot / sqrt(normSq) > bestDot / sqrt(bestNormSq),
                // cross-multiplied to avoid a divide and a square root per event
                if (dot * dot * bestNormSq > bestDot * bestDot * normSq) {
                    bestDot = dot;
                    bestNormSq = normSq;
                    bestStep = step;
                    bestMag = mag;
                }
            }
        }

        if (bestStep == 0) {
            // bah humbug, didn't find anything, so everything is 0.5
            return quantizeExact1Bit(z, zOffset, out, outOffset, d);
        }

        setGeneralOutput(z, zOffset, out, outOffset, d, nSteps, bestStep, bestMag);

        // Each event contributed 2 * step to normSq, and summing that over steps 1..m gives
        // exactly (0.5 + m)^2 - 0.25, so the tracked value is the code's squared norm
        return (float) Math.sqrt(bestNormSq);
    }

    /**
     * Min-heap of runs, ordered by the critical time of their next event.
     */
    private static class RunHeap {
        private final int[] heap;
        private final int[] heads;
        private final double[] headMags;

        RunHeap(int nSteps, int d, double maxMag) {
            heap = new int[nSteps];
            heads = new int[nSteps];
            headMags = new double[nSteps];
            Arrays.setAll(heap, IntUnaryOperator.identity());
            Arrays.fill(heads, d - 1);
            Arrays.fill(headMags, maxMag);
        }

        /**
         * Restores the heap invariant at the root, ordering runs by the critical time
         * of their next event.
         */
        private void siftDown() {
            int i = 0;
            while (true) {
                int child = 2 * i + 1;
                if (child >= heap.length) {
                    return;
                }
                int right = child + 1;
                if (right < heap.length && earlier(heap[right], heap[child])) {
                    child = right;
                }
                if (!earlier(heap[child], heap[i])) {
                    return;
                }
                int swp = heap[i];
                heap[i] = heap[child];
                heap[child] = swp;
                i = child;
            }
        }

        /**
         * Whether run {@code r1}'s next event precedes run {@code r2}'s,
         * i.e. {@code (r1 + 1) / mag(r1) < (r2 + 1) / mag(r2)}, but cross-multiplied for efficiency.
         * A run carrying magnitude 0 is exhausted and never precedes a live run.
         */
        private boolean earlier(int r1, int r2) {
            return (r1 + 1) * headMags[r2] < (r2 + 1) * headMags[r1];
        }
    }
}
