/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Scorer for ASH-encoded vectors stored in an {@link IndexInput}.
 * <p>
 * Reads packed bit-plane codes directly from the wrapped input and computes
 * raw (uncorrected) dot products against a precomputed query. Two scoring paths
 * are supported:
 * <ul>
 *   <li><b>Float path</b> ({@code queryBitsPerDim == 0}): full-precision projected
 *       query scored via {@code ipFloatBit} per bit-plane. The result is the
 *       weighted sum over bit planes before scale/offset correction.</li>
 *   <li><b>Integer path</b> ({@code queryBitsPerDim > 0}): quantized query scored
 *       via AND+popcount across query and document bit-plane pairs. The result
 *       is the raw integer dot product before correction.</li>
 * </ul>
 * <p>
 * The caller is responsible for applying per-vector corrections (scale, offset,
 * docSum, etc.) from the AoS correction buffer after the codes have been read.
 * <p>
 * Subclasses may override the scoring methods to use SIMD or native
 * implementations while reading from {@link java.lang.foreign.MemorySegment}
 * views of the same input.
 */
public class AsymmetricHashingVectorsScorer {

    /** The wrapped {@link IndexInput}. */
    protected final IndexInput in;
    protected final int nDims;
    protected final int bitsPerDim;
    protected final int planeBytes;
    protected final int packedCodeBytes;

    // Pre-allocated scratch buffer for reading document bit-plane data in the scalar integer path.
    // Sized to hold one vector's worth of packed codes (bitsPerDim * planeBytes bytes).
    private final byte[] docPlanesScratch;

    public AsymmetricHashingVectorsScorer(IndexInput in, int nDims, int bitsPerDim) {
        this.in = in;
        this.nDims = nDims;
        this.bitsPerDim = bitsPerDim;
        this.planeBytes = (nDims + 7) >>> 3;
        this.packedCodeBytes = bitsPerDim * planeBytes;
        this.docPlanesScratch = new byte[packedCodeBytes];
    }

    /**
     * Reads {@code blockSize} vectors' packed codes from the input and computes
     * raw (uncorrected) dot products using the float scoring path.
     * <p>
     * The raw dot product is the weighted sum over bit planes:
     * {@code dot = sum_p (2^p * ipFloatBit(queryTransformed, plane_p)) - centerOffset * sum(queryTransformed)}
     * <p>
     * The input must be positioned at the start of the first vector's packed codes
     * (all {@code blockSize} vectors contiguous). After return the input is
     * advanced past all packed code bytes.
     *
     * @param queryTransformed precomputed query @ W (raw projection, not centered)
     * @param scores output array for raw dot products, must have length >= blockSize
     * @param blockSize number of vectors to score
     */
    public void scoreFloatBulk(float[] queryTransformed, float[] scores, int blockSize) throws IOException {
        float sum = ESVectorUtil.sum(queryTransformed, nDims);
        int numLevels = 1 << bitsPerDim;
        float centerOffset = (numLevels - 1) / 2.0f;
        for (int j = 0; j < blockSize; j++) {
            scores[j] = scoreFloatSingleScalar(queryTransformed, sum, centerOffset);
        }
    }

    /**
     * Reads {@code blockSize} vectors' packed codes from the input and computes
     * raw (uncorrected) integer dot products using the integer scoring path.
     * <p>
     * The raw dot product is the weighted AND+popcount sum:
     * {@code rawDot = sum_qp sum_dp (2^qp * 2^dp * andBitCount(queryPlane[qp], docPlane[dp]))}
     * <p>
     * After return the input is advanced past all packed code bytes.
     *
     * @param queryQuantized quantized query in bit-plane format
     * @param queryBitsPerDim bits per dimension for the quantized query
     * @param scores output array for raw integer dot products (stored as float), must have length >= blockSize
     * @param blockSize number of vectors to score
     */
    public void scoreIntegerBulk(byte[] queryQuantized, int queryBitsPerDim, float[] scores, int blockSize) throws IOException {
        for (int j = 0; j < blockSize; j++) {
            scores[j] = scoreIntegerSingleScalar(queryQuantized, queryBitsPerDim);
        }
    }

    // --- Scalar implementations ---

    private float scoreFloatSingleScalar(float[] queryTransformed, float querySum, float centerOffset) throws IOException {
        float dot = -centerOffset * querySum;
        for (int p = 0; p < bitsPerDim; p++) {
            float planeSum = ipFloatBitFromInput(queryTransformed, planeBytes);
            dot = Math.fma(1 << p, planeSum, dot);
        }
        return dot;
    }

    /**
     * Scalar ipFloatBit reading bit-plane data directly from the IndexInput.
     */
    private float ipFloatBitFromInput(float[] q, int byteLength) throws IOException {
        float acc0 = 0, acc1 = 0, acc2 = 0, acc3 = 0;
        for (int i = 0; i < byteLength; i++) {
            byte mask = in.readByte();
            int base = i * Byte.SIZE;
            int remaining = nDims - base;
            if (remaining >= 8) {
                acc0 = Math.fma(q[base], (mask >> 7) & 1, acc0);
                acc1 = Math.fma(q[base + 1], (mask >> 6) & 1, acc1);
                acc2 = Math.fma(q[base + 2], (mask >> 5) & 1, acc2);
                acc3 = Math.fma(q[base + 3], (mask >> 4) & 1, acc3);
                acc0 = Math.fma(q[base + 4], (mask >> 3) & 1, acc0);
                acc1 = Math.fma(q[base + 5], (mask >> 2) & 1, acc1);
                acc2 = Math.fma(q[base + 6], (mask >> 1) & 1, acc2);
                acc3 = Math.fma(q[base + 7], mask & 1, acc3);
            } else {
                for (int j = 0; j < remaining; j++) {
                    acc0 = Math.fma(q[base + j], (mask >> (7 - j)) & 1, acc0);
                }
            }
        }
        return acc0 + acc1 + acc2 + acc3;
    }

    private float scoreIntegerSingleScalar(byte[] queryQuantized, int queryBitsPerDim) throws IOException {
        // Read all bit planes for this document vector into the pre-allocated scratch buffer
        in.readBytes(docPlanesScratch, 0, packedCodeBytes);

        int rawDot = 0;
        for (int qp = 0; qp < queryBitsPerDim; qp++) {
            for (int dp = 0; dp < bitsPerDim; dp++) {
                int pc = ESVectorUtil.andBitCount(queryQuantized, qp * planeBytes, docPlanesScratch, dp * planeBytes, planeBytes);
                int weight = (1 << qp) * (1 << dp);
                rawDot += weight * pc;
            }
        }
        return rawDot;
    }
}
