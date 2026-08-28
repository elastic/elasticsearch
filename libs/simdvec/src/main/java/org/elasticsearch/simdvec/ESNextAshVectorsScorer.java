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
import org.elasticsearch.simdvec.internal.FixedSizeScratch;

import java.io.IOException;

/**
 * Scalar scorer for ASH-encoded vectors. Provides factory methods that return
 * typed {@link AshScorer} instances for the float or integer query path.
 *
 * @see AshScorer
 */
public final class ESNextAshVectorsScorer {

    private ESNextAshVectorsScorer() {}

    /** Creates a scalar {@link AshScorer} for the float-query path. */
    public static AshScorer<float[]> createFloat(IndexInput in, int nDims, int bitsPerDim) {
        return new FloatImpl(in, nDims, bitsPerDim);
    }

    /** Creates a scalar {@link AshScorer} for the integer-query path. */
    public static AshScorer<byte[]> createInteger(IndexInput in, int nDims, int bitsPerDim, int queryBitsPerDim) {
        return new IntegerImpl(in, nDims, bitsPerDim, queryBitsPerDim);
    }

    /** Scalar float-path scorer. */
    static final class FloatImpl implements AshScorer<float[]> {
        private final IndexInput in;
        private final int nDims;
        private final int bitsPerDim;
        private final int planeBytes;

        FloatImpl(IndexInput in, int nDims, int bitsPerDim) {
            this.in = in;
            this.nDims = nDims;
            this.bitsPerDim = bitsPerDim;
            this.planeBytes = (nDims + 7) >>> 3;
        }

        @Override
        public float score(float[] queryTransformed) throws IOException {
            float sum = ESVectorUtil.sum(queryTransformed, nDims);
            int numLevels = 1 << bitsPerDim;
            float centerOffset = (numLevels - 1) / 2.0f;
            return scoreFloatSingle(queryTransformed, sum, centerOffset);
        }

        @Override
        public void scoreBulk(float[] queryTransformed, float[] scores, int blockSize) throws IOException {
            float sum = ESVectorUtil.sum(queryTransformed, nDims);
            int numLevels = 1 << bitsPerDim;
            float centerOffset = (numLevels - 1) / 2.0f;
            for (int j = 0; j < blockSize; j++) {
                scores[j] = scoreFloatSingle(queryTransformed, sum, centerOffset);
            }
        }

        private float scoreFloatSingle(float[] queryTransformed, float querySum, float centerOffset) throws IOException {
            float dot = -centerOffset * querySum;
            for (int p = 0; p < bitsPerDim; p++) {
                float planeSum = ipFloatBitFromInput(queryTransformed);
                dot = Math.fma(1 << p, planeSum, dot);
            }
            return dot;
        }

        private float ipFloatBitFromInput(float[] q) throws IOException {
            float acc0 = 0, acc1 = 0, acc2 = 0, acc3 = 0;
            for (int i = 0; i < planeBytes; i++) {
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
    }

    /** Scalar integer-path scorer. */
    static final class IntegerImpl implements AshScorer<byte[]> {
        private final IndexInput in;
        private final int bitsPerDim;
        private final int queryBitsPerDim;
        private final int planeBytes;
        private final int packedCodeBytes;
        private final FixedSizeScratch docPlanesScratch;

        IntegerImpl(IndexInput in, int nDims, int bitsPerDim, int queryBitsPerDim) {
            this.in = in;
            this.bitsPerDim = bitsPerDim;
            this.queryBitsPerDim = queryBitsPerDim;
            this.planeBytes = (nDims + 7) >>> 3;
            this.packedCodeBytes = bitsPerDim * planeBytes;
            this.docPlanesScratch = new FixedSizeScratch(packedCodeBytes);
        }

        @Override
        public float score(byte[] queryQuantized) throws IOException {
            return scoreIntegerSingle(queryQuantized);
        }

        @Override
        public void scoreBulk(byte[] queryQuantized, float[] scores, int blockSize) throws IOException {
            for (int j = 0; j < blockSize; j++) {
                scores[j] = scoreIntegerSingle(queryQuantized);
            }
        }

        private float scoreIntegerSingle(byte[] queryQuantized) throws IOException {
            byte[] docPlanes = docPlanesScratch.apply(packedCodeBytes);
            in.readBytes(docPlanes, 0, packedCodeBytes);

            int rawDot = 0;
            for (int qp = 0; qp < queryBitsPerDim; qp++) {
                for (int dp = 0; dp < bitsPerDim; dp++) {
                    int pc = ESVectorUtil.andBitCount(queryQuantized, qp * planeBytes, docPlanes, dp * planeBytes, planeBytes);
                    int weight = (1 << qp) * (1 << dp);
                    rawDot += weight * pc;
                }
            }
            return rawDot;
        }
    }
}
