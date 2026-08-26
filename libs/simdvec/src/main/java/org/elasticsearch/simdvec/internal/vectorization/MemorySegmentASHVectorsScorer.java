/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.AsymmetricHashingVectorsScorer;
import org.elasticsearch.simdvec.internal.BufferScratch;

import java.io.IOException;

/**
 * Panama-accelerated scorer for ASH-encoded vectors stored as an {@link IndexInput}
 * backed by a {@link java.lang.foreign.MemorySegment}.
 * <p>
 * Follows the same pattern as {@code MemorySegmentES940OSQVectorsScorer}:
 * this class wraps an inner {@link ASHMemorySegmentScorer} that provides the
 * encoding-specific SIMD implementation, falling back to the scalar
 * {@link AsymmetricHashingVectorsScorer} base when the inner scorer returns
 * {@code false}.
 */
public final class MemorySegmentASHVectorsScorer extends AsymmetricHashingVectorsScorer {

    /**
     * Creates a Panama-accelerated scorer for the given bit combination.
     *
     * @param in the IndexInput; must support MemorySegment slices
     * @param nDims number of projected dimensions
     * @param bitsPerDim document bits per dimension
     * @param queryBitsPerDim query bits per dimension (0 for float path)
     * @return a scorer using Panama SIMD where available
     */
    public static MemorySegmentASHVectorsScorer create(IndexInput in, int nDims, int bitsPerDim, int queryBitsPerDim) {
        ASHMemorySegmentScorer inner = createInner(in, nDims, bitsPerDim, queryBitsPerDim);
        return new MemorySegmentASHVectorsScorer(in, nDims, bitsPerDim, inner);
    }

    private static ASHMemorySegmentScorer createInner(IndexInput in, int nDims, int bitsPerDim, int queryBitsPerDim) {
        int planeBytes = (nDims + 7) >>> 3;
        int packedCodeBytes = bitsPerDim * planeBytes;
        // Select encoding-specific scorer based on the (queryBits, docBits) combination
        if (queryBitsPerDim == 0 && bitsPerDim == 1) {
            return new MSASHD1Q0Scorer(in, nDims, planeBytes, packedCodeBytes);
        } else if (queryBitsPerDim == 4 && bitsPerDim == 1) {
            return new MSASHD1Q4Scorer(in, nDims, planeBytes, packedCodeBytes);
        } else if (queryBitsPerDim == 4 && bitsPerDim == 2) {
            return new MSASHD2Q4Scorer(in, nDims, planeBytes, packedCodeBytes);
        }
        // Fallback: no specialized implementation for this combo
        return new ASHMemorySegmentScorer(in, nDims, planeBytes, packedCodeBytes);
    }

    private final ASHMemorySegmentScorer scorer;

    private MemorySegmentASHVectorsScorer(IndexInput in, int nDims, int bitsPerDim, ASHMemorySegmentScorer scorer) {
        super(in, nDims, bitsPerDim);
        this.scorer = scorer;
    }

    @Override
    public void scoreFloatBulk(float[] queryTransformed, float[] scores, int blockSize) throws IOException {
        boolean handled = scorer.scoreFloatBulk(queryTransformed, scores, blockSize);
        if (handled == false) {
            super.scoreFloatBulk(queryTransformed, scores, blockSize);
        }
    }

    @Override
    public void scoreIntegerBulk(byte[] queryQuantized, int queryBitsPerDim, float[] scores, int blockSize) throws IOException {
        boolean handled = scorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, scores, blockSize);
        if (handled == false) {
            super.scoreIntegerBulk(queryQuantized, queryBitsPerDim, scores, blockSize);
        }
    }

    /**
     * Sealed base class for MemorySegment-backed ASH scorers.
     * <p>
     * Default implementations return {@code false} to signal that the base
     * scalar implementation should be used instead.
     */
    static sealed class ASHMemorySegmentScorer permits MSASHD1Q0Scorer, MSASHD1Q4Scorer, MSASHD2Q4Scorer {

        protected final IndexInput in;
        protected final int nDims;
        protected final int planeBytes;
        protected final int packedCodeBytes;
        protected final BufferScratch scratch = new BufferScratch();

        ASHMemorySegmentScorer(IndexInput in, int nDims, int planeBytes, int packedCodeBytes) {
            IndexInputUtils.checkInputType(in);
            this.in = in;
            this.nDims = nDims;
            this.planeBytes = planeBytes;
            this.packedCodeBytes = packedCodeBytes;
        }

        /**
         * Float-path bulk scoring. Returns {@code false} if not implemented.
         * When returning {@code true}, the IndexInput has been advanced past
         * all {@code blockSize * packedCodeBytes} bytes.
         */
        boolean scoreFloatBulk(float[] queryTransformed, float[] scores, int blockSize) throws IOException {
            return false;
        }

        /**
         * Integer-path bulk scoring. Returns {@code false} if not implemented.
         * When returning {@code true}, the IndexInput has been advanced past
         * all {@code blockSize * packedCodeBytes} bytes.
         */
        boolean scoreIntegerBulk(byte[] queryQuantized, int queryBitsPerDim, float[] scores, int blockSize) throws IOException {
            return false;
        }
    }
}
