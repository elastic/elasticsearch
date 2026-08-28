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
import org.elasticsearch.simdvec.AshScorer;
import org.elasticsearch.simdvec.ESNextAshVectorsScorer;
import org.elasticsearch.simdvec.internal.BufferScratch;

import java.io.IOException;

/**
 * Panama-accelerated scorers for ASH-encoded vectors stored as an {@link IndexInput}
 * backed by a {@link java.lang.foreign.MemorySegment}.
 * <p>
 * Provides factory methods that return typed {@link AshScorer} instances, delegating
 * to encoding-specific inner scorers for SIMD acceleration and falling back to the
 * scalar {@link ESNextAshVectorsScorer} implementations when the SIMD path is unavailable.
 */
public final class MemorySegmentESNextAshVectorsScorer {

    private MemorySegmentESNextAshVectorsScorer() {}

    /**
     * Creates a Panama-accelerated float-path scorer for D*QF combinations.
     *
     * @param in the IndexInput; must support MemorySegment slices
     * @param nDims number of projected dimensions
     * @param bitsPerDim document bits per dimension
     * @return a scorer implementing {@link AshScorer}{@code <float[]>}
     */
    public static AshScorer<float[]> createFloat(IndexInput in, int nDims, int bitsPerDim) {
        int planeBytes = (nDims + 7) >>> 3;
        int packedCodeBytes = bitsPerDim * planeBytes;
        AshMemorySegmentScorer<float[]> inner = null;
        if (bitsPerDim == 1) {
            inner = new MSAshD1QFScorer(in, nDims, planeBytes, packedCodeBytes);
        }
        AshScorer<float[]> scalar = ESNextAshVectorsScorer.createFloat(in, nDims, bitsPerDim);
        return new DelegatingScorer<>(inner, scalar);
    }

    /**
     * Creates a Panama-accelerated integer-path scorer for D*Q4 combinations.
     *
     * @param in the IndexInput; must support MemorySegment slices
     * @param nDims number of projected dimensions
     * @param bitsPerDim document bits per dimension
     * @param queryBitsPerDim query bits per dimension
     * @return a scorer implementing {@link AshScorer}{@code <byte[]>}
     */
    public static AshScorer<byte[]> createInteger(IndexInput in, int nDims, int bitsPerDim, int queryBitsPerDim) {
        int planeBytes = (nDims + 7) >>> 3;
        int packedCodeBytes = bitsPerDim * planeBytes;
        AshMemorySegmentScorer<byte[]> inner = null;
        if (queryBitsPerDim == 4 && bitsPerDim == 1) {
            inner = new MSAshD1Q4Scorer(in, nDims, planeBytes, packedCodeBytes);
        } else if (queryBitsPerDim == 4 && bitsPerDim == 2) {
            inner = new MSAshD2Q4Scorer(in, nDims, planeBytes, packedCodeBytes);
        }
        AshScorer<byte[]> scalar = ESNextAshVectorsScorer.createInteger(in, nDims, bitsPerDim, queryBitsPerDim);
        return new DelegatingScorer<>(inner, scalar);
    }

    /**
     * Wraps an optional SIMD inner scorer with a scalar fallback.
     * When the inner scorer returns a sentinel value, delegates to the scalar implementation.
     */
    private record DelegatingScorer<T>(AshMemorySegmentScorer<T> inner, AshScorer<T> scalar) implements AshScorer<T> {

        @Override
        public float score(T query) throws IOException {
            if (inner != null) {
                float score = inner.score(query);
                if (score != Float.NEGATIVE_INFINITY) {
                    return score;
                }
            }
            return scalar.score(query);
        }

        @Override
        public void scoreBulk(T query, float[] scores, int blockSize) throws IOException {
            if (inner != null) {
                boolean handled = inner.scoreBulk(query, scores, blockSize);
                if (handled) {
                    return;
                }
            }
            scalar.scoreBulk(query, scores, blockSize);
        }
    }

    /**
     * MemorySegment-backed inner scorer interface.
     * Returns {@link Float#NEGATIVE_INFINITY} from {@code score()} or {@code false}
     * from {@code scoreBulk()} when the SIMD path is not available for a given input.
     */
    interface AshMemorySegmentScorer<T> {
        float score(T query) throws IOException;

        boolean scoreBulk(T query, float[] scores, int blockSize) throws IOException;
    }

    /**
     * Shared state for all MemorySegment-backed ASH inner scorers.
     */
    abstract static class AshMemorySegmentScorerBase {
        protected final IndexInput in;
        protected final int nDims;
        protected final int planeBytes;
        protected final int packedCodeBytes;
        protected final BufferScratch scratch = new BufferScratch();

        AshMemorySegmentScorerBase(IndexInput in, int nDims, int planeBytes, int packedCodeBytes) {
            IndexInputUtils.checkInputType(in);
            this.in = in;
            this.nDims = nDims;
            this.planeBytes = planeBytes;
            this.packedCodeBytes = packedCodeBytes;
        }
    }
}
