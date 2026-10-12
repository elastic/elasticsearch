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
import org.elasticsearch.simdvec.AshScorer;
import org.elasticsearch.simdvec.ESNextAshVectorsScorer;

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
        int planeBytes = BBQDotProduct.planeBytes(nDims);
        if (planeBytes >= 2 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            return new MSAshD1QFScorer(in, nDims, planeBytes);
        }
        return ESNextAshVectorsScorer.createFloat(in, nDims, bitsPerDim);
    }
}
