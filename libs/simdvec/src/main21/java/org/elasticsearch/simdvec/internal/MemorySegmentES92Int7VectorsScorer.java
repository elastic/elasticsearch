/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/** Panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
public final class MemorySegmentES92Int7VectorsScorer extends MemorySegmentES92PanamaInt7VectorsScorer {

    public MemorySegmentES92Int7VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions, memorySegment);
    }

    @Override
    public boolean hasNativeAccess() {
        return false; // This class does not support native access
    }

    @Override
    public long int7DotProduct(byte[] q) throws IOException {
        return panamaInt7DotProduct(q);
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        panamaInt7DotProductBulk(q, count, scores);
    }

    @Override
    public void scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        int7DotProductBulk(q, BULK_SIZE, scores);
        applyCorrectionsBulk(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores
        );
    }
}
