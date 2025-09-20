/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/** Panamaized scorer for quantized vectors stored as a {@link MemorySegment}.  */
public final class MemorySegmentES91OSQVectorsScorer extends MemorySegmentES91PanamaOSQVectorsScorer {

    public MemorySegmentES91OSQVectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions, memorySegment);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        return panamaQuantizeScore(q);
    }

    @Override
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        panamaQuantizeScoreBulk(q, count, scores);
    }

    @Override
    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        quantizeScoreBulk(q, BULK_SIZE, scores);
        return applyCorrectionsBulk(
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
