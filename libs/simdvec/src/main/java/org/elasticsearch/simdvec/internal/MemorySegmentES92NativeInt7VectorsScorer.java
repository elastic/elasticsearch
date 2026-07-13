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
import org.elasticsearch.simdvec.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.elasticsearch.simdvec.internal.vectorization.ScoreCorrections.nativeApplyCorrectionsBulk;

/** Native / panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
public final class MemorySegmentES92NativeInt7VectorsScorer extends MemorySegmentES92PanamaInt7VectorsScorer {

    public MemorySegmentES92NativeInt7VectorsScorer(IndexInput in, int dimensions, int bulkSize) {
        super(in, dimensions, bulkSize);
    }

    @Override
    public long int7DotProduct(byte[] q) throws IOException {
        assert q.length == dimensions;
        return IndexInputUtils.withSlice(in, dimensions, this::getScratch, segment -> {
            final MemorySegment querySegment = MemorySegment.ofArray(q);
            return (long) Similarities.dotProductI7u(segment, querySegment, dimensions);
        });
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == dimensions;
        IndexInputUtils.withSlice(in, (long) dimensions * count, this::getScratch, segment -> {
            final MemorySegment scoresSegment = MemorySegment.ofArray(scores);
            final MemorySegment querySegment = MemorySegment.ofArray(q);
            Similarities.dotProductI7uBulk(segment, querySegment, dimensions, count, scoresSegment);
            return null;
        });
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
        float[] scores,
        int bulkSize
    ) throws IOException {
        int7DotProductBulk(q, bulkSize, scores);
        IndexInputUtils.withSlice(in, 16L * bulkSize, this::getScratch, memorySegment -> {
            nativeApplyCorrectionsBulk(
                similarityFunction,
                memorySegment,
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                SEVEN_BIT_SCALE,
                SEVEN_BIT_SCALE,
                centroidDp,
                MemorySegment.ofArray(scores)
            );
            return null;
        });
    }
}
