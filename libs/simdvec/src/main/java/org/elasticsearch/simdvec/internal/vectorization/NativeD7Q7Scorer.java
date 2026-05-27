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
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.internal.IndexInputUtils;
import org.elasticsearch.simdvec.internal.MemorySegmentES92NativeInt7VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7uBulkWithOffsets;

/** Vectorized scorer for 7-bit symmetric quantized vectors stored as a {@link MemorySegment}. */
final class NativeD7Q7Scorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    private final MemorySegmentES92NativeInt7VectorsScorer int7Scorer;

    NativeD7Q7Scorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
        this.int7Scorer = new MemorySegmentES92NativeInt7VectorsScorer(in, dimensions, bulkSize);
    }

    @Override
    long quantizeScore(byte[] q) throws IOException {
        return int7Scorer.int7DotProduct(q);
    }

    @Override
    boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        int7Scorer.int7DotProductBulk(q, count, scores);
        return true;
    }

    @Override
    public boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        assert q.length == length;
        var querySegment = MemorySegment.ofArray(q);
        var offsetsSegment = MemorySegment.ofArray(offsets);
        var scoresSegment = MemorySegment.ofArray(scores);
        nativeQuantizeScoreBulkOffsets(querySegment, offsetsSegment, scoresSegment, offsetsCount, count);
        repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
        return true;
    }

    private void nativeQuantizeScoreBulkOffsets(
        MemorySegment querySegment,
        MemorySegment offsetsSegment,
        MemorySegment scoresSegment,
        int offsetsCount,
        int totalCount
    ) throws IOException {
        var datasetLengthInBytes = (long) length * totalCount;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, datasetSegment -> {
            dotProductI7uBulkWithOffsets(datasetSegment, querySegment, length, length, offsetsSegment, offsetsCount, scoresSegment);
            return null;
        });
    }

    @Override
    float scoreBulk(
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
        int7Scorer.scoreBulk(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores,
            bulkSize
        );
        return ESVectorUtil.max(scores, bulkSize);
    }
}
