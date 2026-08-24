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
import org.elasticsearch.lucene.store.IndexInputUtils;

import java.io.IOException;

/** Panamized scorer for 4-bit vectors stored as a {@link java.lang.foreign.MemorySegment}. */
final class MSInt4SymmetricES940OSQVectorsScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    MSInt4SymmetricES940OSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return quantizeScoreSymmetric256(q);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return quantizeScoreSymmetric128(q);
            }
        }
        return Long.MIN_VALUE;
    }

    private long quantizeScoreSymmetric128(byte[] q) throws IOException {
        long stripe0 = quantizeScore128(q);
        long stripe1 = quantizeScore128(q);
        long stripe2 = quantizeScore128(q);
        long stripe3 = quantizeScore128(q);
        return stripe0 + (stripe1 << 1) + (stripe2 << 2) + (stripe3 << 3);
    }

    private long quantizeScoreSymmetric256(byte[] q) throws IOException {
        long stripe0 = quantizeScore256(q);
        long stripe1 = quantizeScore256(q);
        long stripe2 = quantizeScore256(q);
        long stripe3 = quantizeScore256(q);
        return stripe0 + (stripe1 << 1) + (stripe2 << 2) + (stripe3 << 3);
    }

    private long quantizeScore256(byte[] q) throws IOException {
        int size = length / 4;
        return IndexInputUtils.withSlice(in, size, scratch, segment -> fourStripeBitDotProduct256(q, segment, 0L, size));
    }

    private long quantizeScore128(byte[] q) throws IOException {
        int size = length / 4;
        return IndexInputUtils.withSlice(in, size, scratch, segment -> fourStripeBitDotProduct128(q, segment, 0L, size));
    }

    @Override
    public boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, count, scores);
                return true;
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, count, scores);
                return true;
            }
        }
        return false;
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = quantizeScoreSymmetric128(q);
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = quantizeScoreSymmetric256(q);
        }
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
        float[] scores,
        int bulkSize
    ) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, bulkSize, scores);
                return applyCorrectionsBulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    FOUR_BIT_SCALE
                );
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, bulkSize, scores);
                return applyCorrectionsBulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    FOUR_BIT_SCALE
                );
            }
        }
        return Float.NEGATIVE_INFINITY;
    }

}
