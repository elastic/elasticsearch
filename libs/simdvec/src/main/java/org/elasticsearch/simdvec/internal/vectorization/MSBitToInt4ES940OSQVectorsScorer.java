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

/** Panamized scorer for D1Q4 vectors stored as a {@link java.lang.foreign.MemorySegment}. */
final class MSBitToInt4ES940OSQVectorsScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    MSBitToInt4ES940OSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length * 4;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return quantizeScore256(q);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return quantizeScore128(q);
            }
        }
        return Long.MIN_VALUE;
    }

    private long quantizeScore256(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, scratch, segment -> fourStripeBitDotProduct256(q, segment, 0L, length));
    }

    private long quantizeScore128(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, scratch, segment -> fourStripeBitDotProduct128(q, segment, 0L, length));
    }

    @Override
    public boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length * 4;
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

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        var datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withVoidSlice(in, datasetLengthInBytes, scratch, segment -> {
            for (int i = 0; i < count; i++) {
                scores[i] = fourStripeBitDotProduct256(q, segment, (long) i * length, length);
            }
        });
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        var datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withVoidSlice(in, datasetLengthInBytes, scratch, segment -> {
            for (int i = 0; i < count; i++) {
                scores[i] = fourStripeBitDotProduct128(q, segment, (long) i * length, length);
            }
        });
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
        assert q.length == length * 4;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, bulkSize, scores);
                return applyCorrections256Bulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    ONE_BIT_SCALE
                );
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, bulkSize, scores);
                return applyCorrections128Bulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    ONE_BIT_SCALE
                );
            }
        }
        return Float.NEGATIVE_INFINITY;
    }
}
