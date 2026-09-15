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
import org.elasticsearch.simdvec.ES940OSQVectorsScorer.QuantEncoding;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

abstract sealed class StripedES940OSQVectorsScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    static MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer usingPanama(
        IndexInput in,
        QuantEncoding encoding,
        int dimensions,
        int dataLength,
        int bulkSize
    ) {
        return new PanamaImpl(in, encoding, dimensions, dataLength, bulkSize);
    }

    static MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer usingNative(
        IndexInput in,
        QuantEncoding encoding,
        int dimensions,
        int dataLength,
        int bulkSize
    ) {
        return new NativeImpl(in, encoding, dimensions, dataLength, bulkSize);
    }

    private final BBQDotProduct dotProduct;
    protected final float queryBitScale;
    protected final float indexBitScale;

    private StripedES940OSQVectorsScorer(
        IndexInput in,
        QuantEncoding encoding,
        int dimensions,
        int dataLength,
        int bulkSize,
        BBQDotProduct dotProduct
    ) {
        super(in, dimensions, dataLength, bulkSize);
        this.dotProduct = dotProduct;
        this.queryBitScale = bitScale(encoding.queryBits());
        this.indexBitScale = bitScale(encoding.indexBits());
    }

    /**
     * Applies the corrections that follow the data vectors in the input, returning the maximum score.
     */
    abstract float applyCorrections(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int count
    ) throws IOException;

    @Override
    final long quantizeScore(byte[] q) throws IOException {
        return dotProduct.dotProduct(q);
    }

    @Override
    final boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        dotProduct.dotProductBulk(q, count, scores);
        return true;
    }

    @Override
    final boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        dotProduct.dotProductBulkOffsets(q, offsets, offsetsCount, scores, count);
        return true;
    }

    @Override
    final float scoreBulk(
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
        dotProduct.dotProductBulk(q, bulkSize, scores);
        return applyCorrections(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores,
            bulkSize
        );
    }

    @Override
    final float scoreBulkOffsets(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int[] offsets,
        int offsetsCount,
        float[] scores,
        int count
    ) throws IOException {
        dotProduct.dotProductBulkOffsets(q, offsets, offsetsCount, scores, count);
        // corrections are applied across the whole block; the unscored entries are discarded below
        applyCorrections(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores,
            count
        );
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0, offsetIdx = 0; i < count; i++) {
            if (offsetIdx < offsetsCount && offsets[offsetIdx] == i) {
                offsetIdx++;
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            } else {
                scores[i] = 0.0f;
            }
        }
        return maxScore;
    }

    private static final class PanamaImpl extends StripedES940OSQVectorsScorer {

        PanamaImpl(IndexInput in, QuantEncoding encoding, int dimensions, int dataLength, int bulkSize) {
            super(
                in,
                encoding,
                dimensions,
                dataLength,
                bulkSize,
                PanamaBBQDotProduct.create(in, dimensions, encoding.indexBits(), encoding.queryBits())
            );
        }

        @Override
        float applyCorrections(
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores,
            int count
        ) throws IOException {
            return applyCorrectionsBulk(
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                count,
                queryBitScale,
                indexBitScale
            );
        }
    }

    private static final class NativeImpl extends StripedES940OSQVectorsScorer {

        private float[] cachedScoresArray;
        private MemorySegment cachedScoresSegment;

        NativeImpl(IndexInput in, QuantEncoding encoding, int dimensions, int dataLength, int bulkSize) {
            super(
                in,
                encoding,
                dimensions,
                dataLength,
                bulkSize,
                NativeBBQDotProduct.create(in, dimensions, encoding.indexBits(), encoding.queryBits())
            );
        }

        private MemorySegment scoresSegment(float[] scores) {
            if (scores != cachedScoresArray) {
                cachedScoresArray = scores;
                cachedScoresSegment = MemorySegment.ofArray(scores);
            }
            return cachedScoresSegment;
        }

        @Override
        float applyCorrections(
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            VectorSimilarityFunction similarityFunction,
            float centroidDp,
            float[] scores,
            int count
        ) throws IOException {
            MemorySegment scoresSegment = scoresSegment(scores);
            return IndexInputUtils.withFloatSlice(
                in,
                16L * count,
                scratch,
                corrections -> ScoreCorrections.nativeApplyCorrectionsBulk(
                    similarityFunction,
                    corrections,
                    count,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    scoresSegment
                )
            );
        }
    }
}
