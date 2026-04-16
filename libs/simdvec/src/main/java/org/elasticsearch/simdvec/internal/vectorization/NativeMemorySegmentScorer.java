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
import org.elasticsearch.simdvec.internal.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Native scorer for quantized vectors, optimized for JDK 22+ with heap segment support.
 * All methods assume {@code NATIVE_SUPPORTED && SUPPORTS_HEAP_SEGMENTS} and use
 * {@link MemorySegment#ofArray} directly (no arena copies).
 *
 * <p>Subclasses provide only the native dot-product function and bit-scale constants;
 * the scoring logic (quantize, bulk, corrections) is handled by template methods here.
 */
abstract sealed class NativeMemorySegmentScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer permits NativeD1Q4Scorer,
    NativeD2Q4Scorer, NativeD4Q4Scorer {

    private byte[] cachedQueryArray;
    private MemorySegment cachedQuerySeg;
    private float[] cachedScoresArray;
    private MemorySegment cachedScoresSeg;

    NativeMemorySegmentScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
        assert dataLength >= 16 : "NativeMemorySegmentScorer requires dataLength >= 16, got " + dataLength;
    }

    private MemorySegment querySegment(byte[] q) {
        if (q != cachedQueryArray) {
            cachedQueryArray = q;
            cachedQuerySeg = MemorySegment.ofArray(q);
        }
        return cachedQuerySeg;
    }

    private MemorySegment scoresSegment(float[] scores) {
        if (scores != cachedScoresArray) {
            cachedScoresArray = scores;
            cachedScoresSeg = MemorySegment.ofArray(scores);
        }
        return cachedScoresSeg;
    }

    abstract long dotProduct(MemorySegment dataset, MemorySegment query, int length);

    abstract void dotProductBulk(MemorySegment dataset, MemorySegment query, int length, int count, MemorySegment scores);

    abstract void dotProductBulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int dataLength,
        int dataStride,
        MemorySegment offsets,
        int offsetsCount,
        MemorySegment scores
    );

    abstract float queryBitScale();

    abstract float indexBitScale();

    @Override
    final long quantizeScore(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> dotProduct(segment, querySegment(q), length));
    }

    @Override
    final boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        var qSeg = querySegment(q);
        var sSeg = scoresSegment(scores);
        IndexInputUtils.withSlice(in, (long) length * count, this::getScratch, dSeg -> {
            dotProductBulk(dSeg, qSeg, length, count, sSeg);
            return null;
        });
        return true;
    }

    @Override
    final boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        var qSeg = querySegment(q);
        var offsetsSeg = MemorySegment.ofArray(offsets);
        var sSeg = scoresSegment(scores);
        IndexInputUtils.withSlice(in, (long) length * count, this::getScratch, dSeg -> {
            dotProductBulkWithOffsets(dSeg, qSeg, length, length, offsetsSeg, offsetsCount, sSeg);
            return null;
        });
        repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
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
        var qSeg = querySegment(q);
        var sSeg = scoresSegment(scores);
        long vectorBytes = (long) length * bulkSize;
        long correctionBytes = 16L * bulkSize;
        return IndexInputUtils.withSlice(in, vectorBytes + correctionBytes, this::getScratch, seg -> {
            dotProductBulk(seg.asSlice(0, vectorBytes), qSeg, length, bulkSize, sSeg);
            return ScoreCorrections.nativeApplyCorrectionsBulk(
                similarityFunction,
                seg.asSlice(vectorBytes, correctionBytes),
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale(),
                indexBitScale(),
                centroidDp,
                sSeg
            );
        });
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
        var qSeg = querySegment(q);
        var offsetsSeg = MemorySegment.ofArray(offsets);
        var sSeg = scoresSegment(scores);
        long vectorBytes = (long) length * count;
        long correctionBytes = 16L * count;
        IndexInputUtils.withSlice(in, vectorBytes + correctionBytes, this::getScratch, seg -> {
            dotProductBulkWithOffsets(seg.asSlice(0, vectorBytes), qSeg, length, length, offsetsSeg, offsetsCount, sSeg);
            repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
            ScoreCorrections.nativeApplyCorrectionsBulk(
                similarityFunction,
                seg.asSlice(vectorBytes, correctionBytes),
                count,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale(),
                indexBitScale(),
                centroidDp,
                sSeg
            );
            return null;
        });
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0, offsetIdx = 0; i < count; i++) {
            if (offsetIdx < offsetsCount && offsets[offsetIdx] == i) {
                offsetIdx++;
                if (scores[i] > maxScore) maxScore = scores[i];
            } else {
                scores[i] = 0.0f;
            }
        }
        return maxScore;
    }
}
