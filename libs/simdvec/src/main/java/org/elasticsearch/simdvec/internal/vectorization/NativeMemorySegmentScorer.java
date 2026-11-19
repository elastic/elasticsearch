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
import org.elasticsearch.lucene.store.SegmentBufferScratch;
import org.elasticsearch.simdvec.BulkMask;
import org.elasticsearch.simdvec.ScoringQuery;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Native scorer for quantized vectors, optimized for JDK 22+ with heap segment support.
 * All methods assume {@code NATIVE_SUPPORTED && SUPPORTS_HEAP_SEGMENTS}. The query is copied
 * into a scorer-owned off-heap buffer (once per query change).
 *
 * <p>Subclasses provide only the native dot-product function and bit-scale constants;
 * the scoring logic (quantize, bulk, corrections) is handled by template methods here.
 */
abstract sealed class NativeMemorySegmentScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer permits
    NativeD2Q4PackedScorer, NativeD4Q4PackedScorer, NativeD7Q7Scorer {

    protected static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElseThrow(AssertionError::new);

    private final ScoringQuery.OffHeapCopy queryCopy = new ScoringQuery.OffHeapCopy();
    private float[] cachedScoresArray;
    private MemorySegment cachedScoresSeg;
    private final SegmentBufferScratch scoresScratch = new SegmentBufferScratch();

    NativeMemorySegmentScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
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

    abstract void dotProductBulkMasked(
        MemorySegment dataset,
        MemorySegment query,
        int dataLength,
        int dataStride,
        int mask,
        int count,
        MemorySegment scores
    );

    abstract float queryBitScale();

    abstract float indexBitScale();

    @Override
    final long quantizeScore(ScoringQuery q) throws IOException {
        return IndexInputUtils.withSlice(in, length, scratch, segment -> dotProduct(segment, queryCopy.segmentFor(q), length));
    }

    @Override
    final boolean quantizeScoreBulk(ScoringQuery q, int count, float[] scores) throws IOException {
        var qSeg = queryCopy.segmentFor(q);
        var sSeg = scoresSegment(scores);
        IndexInputUtils.withVoidSlice(in, (long) length * count, scratch, dSeg -> { dotProductBulk(dSeg, qSeg, length, count, sSeg); });
        return true;
    }

    @Override
    final boolean quantizeScoreBulkMasked(ScoringQuery q, int mask, float[] scores, int count) throws IOException {
        var qSeg = queryCopy.segmentFor(q);
        var scoresSegment = scoresScratch.apply(count * Float.BYTES);
        IndexInputUtils.withVoidSlice(in, (long) length * count, scratch, dSeg -> {
            dotProductBulkMasked(dSeg, qSeg, length, length, mask, count, scoresSegment);
        });
        MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0L, scores, 0, count);
        return true;
    }

    @Override
    final float scoreBulk(
        ScoringQuery q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        var qSeg = queryCopy.segmentFor(q);
        var sSeg = scoresSegment(scores);
        long vectorBytes = (long) length * bulkSize;
        long correctionBytes = 16L * bulkSize;
        return IndexInputUtils.withFloatSlice(in, vectorBytes + correctionBytes, scratch, seg -> {
            dotProductBulk(seg, qSeg, length, bulkSize, sSeg);
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
    final float scoreBulkMasked(
        ScoringQuery q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int mask,
        float[] scores,
        int count
    ) throws IOException {
        var qSeg = queryCopy.segmentFor(q);
        var scoresSegment = scoresScratch.apply(count * Float.BYTES);
        long vectorBytes = (long) length * count;
        long correctionBytes = 16L * count;
        IndexInputUtils.withVoidSlice(in, vectorBytes + correctionBytes, scratch, seg -> {
            dotProductBulkMasked(seg, qSeg, length, length, mask, count, scoresSegment);
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
                scoresSegment
            );
        });
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < count; i++) {
            if (BulkMask.isSelected(mask, i)) {
                scores[i] = scoresSegment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                if (scores[i] > maxScore) maxScore = scores[i];
            } else {
                scores[i] = 0.0f;
            }
        }
        return maxScore;
    }
}
