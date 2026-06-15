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
import org.elasticsearch.simdvec.internal.IndexInputUtils;
import org.elasticsearch.simdvec.internal.Similarities;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Packed-nibble int4 scorer that uses existing native dot-product ops.
 * Returns sentinel values when native support is unavailable so callers can fallback.
 */
final class NativePackedInt4Scorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    private byte[] cachedQueryArray;
    private MemorySegment cachedQuerySeg;
    private float[] cachedScoresArray;
    private MemorySegment cachedScoresSeg;

    NativePackedInt4Scorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
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

    @Override
    long quantizeScore(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(
            in,
            length,
            this::getScratch,
            segment -> Similarities.dotProductI4(querySegment(q), segment, length)
        );
    }

    @Override
    boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        var qSeg = querySegment(q);
        var sSeg = scoresSegment(scores);
        IndexInputUtils.withSlice(in, (long) length * count, this::getScratch, dSeg -> {
            Similarities.dotProductI4Bulk(dSeg, qSeg, length, count, sSeg);
            return null;
        });
        return true;
    }

    @Override
    boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        var qSeg = querySegment(q);
        var offsetsSeg = MemorySegment.ofArray(offsets);
        var sSeg = scoresSegment(scores);
        IndexInputUtils.withSlice(in, (long) length * count, this::getScratch, dSeg -> {
            Similarities.dotProductI4BulkWithOffsets(dSeg, qSeg, length, length, offsetsSeg, offsetsCount, sSeg);
            return null;
        });
        repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
        return true;
    }
}
