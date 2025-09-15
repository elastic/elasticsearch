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
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.elasticsearch.simdvec.internal.Similarities.int4BitDotProduct;
import static org.elasticsearch.simdvec.internal.Similarities.int4BitDotProductBulk;

/** Native scorer for quantized vectors stored as an {@link IndexInput}. */
public final class MemorySegmentES91OSQVectorsScorer extends MemorySegmentES91PanamaOSQVectorsScorer {

    private static final boolean NATIVE_SUPPORTED = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    public MemorySegmentES91OSQVectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions, memorySegment);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length * 4;
        if (NATIVE_SUPPORTED) {
            return nativeQuantizeScore(q);
        } else {
            return panamaQuantizeScore(q);
        }
    }

    private long nativeQuantizeScore(byte[] q) throws IOException {
        long initialOffset = in.getFilePointer();
        MemorySegment query = MemorySegment.ofArray(q);
        long qScore = int4BitDotProduct(query, memorySegment, initialOffset, length);
        if (qScore == -1) {
            return panamaQuantizeScore(q);
        }
        in.skipBytes(length);
        return qScore;
    }

    @Override
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (NATIVE_SUPPORTED) {
            nativeQuantizeScoreBulk(q, count, scores);
        } else {
            panamaQuantizeScoreBulk(q, count, scores);
        }
    }

    private void nativeQuantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        long initialOffset = in.getFilePointer();
        MemorySegment query = MemorySegment.ofArray(q);
        MemorySegment scoresSegment = MemorySegment.ofArray(scores);
        int4BitDotProductBulk(query, memorySegment, initialOffset, scoresSegment, count, length);
        if (scores[0] == -1) {
            panamaQuantizeScoreBulk(q, count, scores);
        } else {
            in.skipBytes(count * length);
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
