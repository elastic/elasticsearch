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

/** Panamized scorer for quantized vectors stored as a {@link IndexInput}. */
public final class OnHeapES91OSQVectorsScorer extends OnHeapES91PanamaOSQVectorsScorer {

    private static final boolean NATIVE_SUPPORTED = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    public OnHeapES91OSQVectorsScorer(IndexInput in, int dimensions) {
        super(in, dimensions);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        if (NATIVE_SUPPORTED) {
            return nativeQuantizeScore(q);
        } else {
            return panamaQuantizeScore(q);
        }
    }

    private long nativeQuantizeScore(byte[] q) throws IOException {
        in.readBytes(bytes, 0, length);
        MemorySegment query = MemorySegment.ofArray(q);
        MemorySegment memorySegment = MemorySegment.ofArray(bytes).asSlice(0, length);
        return int4BitDotProduct(query, memorySegment, 0L, length);
    }

    @Override
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        if (NATIVE_SUPPORTED) {
            nativeQuantizeScoreBulk(q, count, scores);
        } else {
            panamaQuantizeScoreBulk(q, count, scores);
        }
    }

    private void nativeQuantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        int j = 0;
        MemorySegment scoresSegment = MemorySegment.ofArray(scores);
        for (; j < count - 15; j += BULK_SIZE) {
            in.readBytes(bytes, 0, BULK_SIZE * length);
            MemorySegment query = MemorySegment.ofArray(q);
            MemorySegment memorySegment = MemorySegment.ofArray(bytes);
            int4BitDotProductBulk(query, memorySegment, 0L, scoresSegment.asSlice(j, BULK_SIZE), count, length);
        }
        for (; j < count; j++) {
            scores[j] = quantizeScore(q);
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
