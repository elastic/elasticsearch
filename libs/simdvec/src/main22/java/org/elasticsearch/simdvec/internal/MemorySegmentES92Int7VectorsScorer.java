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
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/** Native / panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
public final class MemorySegmentES92Int7VectorsScorer extends MemorySegmentES92PanamaInt7VectorsScorer {

    private static final boolean NATIVE_SUPPORTED = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    public MemorySegmentES92Int7VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions, memorySegment);
    }

    @Override
    public boolean hasNativeAccess() {
        return NATIVE_SUPPORTED;
    }

    @Override
    public long int7DotProduct(byte[] q) throws IOException {
        assert q.length == dimensions;
        if (NATIVE_SUPPORTED) {
            return nativeInt7DotProduct(q);
        } else {
            return panamaInt7DotProduct(q);
        }

    }

    private long nativeInt7DotProduct(byte[] q) throws IOException {
        final MemorySegment segment = memorySegment.asSlice(in.getFilePointer(), dimensions);
        final MemorySegment querySegment = MemorySegment.ofArray(q);
        final long res = Similarities.dotProduct7u(segment, querySegment, dimensions);
        in.skipBytes(dimensions);
        return res;
    }

    private void nativeInt7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        final MemorySegment scoresSegment = MemorySegment.ofArray(scores);
        final MemorySegment segment = memorySegment.asSlice(in.getFilePointer(), dimensions * count);
        final MemorySegment querySegment = MemorySegment.ofArray(q);
        Similarities.dotProduct7uBulk(segment, querySegment, dimensions, count, scoresSegment);
        in.skipBytes(dimensions * count);
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == dimensions;
        if (NATIVE_SUPPORTED) {
            nativeInt7DotProductBulk(q, count, scores);
        } else {
            panamaInt7DotProductBulk(q, count, scores);
        }
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
        float[] scores
    ) throws IOException {
        int7DotProductBulk(q, BULK_SIZE, scores);
        applyCorrectionsBulk(
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
