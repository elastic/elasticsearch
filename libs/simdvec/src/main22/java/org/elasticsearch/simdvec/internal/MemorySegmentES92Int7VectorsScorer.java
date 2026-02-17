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

    public MemorySegmentES92Int7VectorsScorer(IndexInput in, int dimensions, int bulkSize, MemorySegment memorySegment) {
        super(in, dimensions, bulkSize, memorySegment);
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
        } else if (memorySegment != null) {
            return panamaInt7DotProduct(q);
        } else {
            return super.int7DotProduct(q);
        }
    }

    private long nativeInt7DotProduct(byte[] q) throws IOException {
        return IndexInputSegments.withSlice(in, memorySegment, dimensions, segment -> {
            final MemorySegment querySegment = MemorySegment.ofArray(q);
            return Similarities.dotProductI7u(segment, querySegment, dimensions);
        });
    }

    private void nativeInt7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        IndexInputSegments.withSlice(in, memorySegment, (long) dimensions * count, segment -> {
            final MemorySegment scoresSegment = MemorySegment.ofArray(scores);
            final MemorySegment querySegment = MemorySegment.ofArray(q);
            Similarities.dotProductI7uBulk(segment, querySegment, dimensions, count, scoresSegment);
            return null;
        });
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == dimensions;
        if (NATIVE_SUPPORTED) {
            nativeInt7DotProductBulk(q, count, scores);
        } else if (memorySegment != null) {
            panamaInt7DotProductBulk(q, count, scores);
        } else {
            super.int7DotProductBulk(q, count, scores);
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
        float[] scores,
        int bulkSize
    ) throws IOException {
        if (memorySegment != null) {
            int7DotProductBulk(q, bulkSize, scores);
            applyCorrectionsBulk(
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                bulkSize
            );
        } else {
            super.scoreBulk(
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
        }
    }
}
