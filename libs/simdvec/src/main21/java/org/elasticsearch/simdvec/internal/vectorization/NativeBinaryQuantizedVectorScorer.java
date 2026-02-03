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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.simdvec.internal.Similarities;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class NativeBinaryQuantizedVectorScorer extends DefaultES93BinaryQuantizedVectorsScorer {

    private final MemorySegmentAccessInput msai;

    public NativeBinaryQuantizedVectorScorer(IndexInput in, int vectorLengthInBytes, MemorySegmentAccessInput msai) {
        super(in, vectorLengthInBytes);
        this.msai = msai;
    }

    public float score(
        int dims,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        float queryAdditionalCorrection,
        int queryQuantizedComponentSum,
        int targetOrd
    ) throws IOException {

        var offset = ((long) targetOrd * byteSize);
        var segment = msai.segmentSliceOrNull(offset, byteSize);
        if (segment == null) {
            super.score(
                dims,
                similarityFunction,
                centroidDp,
                q,
                queryLowerInterval,
                queryUpperInterval,
                queryAdditionalCorrection,
                queryQuantizedComponentSum,
                targetOrd
            );
        }

        var indexLowerInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes);
        var indexUpperInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + Float.BYTES);
        var indexAdditionalCorrection = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + 2 * Float.BYTES);
        var indexQuantizedComponentSum = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, numBytes + 3 * Float.BYTES));

        var qcDist = Similarities.dotProductI1I4(segment, MemorySegment.ofArray(q), numBytes);
        return quantizedScore(
            dims,
            similarityFunction,
            centroidDp,
            qcDist,
            queryLowerInterval,
            queryUpperInterval,
            queryAdditionalCorrection,
            queryQuantizedComponentSum,
            indexLowerInterval,
            indexUpperInterval,
            indexAdditionalCorrection,
            indexQuantizedComponentSum
        );
    }
}
