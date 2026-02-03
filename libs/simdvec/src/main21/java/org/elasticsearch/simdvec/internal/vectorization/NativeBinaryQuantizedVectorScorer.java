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

    public NativeBinaryQuantizedVectorScorer(IndexInput in, int dimensions, int vectorLengthInBytes, MemorySegmentAccessInput msai) {
        super(in, dimensions, vectorLengthInBytes);
        this.msai = msai;
    }

    public float score(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryQuantizedComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int targetOrd
    ) throws IOException {

        var offset = ((long) targetOrd * byteSize);
        var segment = msai.segmentSliceOrNull(offset, byteSize);
        if (segment == null) {
            super.score(
                q,
                queryLowerInterval,
                queryUpperInterval,
                queryQuantizedComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                targetOrd
            );
        }

        var indexLowerInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes);
        var indexUpperInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + Float.BYTES);
        var indexAdditionalCorrection = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + 2 * Float.BYTES);
        var indexQuantizedComponentSum = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, numBytes + 3 * Float.BYTES));

        var qcDist = Similarities.dotProductI1I4(segment, MemorySegment.ofArray(q), numBytes);
        return quantizedScore(
            dimensions,
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

    @Override
    public void scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryQuantizedComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int[] nodes,
        float[] scores,
        int bulkSize
    ) throws IOException {
        var segment = msai.segmentSliceOrNull(0, slice.length());
        if (segment == null) {
            // Try to score individually, delegating it to our parent implementation (which is looping)
            super.scoreBulk(
                q,
                queryLowerInterval,
                queryUpperInterval,
                queryQuantizedComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                nodes,
                scores,
                bulkSize
            );
            return;
        }

        Similarities.dotProductI1I4BulkWithOffsets(
            segment,
            MemorySegment.ofArray(q),
            numBytes,
            byteSize,
            MemorySegment.ofArray(nodes),
            bulkSize,
            MemorySegment.ofArray(scores)
        );

        // TODO: native/vectorize this code too
        for (int i = 0; i < bulkSize; i++) {
            var offset = ((long) nodes[i] * byteSize);

            var indexLowerInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, offset + numBytes);
            var indexUpperInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, offset + numBytes + Float.BYTES);
            var indexAdditionalCorrection = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, offset + numBytes + 2 * Float.BYTES);
            var indexQuantizedComponentSum = Short.toUnsignedInt(
                segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, offset + numBytes + 3 * Float.BYTES)
            );

            scores[i] = quantizedScore(
                dimensions,
                similarityFunction,
                centroidDp,
                scores[i],
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
}
