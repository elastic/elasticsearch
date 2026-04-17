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
import org.elasticsearch.simdvec.internal.Similarities;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class NativeBinaryQuantizedVectorScorer extends DefaultES93BinaryQuantizedVectorScorer {

    private byte[] scratch;

    public NativeBinaryQuantizedVectorScorer(IndexInput in, int dimensions, int vectorLengthInBytes) {
        super(in, dimensions, vectorLengthInBytes);
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
        slice.seek(offset);
        return IndexInputUtils.withSlice(slice, byteSize, this::getScratch, segment -> {
            var indexLowerInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes);
            var indexUpperInterval = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + Float.BYTES);
            var indexAdditionalCorrection = segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, numBytes + 2 * Float.BYTES);
            var indexQuantizedComponentSum = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, numBytes + 3 * Float.BYTES));

            long qcDist = Similarities.dotProductD1Q4(segment, MemorySegment.ofArray(q), numBytes);
            return applyCorrections(
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
        });
    }

    @Override
    public float scoreBulk(
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
        if (bulkSize == 0) {
            return Float.NEGATIVE_INFINITY;
        }
        long[] vectorOffsets = new long[bulkSize];
        for (int i = 0; i < bulkSize; i++) {
            vectorOffsets[i] = (long) nodes[i] * byteSize;
        }

        float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
        boolean resolved = IndexInputUtils.withSliceAddresses(slice, vectorOffsets, numBytes, bulkSize, addrs -> {
            var scoresSegment = MemorySegment.ofArray(scores);
            Similarities.dotProductD1Q4BulkSparse(addrs, MemorySegment.ofArray(q), numBytes, bulkSize, scoresSegment);
            maxScore[0] = ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                similarityFunction,
                addrs,
                bulkSize,
                numBytes,
                byteSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryQuantizedComponentSum,
                queryAdditionalCorrection,
                FOUR_BIT_SCALE,
                1.0f,
                centroidDp,
                scoresSegment
            );
        });

        if (resolved == false) {
            return super.scoreBulk(
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
        }
        return maxScore[0];
    }

    protected byte[] getScratch(int len) {
        if (scratch == null || scratch.length < len) {
            scratch = new byte[len];
        }
        return scratch;
    }
}
