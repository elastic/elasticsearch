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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

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

            final long qcDist;
            if (SUPPORTS_HEAP_SEGMENTS) {
                qcDist = Similarities.dotProductD1Q4(segment, MemorySegment.ofArray(q), numBytes);
            } else {
                try (var arena = Arena.ofConfined()) {
                    var querySegment = arena.allocate(q.length, 64);
                    MemorySegment.copy(q, 0, querySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                    qcDist = Similarities.dotProductD1Q4(segment, querySegment, numBytes);
                }
            }
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
        slice.seek(0);
        return IndexInputUtils.withSlice(slice, slice.length(), this::getScratch, segment -> {
            if (SUPPORTS_HEAP_SEGMENTS) {
                var nodesSegment = MemorySegment.ofArray(nodes);
                var scoresSegment = MemorySegment.ofArray(scores);
                Similarities.dotProductD1Q4BulkWithOffsets(
                    segment,
                    MemorySegment.ofArray(q),
                    numBytes,
                    byteSize,
                    nodesSegment,
                    bulkSize,
                    scoresSegment
                );
                return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                    similarityFunction,
                    segment,
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
                    nodesSegment,
                    scoresSegment
                );
            } else {
                try (var arena = Arena.ofConfined()) {
                    var querySegment = arena.allocate(q.length, 64);
                    var offsetsSegment = arena.allocate((long) bulkSize * Integer.BYTES, 64);
                    var scoresSegment = arena.allocate((long) bulkSize * Float.BYTES, 64);
                    MemorySegment.copy(q, 0, querySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                    MemorySegment.copy(nodes, 0, offsetsSegment, ValueLayout.JAVA_INT, 0, bulkSize);
                    Similarities.dotProductD1Q4BulkWithOffsets(
                        segment,
                        querySegment,
                        numBytes,
                        byteSize,
                        offsetsSegment,
                        bulkSize,
                        scoresSegment
                    );
                    var maxScore = ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                        similarityFunction,
                        segment,
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
                        offsetsSegment,
                        scoresSegment
                    );
                    MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, bulkSize);
                    return maxScore;
                }
            }
        });
    }

    protected byte[] getScratch(int len) {
        if (scratch == null || scratch.length < len) {
            scratch = new byte[len];
        }
        return scratch;
    }
}
