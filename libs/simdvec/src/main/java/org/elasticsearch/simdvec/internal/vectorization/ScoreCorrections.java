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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;

import java.lang.foreign.MemorySegment;

public class ScoreCorrections {
    static final VectorSimilarityFunctions SIMILARITY_FUNCTIONS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    public static float nativeApplyCorrectionsBulk(
        VectorSimilarityFunction similarityFunction,
        MemorySegment corrections,
        int bulkSize,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        MemorySegment scores
    ) {
        return switch (similarityFunction) {
            case EUCLIDEAN -> SIMILARITY_FUNCTIONS.applyCorrectionsEuclideanBulk(
                corrections,
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                scores
            );
            case DOT_PRODUCT, COSINE -> SIMILARITY_FUNCTIONS.applyCorrectionsDotProductBulk(
                corrections,
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                scores
            );
            case MAXIMUM_INNER_PRODUCT -> SIMILARITY_FUNCTIONS.applyCorrectionsMaxInnerProductBulk(
                corrections,
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                scores
            );
        };
    }

    /**
     * Apply corrections to a bulk of scores, reading per-vector correction trailers inline at
     * {@code addresses[i] + vectorSizeInBytes} (BBQ-style layout). The trailing target component sum
     * is encoded either as a 4-byte int ({@code readComponentSumAsInt = true}, or as a 2-byte unsigned short
     * zero-extended to int ({@code readComponentSumAsInt = false}), mirroring
     * {@code writeComponentSumAsInt} in {@code DiskBBQBulkWriter})
     */
    public static float nativeBbqApplyCorrectionsBulk(
        VectorSimilarityFunction similarityFunction,
        MemorySegment data,
        int bulkSize,
        int vectorSizeInBytes,
        int pitchInBytes,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float indexBitScale,
        float centroidDp,
        boolean readComponentSumAsInt,
        MemorySegment scores
    ) {
        byte readAsInt = readComponentSumAsInt ? (byte) 1 : (byte) 0;
        return switch (similarityFunction) {
            case EUCLIDEAN -> SIMILARITY_FUNCTIONS.bbqApplyCorrectionsEuclideanBulk(
                data,
                bulkSize,
                vectorSizeInBytes,
                pitchInBytes,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                readAsInt,
                scores
            );
            case DOT_PRODUCT, COSINE -> SIMILARITY_FUNCTIONS.bbqApplyCorrectionsDotProductBulk(
                data,
                bulkSize,
                vectorSizeInBytes,
                pitchInBytes,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                readAsInt,
                scores
            );
            case MAXIMUM_INNER_PRODUCT -> SIMILARITY_FUNCTIONS.bbqApplyCorrectionsMaxInnerProductBulk(
                data,
                bulkSize,
                vectorSizeInBytes,
                pitchInBytes,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                queryBitScale,
                indexBitScale,
                centroidDp,
                readAsInt,
                scores
            );
        };
    }
}
