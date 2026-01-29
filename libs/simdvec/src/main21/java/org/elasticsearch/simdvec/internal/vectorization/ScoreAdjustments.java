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
import java.lang.invoke.MethodHandle;

class ScoreAdjustments {
    static final VectorSimilarityFunctions SIMILARITY_FUNCTIONS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    static final MethodHandle SCORE_EUCLIDEAN_BULK = SIMILARITY_FUNCTIONS.scoreEuclideanBulk();
    static final MethodHandle SCORE_MAX_INNER_PRODUCT_BULK = SIMILARITY_FUNCTIONS.scoreMaxInnerProductBulk();
    static final MethodHandle SCORE_DOT_PRODUCT_BULK = SIMILARITY_FUNCTIONS.scoreDotProductBulk();

    static float nativeScoreBulk(
        VectorSimilarityFunction similarityFunction,
        MemorySegment corrections,
        int bulkSize,
        int dimensions,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        float queryBitScale,
        float centroidDp,
        MemorySegment scores
    ) {
        try {
            return switch (similarityFunction) {
                case EUCLIDEAN -> (float) SCORE_EUCLIDEAN_BULK.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    centroidDp,
                    scores
                );
                case DOT_PRODUCT, COSINE -> (float) SCORE_DOT_PRODUCT_BULK.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    centroidDp,
                    scores
                );
                case MAXIMUM_INNER_PRODUCT -> (float) SCORE_MAX_INNER_PRODUCT_BULK.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    centroidDp,
                    scores
                );
            };
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof Error err) {
            throw err;
        }
        return t instanceof RuntimeException re ? re : new RuntimeException(t);
    }
}
