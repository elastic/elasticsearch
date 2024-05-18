/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.apache.lucene.store.IndexInput;

import java.util.Optional;

/** A factory of quantized vector scorers. */
public interface VectorScorerFactory {

    static Optional<VectorScorerFactory> instance() {
        return Optional.ofNullable(VectorScorerFactoryImpl.INSTANCE);
    }

    /**
     * Returns an optional containing an int7 scalar quantized vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param dims the vector dimensions
     * @param maxOrd the ordinal of the largest vector accessible
     * @param scoreCorrectionConstant the score correction constant
     * @param similarityType the similarity type
     * @param indexInput the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd + Float#BYTES) * dims
     * @return an optional containing the vector scorer, or empty
     */
    Optional<VectorScorer> getInt7ScalarQuantizedVectorScorer(
        int dims,
        int maxOrd,
        float scoreCorrectionConstant,
        VectorSimilarityType similarityType,
        IndexInput indexInput
    );
}
