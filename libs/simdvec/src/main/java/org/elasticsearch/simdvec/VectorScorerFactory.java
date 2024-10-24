/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

import java.util.Optional;

/** A factory of quantized vector scorers. */
public interface VectorScorerFactory {

    static Optional<VectorScorerFactory> instance() {
        return Optional.ofNullable(VectorScorerFactoryImpl.INSTANCE);
    }

    /**
     * Returns an optional containing an int7 scalar quantized vector score supplier
     * for the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param similarityType the similarity type
     * @param input the index input containing the vector data;
     *    offset of the first vector is 0,
     *    the length must be (maxOrd + Float#BYTES) * dims
     * @param values the random access vector values
     * @param scoreCorrectionConstant the score correction constant
     * @return an optional containing the vector scorer supplier, or empty
     */
    Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    );

    /**
     * Returns an optional containing an int7 scalar quantized vector scorer for
     * the given parameters, or an empty optional if a scorer is not supported.
     *
     * @param sim the similarity type
     * @param values the random access vector values
     * @param queryVector the query vector
     * @return an optional containing the vector scorer, or empty
     */
    Optional<RandomVectorScorer> getInt7SQVectorScorer(VectorSimilarityFunction sim, QuantizedByteVectorValues values, float[] queryVector);
}
