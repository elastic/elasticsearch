/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

import java.util.Optional;

class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE = null;

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7ScalarQuantizedVectorScorer(
        VectorSimilarityType similarityType,
        IndexInput input,
        RandomAccessQuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        throw new UnsupportedOperationException("should not reach here");
    }
}
