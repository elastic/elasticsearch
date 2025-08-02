/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.internal.Float32VectorScorer;
import org.elasticsearch.simdvec.internal.Float32VectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorerSupplier;

import java.util.Optional;

final class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE;

    private VectorScorerFactoryImpl() {}

    static {
        INSTANCE = NativeAccess.instance().getVectorSimilarityFunctions().map(ignore -> new VectorScorerFactoryImpl()).orElse(null);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        return Int7SQVectorScorerSupplier.create(similarityType, input, values, scoreCorrectionConstant);
    }

    @Override
    public Optional<RandomVectorScorer> getInt7SQVectorScorer(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        float[] queryVector
    ) {
        return Int7SQVectorScorer.create(sim, values, queryVector);
    }

    // -- floats

    @Override
    public Optional<RandomVectorScorerSupplier> getFloat32VectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        return Float32VectorScorerSupplier.create(similarityType, input, values);
    }

    @Override
    public Optional<RandomVectorScorer> getFloat32VectorScorer(
        VectorSimilarityFunction sim,
        FloatVectorValues values,
        float[] queryVector
    ) {
        return Float32VectorScorer.create(sim, values, queryVector);
    }

    @Override
    public String toString() {
        return "VectorScorerFactoryImpl";
    }
}
