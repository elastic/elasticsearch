/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;
import java.util.Objects;

/**
 * Delegates all operations to a given delegate, while exposing the most recently seen KnnVectorValues.
 */
final class DelegatingFlatVectorsScorer implements FlatVectorsScorer {

    private final FlatVectorsScorer delegate;
    private KnnVectorValues lastVectorValues;

    DelegatingFlatVectorsScorer(FlatVectorsScorer delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    public KnnVectorValues getLastVectorValuesOrNull() {
        return lastVectorValues;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(VectorSimilarityFunction sim, KnnVectorValues values)
        throws IOException {
        lastVectorValues = values;
        return delegate.getRandomVectorScorerSupplier(sim, values);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, float[] target)
        throws IOException {
        lastVectorValues = values;
        return delegate.getRandomVectorScorer(sim, values, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, byte[] target)
        throws IOException {
        lastVectorValues = values;
        return delegate.getRandomVectorScorer(sim, values, target);
    }

    @Override
    public String toString() {
        return "DelegatingFlatVectorsScorer[" + delegate + "]";
    }
}
