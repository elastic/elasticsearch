/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.util.Objects;

/**
 * Flat vectors scorer that wraps a given delegate. This scorer first checks the Elasticsearch
 * simdvec scorer factory for an optimized implementation before falling back to the delegate
 * if one does not exist.
 *
 * <p>The current implementation checks for optimized simdvec float32 scorers.
 */
public final class ESFlatVectorsScorer implements FlatVectorsScorer {

    static final VectorScorerFactory VECTOR_SCORER_FACTORY = VectorScorerFactory.instance().orElse(null);

    final FlatVectorsScorer delegate;

    /** Creates a FlatVectorsScorer delegating to Lucene's default scorer {@link FlatVectorScorerUtil#getLucene99FlatVectorsScorer()}
     *  if the platform does not have an optimized scorer factory.
     */
    public static FlatVectorsScorer create() {
        return create(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    }

    /** Creates a FlatVectorsScorer. Returns the delegate if the platform does not have an optimized scorer factory. */
    static FlatVectorsScorer create(FlatVectorsScorer delegate) {
        Objects.requireNonNull(delegate);
        if (VECTOR_SCORER_FACTORY == null) {
            return delegate;
        }
        return new ESFlatVectorsScorer(delegate);
    }

    private ESFlatVectorsScorer(FlatVectorsScorer delegate) {
        this.delegate = delegate;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(VectorSimilarityFunction sim, KnnVectorValues values)
        throws IOException {
        assert VECTOR_SCORER_FACTORY != null;
        if (values instanceof FloatVectorValues fValues && fValues instanceof HasIndexSlice sliceable) {
            if (sliceable.getSlice() != null) {
                var scorer = VECTOR_SCORER_FACTORY.getFloat32VectorScorerSupplier(
                    VectorSimilarityType.of(sim),
                    sliceable.getSlice(),
                    fValues
                );
                if (scorer.isPresent()) {
                    return scorer.get();
                }
            }
        }
        return delegate.getRandomVectorScorerSupplier(sim, values);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, float[] query)
        throws IOException {
        assert VECTOR_SCORER_FACTORY != null;
        if (values instanceof FloatVectorValues fValues && fValues instanceof HasIndexSlice sliceable) {
            if (sliceable.getSlice() != null) {
                var scorer = VECTOR_SCORER_FACTORY.getFloat32VectorScorer(sim, fValues, query);
                if (scorer.isPresent()) {
                    return scorer.get();
                }
            }
        }
        return delegate.getRandomVectorScorer(sim, values, query);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, byte[] query) throws IOException {
        assert VECTOR_SCORER_FACTORY != null;
        return delegate.getRandomVectorScorer(sim, values, query);
    }

    @Override
    public String toString() {
        return "ESFlatVectorsScorer(" + "delegate=" + delegate + ", factory=" + VECTOR_SCORER_FACTORY + ')';
    }
}
