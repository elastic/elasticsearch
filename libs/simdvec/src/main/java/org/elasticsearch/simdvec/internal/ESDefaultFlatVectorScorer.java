/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;

public final class ESDefaultFlatVectorScorer implements FlatVectorsScorer {

    private static final FlatVectorsScorer DELEGATE = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

    private static FlatVectorsScorer getScorerFor(KnnVectorValues values) {
        // can't use MemorySegment scorer for bfloat16, have to fallback to standard scorer
        // which operates on arrays, not raw MemorySegments
        return values.getEncoding() == VectorEncoding.FLOAT32 && values.getVectorByteLength() == values.dimension() * 2
            ? DefaultFlatVectorScorer.INSTANCE
            : DELEGATE;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues
    ) throws IOException {
        return getScorerFor(vectorValues).getRandomVectorScorerSupplier(similarityFunction, vectorValues);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        float[] target
    ) throws IOException {
        return getScorerFor(vectorValues).getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        return getScorerFor(vectorValues).getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    @Override
    public String toString() {
        return "ESDefaultFlatVectorScorer(delegate=" + DELEGATE + ")";
    }
}
