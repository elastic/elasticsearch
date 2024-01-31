/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;

public class ESScalarQuantizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {

    private final ESRandomAccessQuantizedByteVectorValues values;
    private final ScalarQuantizedVectorSimilarity similarity;

    ESScalarQuantizedRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        ScalarQuantizer scalarQuantizer,
        ESRandomAccessQuantizedByteVectorValues values
    ) {
        this.similarity = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(similarityFunction, scalarQuantizer.getConstantMultiplier());
        this.values = values;
    }

    private ESScalarQuantizedRandomVectorScorerSupplier(
        ScalarQuantizedVectorSimilarity similarity,
        ESRandomAccessQuantizedByteVectorValues values
    ) {
        this.similarity = similarity;
        this.values = values;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
        final ESRandomAccessQuantizedByteVectorValues vectorsCopy = values.copy();
        final byte[] queryVector = values.vectorValue(ord);
        final float queryOffset = values.getScoreCorrectionConstant();
        return new ESScalarQuantizedRandomVectorScorer(similarity, vectorsCopy, queryVector, queryOffset);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
        return new ESScalarQuantizedRandomVectorScorerSupplier(similarity, values.copy());
    }
}
