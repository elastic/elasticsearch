/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

/** Quantized vector scorer */
final class ESScalarQuantizedRandomVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer<byte[]> {

    private static float quantizeQuery(
        float[] query,
        byte[] quantizedQuery,
        VectorSimilarityFunction similarityFunction,
        ScalarQuantizer scalarQuantizer
    ) {
        float[] processedQuery = query;
        if (similarityFunction.equals(VectorSimilarityFunction.COSINE)) {
            float[] queryCopy = ArrayUtil.copyOfSubArray(query, 0, query.length);
            VectorUtil.l2normalize(queryCopy);
            processedQuery = queryCopy;
        }
        return scalarQuantizer.quantize(processedQuery, quantizedQuery, similarityFunction);
    }

    private final byte[] quantizedQuery;
    private final float queryOffset;
    private final ESRandomAccessQuantizedByteVectorValues values;
    private final ScalarQuantizedVectorSimilarity similarity;

    ESScalarQuantizedRandomVectorScorer(
        ScalarQuantizedVectorSimilarity similarityFunction,
        ESRandomAccessQuantizedByteVectorValues values,
        byte[] query,
        float queryOffset
    ) {
        super(values);
        this.quantizedQuery = query;
        this.queryOffset = queryOffset;
        this.similarity = similarityFunction;
        this.values = values;
    }

    ESScalarQuantizedRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        ScalarQuantizer scalarQuantizer,
        ESRandomAccessQuantizedByteVectorValues values,
        float[] query
    ) {
        super(values);
        byte[] quantizedQuery = new byte[query.length];
        float correction = quantizeQuery(query, quantizedQuery, similarityFunction, scalarQuantizer);
        this.quantizedQuery = quantizedQuery;
        this.queryOffset = correction;
        this.similarity = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(similarityFunction, scalarQuantizer.getConstantMultiplier());
        this.values = values;
    }

    @Override
    public float score(int node) throws IOException {
        byte[] storedVectorValue = values.vectorValue(node);
        float storedVectorCorrection = values.getScoreCorrectionConstant();
        return similarity.score(quantizedQuery, this.queryOffset, storedVectorValue, storedVectorCorrection);
    }
}
