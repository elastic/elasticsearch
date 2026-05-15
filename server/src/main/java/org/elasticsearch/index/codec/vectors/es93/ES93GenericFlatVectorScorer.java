/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.util.Optional;

public class ES93GenericFlatVectorScorer implements FlatVectorsScorer {

    private static final FlatVectorsScorer FALLBACK = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();
    private static final VectorScorerFactory FACTORY = ESVectorizationProvider.getInstance().getVectorScorerFactory();

    public static final ES93GenericFlatVectorScorer INSTANCE = new ES93GenericFlatVectorScorer();

    private static boolean isBFloat16(KnnVectorValues values) {
        return values.getEncoding() == VectorEncoding.FLOAT32 && values.getVectorByteLength() == values.dimension() * 2;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues
    ) throws IOException {
        if (isBFloat16(vectorValues)) {
            // can't use MemorySegment scorer for bfloat16, have to fallback to standard scorer
            // which operates on arrays, not raw MemorySegments
            if (vectorValues instanceof HasIndexSlice sl) {
                Optional<RandomVectorScorerSupplier> scorer = FACTORY.getBFloat16VectorScorerSupplier(
                    VectorSimilarityType.of(similarityFunction),
                    sl.getSlice(),
                    (FloatVectorValues) vectorValues
                );
                if (scorer.isPresent()) {
                    return scorer.get();
                }
            }
            return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
        }

        if (vectorValues instanceof HasIndexSlice sl) {
            Optional<RandomVectorScorerSupplier> scorer = switch (vectorValues.getEncoding()) {
                case BYTE -> FACTORY.getInt8VectorScorerSupplier(
                    VectorSimilarityType.of(similarityFunction),
                    sl.getSlice(),
                    (ByteVectorValues) vectorValues
                );
                case FLOAT32 -> FACTORY.getFloat32VectorScorerSupplier(
                    VectorSimilarityType.of(similarityFunction),
                    sl.getSlice(),
                    (FloatVectorValues) vectorValues
                );
            };
            if (scorer.isPresent()) {
                return scorer.get();
            }
        }
        return FALLBACK.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        float[] target
    ) throws IOException {
        if (isBFloat16(vectorValues)) {
            // can't use MemorySegment scorer for bfloat16, have to fallback to standard scorer
            // which operates on arrays, not raw MemorySegments
            Optional<RandomVectorScorer> scorer = FACTORY.getBFloat16VectorScorer(
                similarityFunction,
                (FloatVectorValues) vectorValues,
                target
            );
            if (scorer.isPresent()) {
                return scorer.get();
            }
            return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(similarityFunction, vectorValues, target);
        }

        var scorer = FACTORY.getFloat32VectorScorer(similarityFunction, (FloatVectorValues) vectorValues, target);
        if (scorer.isPresent()) {
            return scorer.get();
        }
        return FALLBACK.getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        var scorer = FACTORY.getInt8VectorScorer(similarityFunction, (ByteVectorValues) vectorValues, target);
        if (scorer.isPresent()) {
            return scorer.get();
        }
        return FALLBACK.getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    @Override
    public String toString() {
        return "ES93GenericFlatVectorScorer(delegate=" + FALLBACK + ")";
    }
}
