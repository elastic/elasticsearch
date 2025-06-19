/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es910;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Vector scorer over binarized vector values */
public class ES910BinaryFlatVectorsScorer implements FlatVectorsScorer {
    private final FlatVectorsScorer nonQuantizedDelegate;
    private final byte queryBits;

    public ES910BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate, byte queryBits) {
        this.nonQuantizedDelegate = nonQuantizedDelegate;
        this.queryBits = queryBits;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues
    ) throws IOException {
        if (vectorValues instanceof BinarizedByteVectorValues) {
            throw new UnsupportedOperationException(
                "getRandomVectorScorerSupplier(VectorSimilarityFunction,RandomAccessVectorValues) not implemented for binarized format"
            );
        }
        return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        float[] target
    ) throws IOException {
        if (vectorValues instanceof BinarizedByteVectorValues binarizedVectors) {
            OptimizedScalarQuantizer quantizer = binarizedVectors.getQuantizer();
            float[] centroid = binarizedVectors.getCentroid();
            // We make a copy as the quantization process mutates the input
            float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
            if (similarityFunction == COSINE) {
                VectorUtil.l2normalize(copy);
            }
            target = copy;
            byte[] quantized = new byte[target.length];
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(target, quantized, queryBits, centroid);
            return new RandomVectorScorer.AbstractRandomVectorScorer(vectorValues) {
                @Override
                public float score(int i) throws IOException {
                    return quantizedScore(
                        binarizedVectors.dimension(),
                        similarityFunction,
                        binarizedVectors.getCentroidDP(),
                        quantized,
                        queryCorrections,
                        binarizedVectors.vectorValue(i),
                        binarizedVectors.getCorrectiveTerms(i),
                        getBitsScale()
                    );
                }
            };
        }
        return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    private float getBitsScale() {
        return 1f / ((1 << queryBits) - 1);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        ES910BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
        BinarizedByteVectorValues targetVectors
    ) {
        return new BinarizedRandomVectorScorerSupplier(scoringVectors, targetVectors, similarityFunction, queryBits);
    }

    @Override
    public String toString() {
        return "ES910BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ", queryBits = " + queryBits + ")";
    }

    /** Vector scorer supplier over binarized vector values */
    static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
        private final ES910BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;
        private final byte queryBits;

        BinarizedRandomVectorScorerSupplier(
            ES910BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            BinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction,
            byte queryBits
        ) {
            this.queryVectors = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
            this.queryBits = queryBits;
        }

        @Override
        public BinarizedRandomVectorScorer scorer() throws IOException {
            return new BinarizedRandomVectorScorer(queryVectors.copy(), targetVectors.copy(), similarityFunction, queryBits);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new BinarizedRandomVectorScorerSupplier(queryVectors, targetVectors, similarityFunction, queryBits);
        }
    }

    /** Vector scorer over binarized vector values */
    public static class BinarizedRandomVectorScorer extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
        private final ES910BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;
        private final byte[] quantizedQuery;
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections = null;
        private int currentOrdinal = -1;
        private final float bitScale;

        BinarizedRandomVectorScorer(
            ES910BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            BinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction,
            byte queryBits
        ) {
            super(targetVectors);
            this.queryVectors = queryVectors;
            this.quantizedQuery = new byte[queryVectors.quantizedDimension()];
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
            bitScale = 1.0F / (float) ((1 << queryBits) - 1);
        }

        @Override
        public float score(int targetOrd) throws IOException {
            if (queryCorrections == null) {
                throw new IllegalStateException("score() called before setScoringOrdinal()");
            }
            return quantizedScore(
                targetVectors.dimension(),
                similarityFunction,
                targetVectors.getCentroidDP(),
                quantizedQuery,
                queryCorrections,
                targetVectors.vectorValue(targetOrd),
                targetVectors.getCorrectiveTerms(targetOrd),
                bitScale
            );
        }

        @Override
        public void setScoringOrdinal(int i) throws IOException {
            if (i == currentOrdinal) {
                return;
            }
            System.arraycopy(queryVectors.vectorValue(i), 0, quantizedQuery, 0, quantizedQuery.length);
            queryCorrections = queryVectors.getCorrectiveTerms(i);
            currentOrdinal = i;
        }
    }

    private static float quantizedScore(
        int dims,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        byte[] q,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        byte[] d,
        OptimizedScalarQuantizer.QuantizationResult indexCorrections,
        float bitsScale
    ) {
        float qcDist = VectorUtil.dotProduct(q, d);
        float x1 = indexCorrections.quantizedComponentSum();
        float ax = indexCorrections.lowerInterval();
        // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
        float lx = indexCorrections.upperInterval() - ax;
        float ay = queryCorrections.lowerInterval();
        float ly = (queryCorrections.upperInterval() - ay) * bitsScale;
        float y1 = queryCorrections.quantizedComponentSum();
        float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        if (similarityFunction == EUCLIDEAN) {
            score = queryCorrections.additionalCorrection() + indexCorrections.additionalCorrection() - 2 * score;
            return Math.max(1 / (1f + score), 0);
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryCorrections.additionalCorrection() + indexCorrections.additionalCorrection() - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return Math.max((1f + score) / 2f, 0);
        }
    }
}
