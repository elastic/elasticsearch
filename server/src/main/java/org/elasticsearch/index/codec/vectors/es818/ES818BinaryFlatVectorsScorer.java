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
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.index.codec.vectors.BQSpaceUtils;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Vector scorer over binarized vector values */
public class ES818BinaryFlatVectorsScorer implements FlatVectorsScorer {
    private final FlatVectorsScorer nonQuantizedDelegate;
    private static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    public ES818BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
        this.nonQuantizedDelegate = nonQuantizedDelegate;
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
            byte[] initial = new byte[target.length];
            byte[] quantized = new byte[BQSpaceUtils.B_QUERY * binarizedVectors.discretizedDimensions() / 8];
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(target, initial, (byte) 4, centroid);
            BQSpaceUtils.transposeHalfByte(initial, quantized);
            BinaryQueryVector queryVector = new BinaryQueryVector(quantized, queryCorrections);
            return new BinarizedRandomVectorScorer(queryVector, binarizedVectors, similarityFunction);
        }
        return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
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
        ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
        BinarizedByteVectorValues targetVectors
    ) {
        return new BinarizedRandomVectorScorerSupplier(scoringVectors, targetVectors, similarityFunction);
    }

    @Override
    public String toString() {
        return "ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
    }

    /** Vector scorer supplier over binarized vector values */
    static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
        private final ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        BinarizedRandomVectorScorerSupplier(
            ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            BinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryVectors = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public RandomVectorScorer scorer(int ord) throws IOException {
            byte[] vector = queryVectors.vectorValue(ord);
            OptimizedScalarQuantizer.QuantizationResult correctiveTerms = queryVectors.getCorrectiveTerms(ord);
            BinaryQueryVector binaryQueryVector = new BinaryQueryVector(vector, correctiveTerms);
            return new BinarizedRandomVectorScorer(binaryQueryVector, targetVectors, similarityFunction);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new BinarizedRandomVectorScorerSupplier(queryVectors.copy(), targetVectors.copy(), similarityFunction);
        }
    }

    /** A binarized query representing its quantized form along with factors */
    public record BinaryQueryVector(byte[] vector, OptimizedScalarQuantizer.QuantizationResult quantizationResult) {}

    /** Vector scorer over binarized vector values */
    public static class BinarizedRandomVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final BinaryQueryVector queryVector;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        public BinarizedRandomVectorScorer(
            BinaryQueryVector queryVectors,
            BinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            super(targetVectors);
            this.queryVector = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public float score(int targetOrd) throws IOException {
            byte[] quantizedQuery = queryVector.vector();
            byte[] binaryCode = targetVectors.vectorValue(targetOrd);
            float qcDist = ESVectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = queryVector.quantizationResult();
            OptimizedScalarQuantizer.QuantizationResult indexCorrections = targetVectors.getCorrectiveTerms(targetOrd);
            float x1 = indexCorrections.quantizedComponentSum();
            float ax = indexCorrections.lowerInterval();
            // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
            float lx = indexCorrections.upperInterval() - ax;
            float ay = queryCorrections.lowerInterval();
            float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
            float y1 = queryCorrections.quantizedComponentSum();
            float score = ax * ay * targetVectors.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                score = queryCorrections.additionalCorrection() + indexCorrections.additionalCorrection() - 2 * score;
                return Math.max(1 / (1f + score), 0);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                score += queryCorrections.additionalCorrection() + indexCorrections.additionalCorrection() - targetVectors.getCentroidDP();
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    return VectorUtil.scaleMaxInnerProductScore(score);
                }
                return Math.max((1f + score) / 2f, 0);
            }
        }
    }
}
