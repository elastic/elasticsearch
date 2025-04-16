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
package org.elasticsearch.index.codec.vectors.es816;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.index.codec.vectors.BQSpaceUtils;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Vector scorer over binarized vector values */
class ES816BinaryFlatRWVectorsScorer implements FlatVectorsScorer {
    private final FlatVectorsScorer nonQuantizedDelegate;

    ES816BinaryFlatRWVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
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
            BinaryQuantizer quantizer = binarizedVectors.getQuantizer();
            float[] centroid = binarizedVectors.getCentroid();
            // FIXME: precompute this once?
            int discretizedDimensions = BQVectorUtils.discretize(target.length, 64);
            if (similarityFunction == COSINE) {
                float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
                VectorUtil.l2normalize(copy);
                target = copy;
            }
            byte[] quantized = new byte[BQSpaceUtils.B_QUERY * discretizedDimensions / 8];
            BinaryQuantizer.QueryFactors factors = quantizer.quantizeForQuery(target, quantized, centroid);
            BinaryQueryVector queryVector = new BinaryQueryVector(quantized, factors);
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
        ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
        BinarizedByteVectorValues targetVectors
    ) {
        return new BinarizedRandomVectorScorerSupplier(scoringVectors, targetVectors, similarityFunction);
    }

    @Override
    public String toString() {
        return "ES816BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
    }

    /** Vector scorer supplier over binarized vector values */
    static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
        private final ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        BinarizedRandomVectorScorerSupplier(
            ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            BinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryVectors = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            byte[] queryVector = new byte[queryVectors.quantizedDimension()];
            return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(targetVectors) {
                private int ord = -1;
                private BinaryQuantizer.QueryFactors factors = null;
                private final ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors =
                    BinarizedRandomVectorScorerSupplier.this.queryVectors.copy();
                private final BinarizedByteVectorValues targetVectors = BinarizedRandomVectorScorerSupplier.this.targetVectors.copy();

                @Override
                public void setScoringOrdinal(int i) throws IOException {
                    if (i == ord) {
                        return;
                    }
                    ord = i;
                    System.arraycopy(queryVectors.vectorValue(i), 0, queryVector, 0, queryVector.length);
                    int quantizedSum = queryVectors.sumQuantizedValues(ord);
                    float distanceToCentroid = queryVectors.getCentroidDistance(ord);
                    float lower = queryVectors.getLower(ord);
                    float width = queryVectors.getWidth(ord);
                    float normVmC = 0f;
                    float vDotC = 0f;
                    if (similarityFunction != EUCLIDEAN) {
                        normVmC = queryVectors.getNormVmC(ord);
                        vDotC = queryVectors.getVDotC(ord);
                    }
                    factors = new BinaryQuantizer.QueryFactors(quantizedSum, distanceToCentroid, lower, width, normVmC, vDotC);
                }

                @Override
                public float score(int i) throws IOException {
                    if (factors == null) {
                        throw new IllegalStateException("setScoringOrdinal must be called before score");
                    }
                    return quantizedScore(this.targetVectors, i, queryVector, factors, similarityFunction);
                }
            };
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new BinarizedRandomVectorScorerSupplier(queryVectors, targetVectors, similarityFunction);
        }
    }

    /** A binarized query representing its quantized form along with factors */
    record BinaryQueryVector(byte[] vector, BinaryQuantizer.QueryFactors factors) {}

    /** Vector scorer over binarized vector values */
    static class BinarizedRandomVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final BinaryQueryVector queryVector;
        private final BinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        BinarizedRandomVectorScorer(
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
            return quantizedScore(targetVectors, targetOrd, queryVector.vector(), queryVector.factors(), similarityFunction);
        }
    }

    private static float quantizedScore(
        BinarizedByteVectorValues targetVectors,
        int targetOrd,
        byte[] quantizedQuery,
        BinaryQuantizer.QueryFactors queryFactors,
        VectorSimilarityFunction similarityFunction
    ) throws IOException {
        if (similarityFunction == EUCLIDEAN) {
            return euclideanQuantizedScore(targetVectors, targetOrd, queryFactors, quantizedQuery);
        }
        return dotProductQuantizedScore(targetVectors, targetOrd, quantizedQuery, queryFactors, similarityFunction);
    }

    private static float dotProductQuantizedScore(
        BinarizedByteVectorValues targetVectors,
        int targetOrd,
        byte[] quantizedQuery,
        BinaryQuantizer.QueryFactors queryFactors,
        VectorSimilarityFunction similarityFunction
    ) throws IOException {
        float vmC = queryFactors.normVmC();
        float vDotC = queryFactors.vDotC();
        float cDotC = targetVectors.getCentroidDP();
        byte[] binaryCode = targetVectors.vectorValue(targetOrd);
        float ooq = targetVectors.getOOQ(targetOrd);
        float normOC = targetVectors.getNormOC(targetOrd);
        float oDotC = targetVectors.getODotC(targetOrd);

        float qcDist = ESVectorUtil.ipByteBinByte(quantizedQuery, binaryCode);

        float xbSum = (float) BQVectorUtils.popcount(binaryCode);
        final float dist;
        // If ||o-c|| == 0, so, it's ok to throw the rest of the equation away
        // and simply use `oDotC + vDotC - cDotC` as centroid == doc vector
        if (normOC == 0 || ooq == 0) {
            dist = oDotC + vDotC - cDotC;
        } else {
            // If ||o-c|| != 0, we should assume that `ooq` is finite
            assert Float.isFinite(ooq);
            float estimatedDot = (2 * queryFactors.width() / targetVectors.sqrtDimensions() * qcDist + 2 * queryFactors.lower()
                / targetVectors.sqrtDimensions() * xbSum - queryFactors.width() / targetVectors.sqrtDimensions() * queryFactors
                    .quantizedSum() - targetVectors.sqrtDimensions() * queryFactors.lower()) / ooq;
            dist = vmC * normOC * estimatedDot + oDotC + vDotC - cDotC;
        }
        assert Float.isFinite(dist);

        float ooqSqr = (float) Math.pow(ooq, 2);
        float errorBound = (float) (vmC * normOC * (targetVectors.maxX1() * Math.sqrt((1 - ooqSqr) / ooqSqr)));
        float score = Float.isFinite(errorBound) ? dist - errorBound : dist;
        if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
            return VectorUtil.scaleMaxInnerProductScore(score);
        }
        return Math.max((1f + score) / 2f, 0);
    }

    private static float euclideanQuantizedScore(
        BinarizedByteVectorValues targetVectors,
        int targetOrd,
        BinaryQuantizer.QueryFactors factors,
        byte[] quantizedQuery
    ) throws IOException {
        byte[] binaryCode = targetVectors.vectorValue(targetOrd);

        float targetDistToC = targetVectors.getCentroidDistance(targetOrd);
        float x0 = targetVectors.getVectorMagnitude(targetOrd);
        float sqrX = targetDistToC * targetDistToC;
        double xX0 = targetDistToC / x0;

        float xbSum = (float) BQVectorUtils.popcount(binaryCode);
        float factorPPC = (float) (-2.0 / targetVectors.sqrtDimensions() * xX0 * (xbSum * 2.0 - targetVectors.dimension()));
        float factorIP = (float) (-2.0 / targetVectors.sqrtDimensions() * xX0);

        long qcDist = ESVectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
        float score = sqrX + factors.distToC() + factorPPC * factors.lower() + (qcDist * 2 - factors.quantizedSum()) * factorIP * factors
            .width();
        float projectionDist = (float) Math.sqrt(xX0 * xX0 - targetDistToC * targetDistToC);
        float error = 2.0f * targetVectors.maxX1() * projectionDist;
        float y = (float) Math.sqrt(factors.distToC());
        float errorBound = y * error;
        if (Float.isFinite(errorBound)) {
            score = score + errorBound;
        }
        return Math.max(1 / (1f + score), 0);
    }

}
