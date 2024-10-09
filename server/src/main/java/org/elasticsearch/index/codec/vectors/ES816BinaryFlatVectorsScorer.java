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
package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Vector scorer over binarized vector values */
public class ES816BinaryFlatVectorsScorer implements FlatVectorsScorer {
    private final FlatVectorsScorer nonQuantizedDelegate;

    public ES816BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
        this.nonQuantizedDelegate = nonQuantizedDelegate;
    }

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        RandomAccessVectorValues vectorValues
    ) throws IOException {
        if (vectorValues instanceof RandomAccessBinarizedByteVectorValues) {
            throw new UnsupportedOperationException(
                "getRandomVectorScorerSupplier(VectorSimilarityFunction,RandomAccessVectorValues) not implemented for binarized format"
            );
        }
        return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        RandomAccessVectorValues vectorValues,
        float[] target
    ) throws IOException {
        if (vectorValues instanceof RandomAccessBinarizedByteVectorValues binarizedVectors) {
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
        RandomAccessVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
    }

    RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
        RandomAccessBinarizedByteVectorValues targetVectors
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
        private final RandomAccessBinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        BinarizedRandomVectorScorerSupplier(
            ES816BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            RandomAccessBinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryVectors = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public RandomVectorScorer scorer(int ord) throws IOException {
            byte[] vector = queryVectors.vectorValue(ord);
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
            BinaryQueryVector binaryQueryVector = new BinaryQueryVector(
                vector,
                new BinaryQuantizer.QueryFactors(quantizedSum, distanceToCentroid, lower, width, normVmC, vDotC)
            );
            return new BinarizedRandomVectorScorer(binaryQueryVector, targetVectors, similarityFunction);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new BinarizedRandomVectorScorerSupplier(queryVectors.copy(), targetVectors.copy(), similarityFunction);
        }
    }

    /** A binarized query representing its quantized form along with factors */
    public record BinaryQueryVector(byte[] vector, BinaryQuantizer.QueryFactors factors) {}

    /** Vector scorer over binarized vector values */
    public static class BinarizedRandomVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        private final BinaryQueryVector queryVector;
        private final RandomAccessBinarizedByteVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        private final float sqrtDimensions;

        public BinarizedRandomVectorScorer(
            BinaryQueryVector queryVectors,
            RandomAccessBinarizedByteVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            super(targetVectors);
            this.queryVector = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
            // FIXME: precompute this once?
            this.sqrtDimensions = (float) Utils.constSqrt(targetVectors.dimension());
        }

        // FIXME: utils class; pull this out
        private static class Utils {
            public static double sqrtNewtonRaphson(double x, double curr, double prev) {
                return (curr == prev) ? curr : sqrtNewtonRaphson(x, 0.5 * (curr + x / curr), curr);
            }

            public static double constSqrt(double x) {
                return x >= 0 && Double.isInfinite(x) == false ? sqrtNewtonRaphson(x, x, 0) : Double.NaN;
            }
        }

        @Override
        public float score(int targetOrd) throws IOException {
            // FIXME: implement fastscan in the future?

            byte[] quantizedQuery = queryVector.vector();
            int quantizedSum = queryVector.factors().quantizedSum();
            float lower = queryVector.factors().lower();
            float width = queryVector.factors().width();
            float distanceToCentroid = queryVector.factors().distToC();
            if (similarityFunction == EUCLIDEAN) {
                return euclideanScore(targetOrd, sqrtDimensions, quantizedQuery, distanceToCentroid, lower, quantizedSum, width);
            }

            float vmC = queryVector.factors().normVmC();
            float vDotC = queryVector.factors().vDotC();
            float cDotC = targetVectors.getCentroidDP();
            byte[] binaryCode = targetVectors.vectorValue(targetOrd);
            float ooq = targetVectors.getOOQ(targetOrd);
            float normOC = targetVectors.getNormOC(targetOrd);
            float oDotC = targetVectors.getODotC(targetOrd);

            float qcDist = ESVectorUtil.ipByteBinByte(quantizedQuery, binaryCode);

            // FIXME: pre-compute these only once for each target vector
            // ... pull this out or use a similar cache mechanism as do in score
            float xbSum = (float) BQVectorUtils.popcount(binaryCode);
            final float dist;
            // If ||o-c|| == 0, so, it's ok to throw the rest of the equation away
            // and simply use `oDotC + vDotC - cDotC` as centroid == doc vector
            if (normOC == 0 || ooq == 0) {
                dist = oDotC + vDotC - cDotC;
            } else {
                // If ||o-c|| != 0, we should assume that `ooq` is finite
                assert Float.isFinite(ooq);
                float estimatedDot = (2 * width / sqrtDimensions * qcDist + 2 * lower / sqrtDimensions * xbSum - width / sqrtDimensions
                    * quantizedSum - sqrtDimensions * lower) / ooq;
                dist = vmC * normOC * estimatedDot + oDotC + vDotC - cDotC;
            }
            assert Float.isFinite(dist);

            // TODO: this is useful for mandatory rescoring by accounting for bias
            // However, for just oversampling & rescoring, it isn't strictly useful.
            // We should consider utilizing this bias in the future to determine which vectors need to
            // be rescored
            // float ooqSqr = (float) Math.pow(ooq, 2);
            // float errorBound = (float) (normVmC * normOC * (maxX1 * Math.sqrt((1 - ooqSqr) / ooqSqr)));
            // float score = dist - errorBound;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(dist);
            }
            return Math.max((1f + dist) / 2f, 0);
        }

        private float euclideanScore(
            int targetOrd,
            float sqrtDimensions,
            byte[] quantizedQuery,
            float distanceToCentroid,
            float lower,
            int quantizedSum,
            float width
        ) throws IOException {
            byte[] binaryCode = targetVectors.vectorValue(targetOrd);

            // FIXME: pre-compute these only once for each target vector
            // .. not sure how to enumerate the target ordinals but that's what we did in PoC
            float targetDistToC = targetVectors.getCentroidDistance(targetOrd);
            float x0 = targetVectors.getVectorMagnitude(targetOrd);
            float sqrX = targetDistToC * targetDistToC;
            double xX0 = targetDistToC / x0;

            // TODO maybe store?
            float xbSum = (float) BQVectorUtils.popcount(binaryCode);
            float factorPPC = (float) (-2.0 / sqrtDimensions * xX0 * (xbSum * 2.0 - targetVectors.dimension()));
            float factorIP = (float) (-2.0 / sqrtDimensions * xX0);

            long qcDist = ESVectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
            float score = sqrX + distanceToCentroid + factorPPC * lower + (qcDist * 2 - quantizedSum) * factorIP * width;
            // TODO: this is useful for mandatory rescoring by accounting for bias
            // However, for just oversampling & rescoring, it isn't strictly useful.
            // We should consider utilizing this bias in the future to determine which vectors need to
            // be rescored
            // float projectionDist = (float) Math.sqrt(xX0 * xX0 - targetDistToC * targetDistToC);
            // float error = 2.0f * maxX1 * projectionDist;
            // float y = (float) Math.sqrt(distanceToCentroid);
            // float errorBound = y * error;
            // if (Float.isFinite(errorBound)) {
            // score = dist + errorBound;
            // }
            return Math.max(1 / (1f + score), 0);
        }
    }
}
