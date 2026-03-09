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
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
import org.elasticsearch.simdvec.ES93BinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

/** Vector scorer over binarized vector values */
public class ES818BinaryFlatVectorsScorer implements FlatVectorsScorer {
    private final FlatVectorsScorer nonQuantizedDelegate;

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
        if (vectorValues instanceof OffHeapBinarizedVectorValues binarizedVectors) {
            assert binarizedVectors.getQuantizer() != null
                : "BinarizedByteVectorValues must have a quantizer for ES816BinaryFlatVectorsScorer";
            assert binarizedVectors.size() > 0 : "BinarizedByteVectorValues must have at least one vector for ES816BinaryFlatVectorsScorer";
            OptimizedScalarQuantizer quantizer = binarizedVectors.getQuantizer();
            float[] centroid = binarizedVectors.getCentroid();
            assert similarityFunction != COSINE || BQVectorUtils.isUnitVector(target);
            float[] scratch = new float[vectorValues.dimension()];
            int[] initial = new int[target.length];
            byte[] quantized = new byte[BinaryQuantizer.B_QUERY * binarizedVectors.discretizedDimensions() / 8];
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                target,
                scratch,
                initial,
                (byte) 4,
                centroid
            );
            ESVectorUtil.transposeHalfByte(initial, quantized);
            var scorer = ESVectorUtil.getES93BinaryQuantizedVectorScorer(
                binarizedVectors.slice,
                binarizedVectors.dimension(),
                binarizedVectors.getVectorByteLength()
            );
            return new RandomVectorScorer.AbstractRandomVectorScorer(vectorValues) {
                @Override
                public float score(int i) throws IOException {
                    return scorer.score(
                        quantized,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        binarizedVectors.getCentroidDP(),
                        i
                    );
                }

                @Override
                public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                    scorer.scoreBulk(
                        quantized,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        binarizedVectors.getCentroidDP(),
                        nodes,
                        scores,
                        numNodes
                    );
                }
            };
        }
        assert vectorValues instanceof BinarizedByteVectorValues == false
            : "BinarizedByteVectorValues must be off-heap (implement OffHeapBinarizedVectorValues)";

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
        OffHeapBinarizedVectorValues targetVectors
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
        private final OffHeapBinarizedVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;

        BinarizedRandomVectorScorerSupplier(
            ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            OffHeapBinarizedVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryVectors = queryVectors;
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public BinarizedRandomVectorScorer scorer() throws IOException {
            return new BinarizedRandomVectorScorer(queryVectors.copy(), targetVectors.copy(), similarityFunction);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new BinarizedRandomVectorScorerSupplier(queryVectors, targetVectors, similarityFunction);
        }
    }

    /** Vector scorer over binarized vector values */
    public static class BinarizedRandomVectorScorer extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
        private final ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors;
        private final OffHeapBinarizedVectorValues targetVectors;
        private final VectorSimilarityFunction similarityFunction;
        private final byte[] quantizedQuery;
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections = null;
        private int currentOrdinal = -1;

        private final ES93BinaryQuantizedVectorScorer scorer;

        BinarizedRandomVectorScorer(
            ES818BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
            OffHeapBinarizedVectorValues targetVectors,
            VectorSimilarityFunction similarityFunction
        ) throws IOException {
            super(targetVectors);
            this.queryVectors = queryVectors;
            this.quantizedQuery = new byte[queryVectors.quantizedDimension()];
            this.targetVectors = targetVectors;
            this.similarityFunction = similarityFunction;
            this.scorer = ESVectorUtil.getES93BinaryQuantizedVectorScorer(
                targetVectors.slice,
                targetVectors.dimension(),
                targetVectors.getVectorByteLength()
            );
        }

        @Override
        public float score(int targetOrd) throws IOException {
            if (queryCorrections == null) {
                throw new IllegalStateException("score() called before setScoringOrdinal()");
            }
            return scorer.score(
                quantizedQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                targetVectors.getCentroidDP(),
                targetOrd
            );
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (queryCorrections == null) {
                throw new IllegalStateException("bulkScore() called before setScoringOrdinal()");
            }
            scorer.scoreBulk(
                quantizedQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                targetVectors.getCentroidDP(),
                nodes,
                scores,
                numNodes
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
}
