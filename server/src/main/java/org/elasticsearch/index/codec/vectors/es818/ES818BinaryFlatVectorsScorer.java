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
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
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
                public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                    return scorer.scoreBulk(
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

    @Override
    public String toString() {
        return "ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
    }
}
