/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.floatVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.writeFloatVectorData;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.dotProduct;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.squareDistance;

public class VectorScorerFloat32BulkBenchmark extends VectorScorerBulkBenchmark {

    @Param({ "32", "375", "32500" })
    public int numVectors;

    @Param
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final FloatVectorValues values;

        private float[] queryVector;

        private ScalarDotProduct(FloatVectorValues values) {
            this.values = values;
        }

        @Override
        public float score(int ordinal) throws IOException {
            return VectorUtil.normalizeToUnitInterval(dotProduct(queryVector, values.vectorValue(ordinal)));
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int targetOrd) throws IOException {
            queryVector = values.vectorValue(targetOrd).clone();
        }
    }

    private static class ScalarSquareDistance implements UpdateableRandomVectorScorer {
        private final FloatVectorValues values;

        private float[] queryVector;

        private ScalarSquareDistance(FloatVectorValues values) {
            this.values = values;
        }

        @Override
        public float score(int ordinal) throws IOException {
            return VectorUtil.normalizeDistanceToUnitInterval(squareDistance(queryVector, values.vectorValue(ordinal)));
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int targetOrd) throws IOException {
            queryVector = values.vectorValue(targetOrd).clone();
        }
    }

    static class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final float[][] vectorData;
        private final float[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            vectorData = new float[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                vectorData[v] = randomFloatArray(random, dims);
            }

            queryVector = randomFloatArray(random, dims);
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            writeFloatVectorData(directory, vectorData);
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) throws IOException {
        setup(vectorData, numVectors);
    }

    @Override
    void createScorers(IndexInput in, VectorScorerBulkBenchmark.VectorData vectorData) throws IOException {
        VectorScorerFactory factory = getScorerFactoryOrDie();
        var values = floatVectorValues(dims, numVectors, in, function.function());

        switch (implementation) {
            case SCALAR:
                scorer = switch (function) {
                    case DOT_PRODUCT -> new ScalarDotProduct(values);
                    case EUCLIDEAN -> new ScalarSquareDistance(values);
                    default -> throw new IllegalArgumentException(function + " not supported");
                };
                break;
            case LUCENE:
                scorer = luceneScoreSupplier(values, function.function()).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = luceneScorer(values, function.function(), ((VectorData) vectorData).queryVector);
                }
                break;
            case NATIVE:
                scorer = factory.getFloat32VectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getFloat32VectorScorer(function.function(), values, ((VectorData) vectorData).queryVector)
                        .orElseThrow();
                }
                break;
        }
    }
}
