/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.quantizedVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.randomInt7BytesBetween;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.writeInt7VectorData;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.dotProduct;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.squareDistance;

public class VectorScorerInt7uBulkBenchmark extends VectorScorerBulkBenchmark {

    @Param({ "128", "1500", "130000" })
    public int numVectors;

    @Param
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final QuantizedByteVectorValues values;
        private final float scoreCorrectionConstant;

        private byte[] queryVector;
        private float queryVectorCorrectionConstant;

        private ScalarDotProduct(QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            this.values = values;
            this.scoreCorrectionConstant = scoreCorrectionConstant;
        }

        @Override
        public float score(int ordinal) throws IOException {
            var vec2 = values.vectorValue(ordinal);
            var vec2CorrectionConstant = values.getScoreCorrectionConstant(ordinal);
            float dotProduct = dotProduct(queryVector, vec2);
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryVectorCorrectionConstant + vec2CorrectionConstant;
            return (1 + adjustedDistance) / 2;
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int targetOrd) throws IOException {
            queryVector = values.vectorValue(targetOrd).clone();
            queryVectorCorrectionConstant = values.getScoreCorrectionConstant(targetOrd);
        }
    }

    private static class ScalarSquareDistance implements UpdateableRandomVectorScorer {
        private final QuantizedByteVectorValues values;
        private final float scoreCorrectionConstant;

        private byte[] queryVector;

        private ScalarSquareDistance(QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            this.values = values;
            this.scoreCorrectionConstant = scoreCorrectionConstant;
        }

        @Override
        public float score(int ordinal) throws IOException {
            var vec2 = values.vectorValue(ordinal);
            float squareDistance = squareDistance(queryVector, vec2);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
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
        private final byte[][] vectorData;
        private final float[] offsets;
        private final float[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            vectorData = new byte[numVectors][];
            offsets = new float[numVectors];
            for (int v = 0; v < numVectors; v++) {
                vectorData[v] = new byte[dims];
                randomInt7BytesBetween(vectorData[v]);
                offsets[v] = random.nextFloat();
            }

            queryVector = randomFloatArray(random, dims);
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            writeInt7VectorData(directory, vectorData, offsets);
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
        var values = quantizedVectorValues(dims, numVectors, in, function.function());
        float scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();

        switch (implementation) {
            case SCALAR:
                scorer = switch (function) {
                    case DOT_PRODUCT -> new ScalarDotProduct(values, scoreCorrectionConstant);
                    case EUCLIDEAN -> new ScalarSquareDistance(values, scoreCorrectionConstant);
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
                scorer = factory.getInt7SQVectorScorerSupplier(function, in, values, scoreCorrectionConstant).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getInt7SQVectorScorer(function.function(), values, ((VectorData) vectorData).queryVector)
                        .orElseThrow();
                }
                break;
        }
    }
}
