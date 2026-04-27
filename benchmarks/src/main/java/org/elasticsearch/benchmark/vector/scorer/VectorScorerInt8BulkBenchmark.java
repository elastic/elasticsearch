/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.nativeaccess.jdk.ScalarOperations;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.byteVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.writeByteVectorData;

public class VectorScorerInt8BulkBenchmark extends VectorScorerBulkBenchmark {

    // 128kb is typically enough to not fit in L1 (core) cache for most processors;
    // 1.5Mb is typically enough to not fit in L2 (core) cache;
    // 130Mb is enough to not fit in L3 cache
    @Param({ "128", "1500", "130000" })
    public int numVectors;

    @Param
    public VectorImplementation implementation;

    @Param({ "COSINE", "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private static class ScalarCosine implements UpdateableRandomVectorScorer {
        private final ByteVectorValues values;

        private byte[] queryVector;

        private ScalarCosine(ByteVectorValues values) {
            this.values = values;
        }

        @Override
        public float score(int ordinal) throws IOException {
            return normalize(ScalarOperations.cosine(queryVector, values.vectorValue(ordinal)));
        }

        private float normalize(float cosine) {
            return (1 + cosine) / 2;
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

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final ByteVectorValues values;

        private byte[] queryVector;
        private float denom;

        private ScalarDotProduct(ByteVectorValues values) {
            this.values = values;
        }

        @Override
        public float score(int ordinal) throws IOException {
            return normalize(ScalarOperations.dotProduct(queryVector, values.vectorValue(ordinal)));
        }

        private float normalize(float dotProduct) {
            return 0.5f + dotProduct / denom;
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int targetOrd) throws IOException {
            queryVector = values.vectorValue(targetOrd).clone();
            // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
            denom = (float) (queryVector.length * (1 << 15));
        }
    }

    private static class ScalarSquareDistance implements UpdateableRandomVectorScorer {
        private final ByteVectorValues values;

        private byte[] queryVector;

        private ScalarSquareDistance(ByteVectorValues values) {
            this.values = values;
        }

        @Override
        public float score(int ordinal) throws IOException {
            return VectorUtil.normalizeDistanceToUnitInterval(ScalarOperations.squareDistance(queryVector, values.vectorValue(ordinal)));
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
        private final byte[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            vectorData = new byte[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                vectorData[v] = new byte[dims];
                random.nextBytes(vectorData[v]);
            }

            queryVector = new byte[dims];
            random.nextBytes(queryVector);
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            writeByteVectorData(directory, vectorData);
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
        var values = byteVectorValues(dims, numVectors, in, function.function());

        switch (implementation) {
            case SCALAR:
                scorer = switch (function) {
                    case COSINE -> new ScalarCosine(values);
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
                scorer = factory.getInt8VectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getInt8VectorScorer(function.function(), values, ((VectorData) vectorData).queryVector)
                        .orElseThrow();
                }
                break;
        }
    }
}
