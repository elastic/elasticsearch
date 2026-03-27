/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.byteVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.writeByteVectorData;

/**
 * Benchmark that compares various byte vector similarity function
 * implementations: scalar, lucene's panama-ized, and Elasticsearch's native.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerByteBenchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class VectorScorerByteBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
        if (supportsHeapSegments() == false) {
            final Logger LOG = LogManager.getLogger(VectorScorerByteBenchmark.class);
            LOG.warn("*Query targets cannot run on " + "JDK " + Runtime.version());
        }
    }

    @Param({ "96", "768", "1024" })
    public int dims;
    public static int numVectors = 2;

    @Param
    public VectorImplementation implementation;

    @Param({ "COSINE", "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private Path path;
    private Directory dir;
    private IndexInput in;

    private static class ScalarCosine implements UpdateableRandomVectorScorer {
        private final byte[] vec1;
        private final byte[] vec2;

        private ScalarCosine(byte[] vec1, byte[] vec2) {
            this.vec1 = vec1;
            this.vec2 = vec2;
        }

        @Override
        public float score(int node) {
            return (1 + cosine(vec1, vec2)) / 2;
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int node) {}
    }

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final byte[] vec1;
        private final byte[] vec2;

        private ScalarDotProduct(byte[] vec1, byte[] vec2) {
            this.vec1 = vec1;
            this.vec2 = vec2;
        }

        @Override
        public float score(int node) {
            // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
            float denom = (float) (vec1.length * (1 << 15));
            return 0.5f + dotProduct(vec1, vec2) / denom;
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int node) {}
    }

    private static class ScalarSquareDistance implements UpdateableRandomVectorScorer {
        private final byte[] vec1;
        private final byte[] vec2;

        private ScalarSquareDistance(byte[] vec1, byte[] vec2) {
            this.vec1 = vec1;
            this.vec2 = vec2;
        }

        @Override
        public float score(int node) {
            return VectorUtil.normalizeDistanceToUnitInterval(squareDistance(vec1, vec2));
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int node) {}
    }

    UpdateableRandomVectorScorer scorer;
    RandomVectorScorer queryScorer;

    public static class VectorData {
        private final byte[][] vectorData;
        private final byte[] queryVector;

        public VectorData(int dims) {
            vectorData = new byte[numVectors][];
            ThreadLocalRandom random = ThreadLocalRandom.current();

            for (int v = 0; v < numVectors; v++) {
                vectorData[v] = new byte[dims];
                random.nextBytes(vectorData[v]);
            }

            queryVector = new byte[dims];
            random.nextBytes(queryVector);
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims));
    }

    public void setup(VectorData vectorData) throws IOException {
        VectorScorerFactory factory = getScorerFactoryOrDie();

        path = Files.createTempDirectory("ByteScorerBenchmark");
        dir = new MMapDirectory(path);
        writeByteVectorData(dir, vectorData.vectorData);

        in = dir.openInput("vector.data", IOContext.DEFAULT);
        var values = byteVectorValues(dims, numVectors, in, function.function());

        switch (implementation) {
            case SCALAR:
                byte[] vec1 = values.vectorValue(0).clone();
                byte[] vec2 = values.vectorValue(1).clone();

                scorer = switch (function) {
                    case COSINE -> new ScalarCosine(vec1, vec2);
                    case DOT_PRODUCT -> new ScalarDotProduct(vec1, vec2);
                    case EUCLIDEAN -> new ScalarSquareDistance(vec1, vec2);
                    default -> throw new IllegalArgumentException(function + " not supported");
                };
                break;
            case LUCENE:
                scorer = luceneScoreSupplier(values, function.function()).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = luceneScorer(values, function.function(), vectorData.queryVector);
                }
                break;
            case NATIVE:
                scorer = factory.getByteVectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getByteVectorScorer(function.function(), values, vectorData.queryVector).orElseThrow();
                }
                break;
        }

        scorer.setScoringOrdinal(0);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(dir, in);
        IOUtils.rm(path);
    }

    @Benchmark
    public float score() throws IOException {
        return scorer.score(1);
    }

    @Benchmark
    public float scoreQuery() throws IOException {
        return queryScorer.score(1);
    }

    static float cosine(byte[] a, byte[] b) {
        int sum = 0;
        int norm1 = 0;
        int norm2 = 0;

        for (int i = 0; i < a.length; i++) {
            byte elem1 = a[i];
            byte elem2 = b[i];
            sum += elem1 * elem2;
            norm1 += elem1 * elem1;
            norm2 += elem2 * elem2;
        }
        return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
    }

    static float dotProduct(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static float squareDistance(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            int d = a[i] - b[i];
            res += d * d;
        }
        return res;
    }
}
