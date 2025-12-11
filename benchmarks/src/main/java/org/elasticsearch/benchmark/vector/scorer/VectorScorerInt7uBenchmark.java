/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.createRandomInt7VectorData;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.readNodeCorrectionConstant;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.vectorValues;

/**
 * Benchmark that compares various scalar quantized vector similarity function
 * implementations: scalar, lucene's panama-ized, and Elasticsearch's native.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt7uBenchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class VectorScorerInt7uBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
        if (supportsHeapSegments() == false) {
            final Logger LOG = LogManager.getLogger(VectorScorerInt7uBenchmark.class);
            LOG.warn("*Query targets cannot run on " + "JDK " + Runtime.version());
        }
    }

    @Param({ "96", "768", "1024" })
    public int dims;
    public int numVectors = 2; // there are only two vectors to compare

    public enum Implementation {
        SCALAR,
        LUCENE,
        NATIVE
    }

    @Param
    public Implementation implementation;

    public enum Function {
        DOT_PRODUCT(VectorSimilarityType.DOT_PRODUCT),
        SQUARE_DISTANCE(VectorSimilarityType.EUCLIDEAN);

        private final VectorSimilarityType type;

        Function(VectorSimilarityType type) {
            this.type = type;
        }

        public VectorSimilarityType type() {
            return type;
        }

        public VectorSimilarityFunction function() {
            return VectorSimilarityType.of(type);
        }
    }

    @Param
    public Function function;

    private Path path;
    private Directory dir;
    private IndexInput in;

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final byte[] vec1;
        private final byte[] vec2;
        private final float vec1CorrectionConstant;
        private final float vec2CorrectionConstant;
        private final float scoreCorrectionConstant;

        private ScalarDotProduct(
            byte[] vec1,
            byte[] vec2,
            float vec1CorrectionConstant,
            float vec2CorrectionConstant,
            float scoreCorrectionConstant
        ) {
            this.vec1 = vec1;
            this.vec2 = vec2;
            this.vec1CorrectionConstant = vec1CorrectionConstant;
            this.vec2CorrectionConstant = vec2CorrectionConstant;
            this.scoreCorrectionConstant = scoreCorrectionConstant;
        }

        @Override
        public float score(int node) throws IOException {
            int dotProduct = 0;
            for (int i = 0; i < vec1.length; i++) {
                dotProduct += vec1[i] * vec2[i];
            }
            float adjustedDistance = dotProduct * scoreCorrectionConstant + vec1CorrectionConstant + vec2CorrectionConstant;
            return (1 + adjustedDistance) / 2;
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {}
    }

    private static class ScalarSquareDistance implements UpdateableRandomVectorScorer {
        private final byte[] vec1;
        private final byte[] vec2;
        private final float scoreCorrectionConstant;

        private ScalarSquareDistance(byte[] vec1, byte[] vec2, float scoreCorrectionConstant) {
            this.vec1 = vec1;
            this.vec2 = vec2;
            this.scoreCorrectionConstant = scoreCorrectionConstant;
        }

        @Override
        public float score(int node) throws IOException {
            int squareDistance = 0;
            for (int i = 0; i < vec1.length; i++) {
                int diff = vec1[i] - vec2[i];
                squareDistance += diff * diff;
            }
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }

        @Override
        public int maxOrd() {
            return 0;
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {}
    }

    UpdateableRandomVectorScorer scorer;
    RandomVectorScorer queryScorer;

    @Setup
    public void setup() throws IOException {
        VectorScorerFactory factory = getScorerFactoryOrDie();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        path = Files.createTempDirectory("Int7uScorerBenchmark");
        dir = new MMapDirectory(path);
        createRandomInt7VectorData(random, dir, dims, numVectors);

        in = dir.openInput("vector.data", IOContext.DEFAULT);
        var values = vectorValues(dims, numVectors, in, function.function());
        float scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();

        float[] queryVec = new float[dims];
        for (int i = 0; i < dims; i++) {
            queryVec[i] = random.nextFloat();
        }

        switch (implementation) {
            case SCALAR:
                byte[] vec1 = values.vectorValue(0).clone();
                float vec1CorrectionConstant = readNodeCorrectionConstant(values, 0);
                byte[] vec2 = values.vectorValue(1).clone();
                float vec2CorrectionConstant = readNodeCorrectionConstant(values, 1);

                scorer = switch (function) {
                    case DOT_PRODUCT -> new ScalarDotProduct(
                        vec1,
                        vec2,
                        vec1CorrectionConstant,
                        vec2CorrectionConstant,
                        scoreCorrectionConstant
                    );
                    case SQUARE_DISTANCE -> new ScalarSquareDistance(vec1, vec2, scoreCorrectionConstant);
                };

                if (supportsHeapSegments()) {
                    // only run this if there's something to compare it against
                    queryScorer = scorer;
                }
                break;
            case LUCENE:
                scorer = luceneScoreSupplier(values, function.function()).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = luceneScorer(values, function.function(), queryVec);
                }
                break;
            case NATIVE:
                scorer = factory.getInt7SQVectorScorerSupplier(function.type(), in, values, scoreCorrectionConstant).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getInt7SQVectorScorer(function.function(), values, queryVec).orElseThrow();
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
}
