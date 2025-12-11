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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.randomInt7BytesBetween;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.readNodeCorrectionConstant;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.vectorValues;

/**
 * Benchmark that compares bulk scoring of various scalar quantized vector similarity function
 * implementations (scalar, lucene's panama-ized, and Elasticsearch's native) against sequential
 * and random access target vectors.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt7uBulkScorerBenchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt7uBulkBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
        if (supportsHeapSegments() == false) {
            final Logger LOG = LogManager.getLogger(VectorScorerInt7uBulkBenchmark.class);
            LOG.warn("*Query targets cannot run on " + "JDK " + Runtime.version());
        }
    }

    @Param({ "1024" })
    public int dims;

    // 128k is typically enough to not fit in L1 (core) cache for most processors;
    // 1.5M is typically enough to not fit in L2 (core) cache;
    // 130M is enough to not fit in L3 cache
    @Param({ "128", "1500", "130000" })
    public int numVectors;
    public int numVectorsToScore;

    public enum Implementation {
        SCALAR,
        LUCENE,
        NATIVE
    }

    @Param
    public Implementation implementation;

    public enum Function {
        DOT_PRODUCT(VectorSimilarityType.DOT_PRODUCT);

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
            var vec2CorrectionConstant = readNodeCorrectionConstant(values, ordinal);
            int dotProduct = 0;
            for (int i = 0; i < queryVector.length; i++) {
                dotProduct += queryVector[i] * vec2[i];
            }
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
            queryVectorCorrectionConstant = readNodeCorrectionConstant(values, targetOrd);
        }
    }

    float[] scores;
    int[] ordinals;
    int[] ids;
    int targetOrd;

    UpdateableRandomVectorScorer scorer;
    RandomVectorScorer queryScorer;

    public static class VectorData {
        private final int numVectorsToScore;
        private final byte[][] vectorData;
        private final float[] offsets;
        private final int[] ordinals;
        private final int targetOrd;
        private final float[] queryVector;

        public VectorData(int dims, int numVectors, int numVectorsToScore) {
            this.numVectorsToScore = numVectorsToScore;
            vectorData = new byte[numVectors][];
            offsets = new float[numVectors];

            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int v = 0; v < numVectors; v++) {
                vectorData[v] = new byte[dims];
                randomInt7BytesBetween(vectorData[v]);
                offsets[v] = random.nextFloat();
            }

            List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
            Collections.shuffle(list, random);
            ordinals = list.stream().limit(numVectorsToScore).mapToInt(Integer::intValue).toArray();

            targetOrd = random.nextInt(numVectors);

            queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = random.nextFloat();
            }
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims, numVectors, Math.min(numVectors, 20_000)));
    }

    public void setup(VectorData vectorData) throws IOException {
        VectorScorerFactory factory = getScorerFactoryOrDie();

        path = Files.createTempDirectory("Int7uBulkScorerBenchmark");
        dir = new MMapDirectory(path);
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            for (int v = 0; v < numVectors; v++) {
                out.writeBytes(vectorData.vectorData[v], dims);
                out.writeInt(Float.floatToIntBits(vectorData.offsets[v]));
            }
        }

        numVectorsToScore = vectorData.numVectorsToScore;
        scores = new float[numVectorsToScore];
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;
        targetOrd = vectorData.targetOrd;

        in = dir.openInput("vector.data", IOContext.DEFAULT);

        var values = vectorValues(dims, numVectors, in, function.function());
        float scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();

        switch (implementation) {
            case SCALAR:
                scorer = switch (function) {
                    case DOT_PRODUCT -> new ScalarDotProduct(values, scoreCorrectionConstant);
                };
                break;
            case LUCENE:
                scorer = luceneScoreSupplier(values, function.function()).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = luceneScorer(values, function.function(), vectorData.queryVector);
                }
                break;
            case NATIVE:
                scorer = factory.getInt7SQVectorScorerSupplier(function.type(), in, values, scoreCorrectionConstant).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getInt7SQVectorScorer(function.function(), values, vectorData.queryVector).orElseThrow();
                }
                break;
        }

        scorer.setScoringOrdinal(targetOrd);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(in, dir);
        IOUtils.rm(path);
    }

    @Benchmark
    public float[] dotProductMultipleSequential() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = scorer.score(v);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductMultipleRandom() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = scorer.score(ordinals[v]);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductQueryMultipleRandom() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = queryScorer.score(ordinals[v]);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductMultipleSequentialBulk() throws IOException {
        scorer.bulkScore(ids, scores, ordinals.length);
        return scores;
    }

    @Benchmark
    public float[] dotProductMultipleRandomBulk() throws IOException {
        scorer.bulkScore(ordinals, scores, ordinals.length);
        return scores;
    }

    @Benchmark
    public float[] dotProductQueryMultipleRandomBulk() throws IOException {
        queryScorer.bulkScore(ordinals, scores, ordinals.length);
        return scores;
    }
}
