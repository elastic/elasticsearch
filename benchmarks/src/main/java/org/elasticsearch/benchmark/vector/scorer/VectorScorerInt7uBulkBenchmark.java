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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.core.IOUtils;
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
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.quantizedVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.randomInt7BytesBetween;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.writeInt7VectorData;
import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.dotProduct;
import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.squareDistance;

/**
 * Benchmark that compares bulk scoring of various scalar quantized vector similarity function
 * implementations (scalar, lucene's panama-ized, and Elasticsearch's native) against sequential
 * and random access target vectors.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt7uBulkBenchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt7uBulkBenchmark {

    static {
        BenchmarkUtils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // 128kb is typically enough to not fit in L1 (core) cache for most processors;
    // 1.5Mb is typically enough to not fit in L2 (core) cache;
    // 130Mb is enough to not fit in L3 cache
    @Param({ "128", "1500", "130000" })
    public int numVectors;
    public int numVectorsToScore;

    // Bulk sizes to test.
    // DiskBBQ has two bulk sizes, 16 and 32
    // HNSW params will have the distributed ordinal bulk sizes depending on the number of connections in the graph
    // The default is 16, maximum is 512, and the bottom layer is 2x that the configured setting, so 1024 is a maximum
    // the MOST common case here is 32
    @Param({ "16", "32", "64", "256", "1024" })
    public int bulkSize;

    @Param({ "SCALAR", "LUCENE", "NATIVE" })
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

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
            var vec2CorrectionConstant = values.getScoreCorrectionConstant(ordinal);
            int dotProduct = dotProduct(queryVector, vec2);
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
            int squareDistance = squareDistance(queryVector, vec2);
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

    private float[] scores;
    private int[] ordinals;
    private int[] ids;
    private int[] toScore; // scratch array for bulk scoring

    private UpdateableRandomVectorScorer scorer;
    private RandomVectorScorer queryScorer;

    static class VectorData {
        private final int numVectorsToScore;
        private final byte[][] vectorData;
        private final float[] offsets;
        private final int[] ordinals;
        private final int targetOrd;
        private final float[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore) {
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

    void setup(VectorData vectorData) throws IOException {
        VectorScorerFactory factory = getScorerFactoryOrDie();

        path = Files.createTempDirectory("Int7uBulkScorerBenchmark");
        dir = new MMapDirectory(path);
        writeInt7VectorData(dir, vectorData.vectorData, vectorData.offsets);

        numVectorsToScore = vectorData.numVectorsToScore;
        scores = new float[bulkSize];
        toScore = new int[bulkSize]; // scratch array for ordinal slices
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        in = dir.openInput("vector.data", IOContext.DEFAULT);

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
                    queryScorer = luceneScorer(values, function.function(), vectorData.queryVector);
                }
                break;
            case NATIVE:
                scorer = factory.getInt7SQVectorScorerSupplier(function, in, values, scoreCorrectionConstant).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    queryScorer = factory.getInt7SQVectorScorer(function.function(), values, vectorData.queryVector).orElseThrow();
                }
                break;
        }

        scorer.setScoringOrdinal(vectorData.targetOrd);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(in, dir);
        IOUtils.rm(path);
    }

    @Benchmark
    public float[] scoreMultipleSequential() throws IOException {
        int v = 0;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                scores[i] = scorer.score(v);
            }
        }
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleRandom() throws IOException {
        int v = 0;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                scores[i] = scorer.score(ordinals[v]);
            }
        }
        return scores;
    }

    @Benchmark
    public float[] scoreQueryMultipleRandom() throws IOException {
        int v = 0;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                scores[i] = queryScorer.score(ordinals[v]);
            }
        }
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleSequentialBulk() throws IOException {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int toScoreInThisBatch = Math.min(bulkSize, numVectorsToScore - i);
            // Copy the slice of sequential IDs to the scratch array
            System.arraycopy(ids, i, toScore, 0, toScoreInThisBatch);
            scorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleRandomBulk() throws IOException {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int toScoreInThisBatch = Math.min(bulkSize, numVectorsToScore - i);
            // Copy the slice of random ordinals to the scratch array
            System.arraycopy(ordinals, i, toScore, 0, toScoreInThisBatch);
            scorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }

    @Benchmark
    public float[] scoreQueryMultipleRandomBulk() throws IOException {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int toScoreInThisBatch = Math.min(bulkSize, numVectorsToScore - i);
            // Copy the slice of random ordinals to the scratch array
            System.arraycopy(ordinals, i, toScore, 0, toScoreInThisBatch);
            queryScorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }
}
