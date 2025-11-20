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
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.createRandomInt7VectorData;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.luceneScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.readNodeCorrectionConstant;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.vectorValues;
import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;

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
    // 40M is typically enough to not fit in L3 cache
    @Param({ "128000", "1500000", "30000000" })
    public int numVectors;
    public int numVectorsToScore = 20_000;

    Path path;
    Directory dir;
    IndexInput in;
    VectorScorerFactory factory;

    float[] scores;
    int[] ordinals;
    int[] ids;
    QuantizedByteVectorValues dotProductValues;
    float scoreCorrectionConstant;
    int targetOrd;

    UpdateableRandomVectorScorer luceneDotScorer;
    UpdateableRandomVectorScorer nativeDotScorer;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        factory = getScorerFactoryOrDie();

        var random = ThreadLocalRandom.current();
        path = Files.createTempDirectory("Int7uBulkScorerBenchmark");
        dir = new MMapDirectory(path);
        createRandomInt7VectorData(random, dir, dims, numVectors);

        scores = new float[numVectorsToScore];
        targetOrd = random.nextInt(numVectors);
        List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
        Collections.shuffle(list, random);
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = list.stream().limit(numVectorsToScore).mapToInt(i -> i).toArray();

        in = dir.openInput("vector.data", IOContext.DEFAULT);

        dotProductValues = vectorValues(dims, numVectors, in, VectorSimilarityFunction.DOT_PRODUCT);
        scoreCorrectionConstant = dotProductValues.getScalarQuantizer().getConstantMultiplier();

        luceneDotScorer = luceneScoreSupplier(dotProductValues, VectorSimilarityFunction.DOT_PRODUCT).scorer();
        luceneDotScorer.setScoringOrdinal(targetOrd);
        nativeDotScorer = factory.getInt7SQVectorScorerSupplier(DOT_PRODUCT, in, dotProductValues, scoreCorrectionConstant)
            .orElseThrow()
            .scorer();
        nativeDotScorer.setScoringOrdinal(targetOrd);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(in, dir);
        IOUtils.rm(path);
    }

    @Benchmark
    public float[] dotProductLuceneMultipleSequential() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = luceneDotScorer.score(v);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductLuceneMultipleRandom() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = luceneDotScorer.score(ordinals[v]);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductNativeMultipleSequential() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = nativeDotScorer.score(v);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductNativeMultipleRandom() throws IOException {
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = nativeDotScorer.score(ordinals[v]);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductNativeMultipleSequentialBulk() throws IOException {
        nativeDotScorer.bulkScore(ids, scores, ordinals.length);
        return scores;
    }

    @Benchmark
    public float[] dotProductNativeMultipleRandomBulk() throws IOException {
        nativeDotScorer.bulkScore(ordinals, scores, ordinals.length);
        return scores;
    }

    @Benchmark
    public float[] dotProductScalarMultipleSequential() throws IOException {
        var queryVector = dotProductValues.vectorValue(targetOrd);
        var queryVectorCorrectionConstant = readNodeCorrectionConstant(dotProductValues, targetOrd);
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = scalarDotScore(v, queryVector, queryVectorCorrectionConstant);
        }
        return scores;
    }

    @Benchmark
    public float[] dotProductScalarMultipleRandom() throws IOException {
        var queryVector = dotProductValues.vectorValue(targetOrd);
        var queryVectorCorrectionConstant = readNodeCorrectionConstant(dotProductValues, targetOrd);
        for (int v = 0; v < numVectorsToScore; v++) {
            scores[v] = scalarDotScore(ordinals[v], queryVector, queryVectorCorrectionConstant);
        }
        return scores;
    }

    private float scalarDotScore(int ordinal, byte[] queryVector, float queryVectorCorrectionConstant) throws IOException {
        var vec2 = dotProductValues.vectorValue(ordinal);
        var vec2CorrectionConstant = readNodeCorrectionConstant(dotProductValues, ordinal);
        int dotProduct = 0;
        for (int i = 0; i < queryVector.length; i++) {

            dotProduct += queryVector[i] * vec2[i];
        }
        float adjustedDistance = dotProduct * scoreCorrectionConstant + queryVectorCorrectionConstant + vec2CorrectionConstant;
        return (1 + adjustedDistance) / 2;
    }
}
