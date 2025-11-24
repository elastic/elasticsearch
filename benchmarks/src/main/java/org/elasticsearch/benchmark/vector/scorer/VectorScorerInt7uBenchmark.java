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
import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;

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

    Path path;
    Directory dir;
    IndexInput in;
    VectorScorerFactory factory;

    byte[] vec1, vec2;
    float vec1CorrectionConstant;
    float vec2CorrectionConstant;
    float scoreCorrectionConstant;

    UpdateableRandomVectorScorer luceneDotScorer;
    UpdateableRandomVectorScorer luceneSqrScorer;
    UpdateableRandomVectorScorer nativeDotScorer;
    UpdateableRandomVectorScorer nativeSqrScorer;

    RandomVectorScorer luceneDotScorerQuery;
    RandomVectorScorer nativeDotScorerQuery;
    RandomVectorScorer luceneSqrScorerQuery;
    RandomVectorScorer nativeSqrScorerQuery;

    @Setup
    public void setup() throws IOException {
        factory = getScorerFactoryOrDie();

        var random = ThreadLocalRandom.current();
        path = Files.createTempDirectory("Int7uScorerBenchmark");
        dir = new MMapDirectory(path);
        createRandomInt7VectorData(random, dir, dims, numVectors);

        in = dir.openInput("vector.data", IOContext.DEFAULT);
        final var dotProductValues = vectorValues(dims, numVectors, in, VectorSimilarityFunction.DOT_PRODUCT);
        scoreCorrectionConstant = dotProductValues.getScalarQuantizer().getConstantMultiplier();
        luceneDotScorer = luceneScoreSupplier(dotProductValues, VectorSimilarityFunction.DOT_PRODUCT).scorer();
        luceneDotScorer.setScoringOrdinal(0);
        nativeDotScorer = factory.getInt7SQVectorScorerSupplier(DOT_PRODUCT, in, dotProductValues, scoreCorrectionConstant)
            .orElseThrow()
            .scorer();
        nativeDotScorer.setScoringOrdinal(0);

        vec1 = dotProductValues.vectorValue(0).clone();
        vec1CorrectionConstant = readNodeCorrectionConstant(dotProductValues, 0);
        vec2 = dotProductValues.vectorValue(1).clone();
        vec2CorrectionConstant = readNodeCorrectionConstant(dotProductValues, 1);

        final var euclideanValues = vectorValues(dims, numVectors, in, VectorSimilarityFunction.EUCLIDEAN);
        luceneSqrScorer = luceneScoreSupplier(euclideanValues, VectorSimilarityFunction.EUCLIDEAN).scorer();
        luceneSqrScorer.setScoringOrdinal(0);
        nativeSqrScorer = factory.getInt7SQVectorScorerSupplier(EUCLIDEAN, in, euclideanValues, scoreCorrectionConstant)
            .orElseThrow()
            .scorer();
        nativeSqrScorer.setScoringOrdinal(0);

        if (supportsHeapSegments()) {
            // setup for getInt7SQVectorScorer / query vector scoring
            float[] queryVec = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVec[i] = ThreadLocalRandom.current().nextFloat();
            }
            luceneDotScorerQuery = luceneScorer(dotProductValues, VectorSimilarityFunction.DOT_PRODUCT, queryVec);
            nativeDotScorerQuery = factory.getInt7SQVectorScorer(VectorSimilarityFunction.DOT_PRODUCT, dotProductValues, queryVec)
                .orElseThrow();
            luceneSqrScorerQuery = luceneScorer(euclideanValues, VectorSimilarityFunction.EUCLIDEAN, queryVec);
            nativeSqrScorerQuery = factory.getInt7SQVectorScorer(VectorSimilarityFunction.EUCLIDEAN, euclideanValues, queryVec)
                .orElseThrow();
        }
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(dir, in);
        IOUtils.rm(path);
    }

    @Benchmark
    public float dotProductLucene() throws IOException {
        return luceneDotScorer.score(1);
    }

    @Benchmark
    public float dotProductNative() throws IOException {
        return nativeDotScorer.score(1);
    }

    @Benchmark
    public float dotProductScalar() {
        int dotProduct = 0;
        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
        }
        float adjustedDistance = dotProduct * scoreCorrectionConstant + vec1CorrectionConstant + vec2CorrectionConstant;
        return (1 + adjustedDistance) / 2;
    }

    @Benchmark
    public float dotProductLuceneQuery() throws IOException {
        return luceneDotScorerQuery.score(1);
    }

    @Benchmark
    public float dotProductNativeQuery() throws IOException {
        return nativeDotScorerQuery.score(1);
    }

    // -- square distance

    @Benchmark
    public float squareDistanceLucene() throws IOException {
        return luceneSqrScorer.score(1);
    }

    @Benchmark
    public float squareDistanceNative() throws IOException {
        return nativeSqrScorer.score(1);
    }

    @Benchmark
    public float squareDistanceScalar() {
        int squareDistance = 0;
        for (int i = 0; i < vec1.length; i++) {
            int diff = vec1[i] - vec2[i];
            squareDistance += diff * diff;
        }
        float adjustedDistance = squareDistance * scoreCorrectionConstant;
        return 1 / (1f + adjustedDistance);
    }

    @Benchmark
    public float squareDistanceLuceneQuery() throws IOException {
        return luceneSqrScorerQuery.score(1);
    }

    @Benchmark
    public float squareDistanceNativeQuery() throws IOException {
        return nativeSqrScorerQuery.score(1);
    }
}
