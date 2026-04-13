/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
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
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104ScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104Scorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.VECTOR_DATA_FILE;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.createI4ScalarQueryScorer;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.createI4ScalarScorer;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.generateCentroid;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.generateCorrectiveTerms;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.quantizeQuery;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.writeI4VectorData;
import static org.elasticsearch.nativeaccess.Int4TestUtils.packNibbles;
import static org.elasticsearch.simdvec.ESVectorUtil.dotProduct;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createDenseInt4VectorValues;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;

/**
 * Benchmark that compares bulk scoring of int4 packed-nibble quantized vectors:
 * scalar vs Lucene's Lucene104ScalarQuantizedVectorScorer vs native,
 * across sequential and random access patterns.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4BulkBenchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt4BulkBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    @Param({ "128", "1500", "130000" })
    public int numVectors;
    public int numVectorsToScore;

    @Param({ "16", "32", "64", "256", "1024" })
    public int bulkSize;

    @Param
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private Path path;
    private Directory dir;
    private IndexInput in;

    private float[] scores;
    private int[] ordinals;
    private int[] ids;
    private int[] toScore;

    private UpdateableRandomVectorScorer scorer;
    private RandomVectorScorer queryScorer;

    static class VectorData {
        final int numVectorsToScore;
        final byte[][] packedVectors;
        final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        final float[] centroid;
        final float centroidDp;
        final int[] ordinals;
        final int targetOrd;
        final float[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore) {
            this.numVectorsToScore = numVectorsToScore;
            packedVectors = new byte[numVectors][];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int v = 0; v < numVectors; v++) {
                byte[] unpacked = new byte[dims];
                randomInt4Bytes(random, unpacked);
                packedVectors[v] = packNibbles(unpacked);
            }
            corrections = generateCorrectiveTerms(dims, numVectors);
            centroid = generateCentroid(dims);
            centroidDp = dotProduct(centroid, centroid);

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
        VectorSimilarityFunction similarityFunction = function.function();

        path = Files.createTempDirectory("Int4BulkScorerBenchmark");
        dir = new MMapDirectory(path);
        writeI4VectorData(dir, vectorData.packedVectors, vectorData.corrections);
        in = dir.openInput(VECTOR_DATA_FILE, IOContext.DEFAULT);

        QuantizedByteVectorValues values = createDenseInt4VectorValues(
            dims,
            vectorData.packedVectors.length,
            vectorData.centroid,
            vectorData.centroidDp,
            in,
            similarityFunction
        );

        numVectorsToScore = vectorData.numVectorsToScore;
        scores = new float[bulkSize];
        toScore = new int[bulkSize];
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        switch (implementation) {
            case SCALAR:
                scorer = createI4ScalarScorer(values, similarityFunction);
                queryScorer = createI4ScalarQueryScorer(values, similarityFunction, vectorData.queryVector);
                break;
            case LUCENE:
                scorer = lucene104ScoreSupplier(values, similarityFunction).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = lucene104Scorer(values, similarityFunction, vectorData.queryVector);
                }
                break;
            case NATIVE:
                var factory = getScorerFactoryOrDie();
                scorer = factory.getInt4VectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    var qQuery = quantizeQuery(values, similarityFunction, vectorData.queryVector);
                    queryScorer = factory.getInt4VectorScorer(
                        similarityFunction,
                        values,
                        qQuery.unpackedQuery(),
                        qQuery.corrections().lowerInterval(),
                        qQuery.corrections().upperInterval(),
                        qQuery.corrections().additionalCorrection(),
                        qQuery.corrections().quantizedComponentSum()
                    ).orElseThrow();
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
            System.arraycopy(ids, i, toScore, 0, toScoreInThisBatch);
            scorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleRandomBulk() throws IOException {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int toScoreInThisBatch = Math.min(bulkSize, numVectorsToScore - i);
            System.arraycopy(ordinals, i, toScore, 0, toScoreInThisBatch);
            scorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }

    @Benchmark
    public float[] scoreQueryMultipleRandomBulk() throws IOException {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int toScoreInThisBatch = Math.min(bulkSize, numVectorsToScore - i);
            System.arraycopy(ordinals, i, toScore, 0, toScoreInThisBatch);
            queryScorer.bulkScore(toScore, scores, toScoreInThisBatch);
        }
        return scores;
    }
}
