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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.benchmark.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base class for benchmarks of Elasticsearch's vector scorer implementations
 *
 * Run benchmarks with ./gradlew -p benchmarks run --args 'class name'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public abstract class VectorScorerBulkBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // Bulk sizes to test.
    // HNSW params will have the distributed ordinal bulk sizes depending on the number of connections in the graph
    // The default is 16, maximum is 512, and the bottom layer is 2x that the configured setting, so 1024 is a maximum
    // the MOST common case here is 32
    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    public int numVectorsToScore;

    private Path path;
    private Directory dir;
    private IndexInput in;

    private float[] scores;
    private int[] ordinals;
    private int[] ids;
    private int[] toScore; // scratch array for bulk scoring

    UpdateableRandomVectorScorer scorer;
    RandomVectorScorer queryScorer;

    abstract static class VectorData {
        final int numVectorsToScore;
        final int[] ordinals;
        final int targetOrd;

        VectorData(int numVectors, int numVectorsToScore, Random random) {
            this.numVectorsToScore = numVectorsToScore;

            List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
            Collections.shuffle(list, random);
            ordinals = list.stream().limit(numVectorsToScore).mapToInt(Integer::intValue).toArray();

            targetOrd = random.nextInt(numVectors);
        }

        String vectorDataFile() {
            return "vector.data";
        }

        abstract void writeVectorData(Directory directory) throws IOException;

        static float[] randomFloatArray(Random random, int dims) {
            float[] vec = new float[dims];
            for (int i = 0; i < vec.length; i++) {
                vec[i] = random.nextFloat();
            }
            return vec;
        }
    }

    void setup(VectorData vectorData, int numVectors) throws IOException {
        path = Files.createTempDirectory("VectorBulkBenchmark");
        dir = new MMapDirectory(path);
        vectorData.writeVectorData(dir);

        numVectorsToScore = vectorData.numVectorsToScore;
        scores = new float[bulkSize];
        toScore = new int[bulkSize];
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        in = dir.openInput(vectorData.vectorDataFile(), IOContext.DEFAULT);
        createScorers(in, vectorData);

        scorer.setScoringOrdinal(vectorData.targetOrd);
    }

    abstract void createScorers(IndexInput in, VectorData vectorData) throws IOException;

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
