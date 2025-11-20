/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.simdvec.ESVectorUtil;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1)
public class VectorScorerDistanceBulkBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "384", "782", "1024" })
    int dims;

    int length;

    int numVectors = 4 * 100;
    int numQueries = 10;

    float[][] vectors;
    float[][] queries;
    float[] distances = new float[4];

    @Setup
    public void setup() throws IOException {
        Random random = new Random(123);

        this.length = OptimizedScalarQuantizer.discretize(dims, 64) / 8;

        vectors = new float[numVectors][dims];
        for (float[] vector : vectors) {
            for (int i = 0; i < dims; i++) {
                vector[i] = random.nextFloat();
            }
        }

        queries = new float[numQueries][dims];
        for (float[] query : queries) {
            for (int i = 0; i < dims; i++) {
                query[i] = random.nextFloat();
            }
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void squareDistance(Blackhole bh) {
        for (int j = 0; j < numQueries; j++) {
            float[] query = queries[j];
            for (int i = 0; i < numVectors; i++) {
                float[] vector = vectors[i];
                float distance = VectorUtil.squareDistance(query, vector);
                bh.consume(distance);
            }
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void soarDistance(Blackhole bh) {
        for (int j = 0; j < numQueries; j++) {
            float[] query = queries[j];
            for (int i = 0; i < numVectors; i++) {
                float[] vector = vectors[i];
                float distance = ESVectorUtil.soarDistance(query, vector, vector, 1.0f, 1.0f);
                bh.consume(distance);
            }
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void squareDistanceBulk(Blackhole bh) {
        for (int j = 0; j < numQueries; j++) {
            float[] query = queries[j];
            for (int i = 0; i < numVectors; i += 4) {
                ESVectorUtil.squareDistanceBulk(query, vectors[i], vectors[i + 1], vectors[i + 2], vectors[i + 3], distances);
                for (float distance : distances) {
                    bh.consume(distance);
                }

            }
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void soarDistanceBulk(Blackhole bh) {
        for (int j = 0; j < numQueries; j++) {
            float[] query = queries[j];
            for (int i = 0; i < numVectors; i += 4) {
                ESVectorUtil.soarDistanceBulk(
                    query,
                    vectors[i],
                    vectors[i + 1],
                    vectors[i + 2],
                    vectors[i + 3],
                    vectors[i],
                    1.0f,
                    1.0f,
                    distances
                );
                for (float distance : distances) {
                    bh.consume(distance);
                }

            }
        }
    }
}
