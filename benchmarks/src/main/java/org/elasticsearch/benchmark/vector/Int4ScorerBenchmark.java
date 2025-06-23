/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
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
public class Int4ScorerBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "1024" })
    int dims;

    int numVectors = 200;
    int numQueries = 10;

    byte[] scratch;
    byte[][] binaryVectors;
    byte[][] binaryQueries;

    ES91Int4VectorsScorer scorer;
    Directory dir;
    IndexInput in;

    @Setup
    public void setup() throws IOException {
        binaryVectors = new byte[numVectors][dims];
        dir = new MMapDirectory(Files.createTempDirectory("vectorData"));
        try (IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT)) {
            for (byte[] binaryVector : binaryVectors) {
                for (int i = 0; i < dims; i++) {
                    // 4-bit quantization
                    binaryVector[i] = (byte) ThreadLocalRandom.current().nextInt(16);
                }
                out.writeBytes(binaryVector, 0, binaryVector.length);
            }
        }

        in = dir.openInput("vectors", IOContext.DEFAULT);
        binaryQueries = new byte[numVectors][dims];
        for (byte[] binaryVector : binaryVectors) {
            for (int i = 0; i < dims; i++) {
                // 4-bit quantization
                binaryVector[i] = (byte) ThreadLocalRandom.current().nextInt(16);
            }
        }

        scratch = new byte[dims];
        scorer = ESVectorizationProvider.getInstance().newES91Int4VectorsScorer(in, dims);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(dir, in);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void scoreFromArray(Blackhole bh) throws IOException {
        for (int j = 0; j < numQueries; j++) {
            in.seek(0);
            for (int i = 0; i < numVectors; i++) {
                in.readBytes(scratch, 0, dims);
                bh.consume(VectorUtil.int4DotProduct(binaryQueries[j], scratch));
            }
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void scoreFromMemorySegmentOnlyVector(Blackhole bh) throws IOException {
        for (int j = 0; j < numQueries; j++) {
            in.seek(0);
            for (int i = 0; i < numVectors; i++) {
                bh.consume(scorer.int4DotProduct(binaryQueries[j]));
            }
        }
    }
}
