/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link BinaryQuantizer} covering the three public quantization
 * methods across different vector dimensions and similarity functions.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3)
public class BinaryQuantizerBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "384", "768", "1024" })
    int dims;

    @Param({ "EUCLIDEAN", "DOT_PRODUCT" })
    String similarity;

    private BinaryQuantizer quantizer;
    private float[] vector;
    private float[] centroid;
    private byte[] indexDestination;
    private byte[] queryDestination;

    @Setup(Level.Iteration)
    public void init() {
        VectorSimilarityFunction simFunc = VectorSimilarityFunction.valueOf(similarity);
        int discretizedDims = BQVectorUtils.discretize(dims, 64);
        quantizer = new BinaryQuantizer(dims, discretizedDims, simFunc);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        vector = new float[dims];
        centroid = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = random.nextFloat(-1f, 1f);
            centroid[i] = random.nextFloat(-1f, 1f);
        }

        indexDestination = new byte[discretizedDims / 8];
        queryDestination = new byte[discretizedDims * BinaryQuantizer.B_QUERY / 8];
    }

    @Benchmark
    public float[] quantizeForIndex() {
        return quantizer.quantizeForIndex(vector, indexDestination, centroid);
    }

    @Benchmark
    public BinaryQuantizer.QueryFactors quantizeForQuery() {
        return quantizer.quantizeForQuery(vector, queryDestination, centroid);
    }

    @Benchmark
    public BinaryQuantizer.QueryAndIndexResults quantizeQueryAndIndex() {
        return quantizer.quantizeQueryAndIndex(vector, indexDestination, queryDestination, centroid);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public float[] quantizeForIndexPanama() {
        return quantizer.quantizeForIndex(vector, indexDestination, centroid);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public BinaryQuantizer.QueryFactors quantizeForQueryPanama() {
        return quantizer.quantizeForQuery(vector, queryDestination, centroid);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public BinaryQuantizer.QueryAndIndexResults quantizeQueryAndIndexPanama() {
        return quantizer.quantizeQueryAndIndex(vector, indexDestination, queryDestination, centroid);
    }
}
