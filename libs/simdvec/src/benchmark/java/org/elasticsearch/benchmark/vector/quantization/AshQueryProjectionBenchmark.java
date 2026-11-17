/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.quantization;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.benchmark.vector.VectorizationInfo;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Cost of projecting one query through the ASH projection matrices of every segment of a shard:
 * <ul>
 *     <li>{@code DISTINCT}: each segment has its own matrix, so the query is projected once per segment.</li>
 *     <li>{@code SHARED_PER_SEGMENT}: the segments share one matrix, but the query is still projected once per segment.</li>
 *     <li>{@code SHARED_ONCE}: the segments share one matrix and the query is projected once, as {@code SharedQueryTransforms}
 *     does for segments with the same matrix.</li>
 * </ul>
 * <p>
 * Optionally streams {@code pollutionMb} of unrelated data before each query, standing in for the posting lists a
 * query scans, which compete with the matrices for the CPU caches. {@link #pollutionOnly} measures that streaming
 * alone so it can be subtracted.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class AshQueryProjectionBenchmark {

    static {
        BenchmarkLogging.configure();
        VectorizationInfo.printOnce();
    }

    public enum Sharing {
        DISTINCT,
        SHARED_PER_SEGMENT,
        SHARED_ONCE
    }

    /** Projected dims (rows of W^T). 768 is the dbpedia 3072-dim tier. */
    @Param({ "768" })
    int nDims;

    /** Original dims (columns of W^T). */
    @Param({ "3072" })
    int originalDim;

    @Param({ "1", "4", "11", "22" })
    int segments;

    @Param({ "DISTINCT", "SHARED_PER_SEGMENT", "SHARED_ONCE" })
    Sharing sharing;

    @Param({ "0", "64" })
    int pollutionMb;

    private ESVectorUtilSupport impl;
    private byte[][] matrices;
    private byte[] query;
    private int[] result;
    private long[] pollution;

    @Setup(Level.Trial)
    public void init() {
        impl = ESVectorizationProvider.lookup(true, true).getVectorUtilSupport();
        Random random = new Random(42);
        int distinct = sharing == Sharing.DISTINCT ? segments : 1;
        matrices = new byte[distinct][];
        for (int i = 0; i < distinct; i++) {
            matrices[i] = new byte[nDims * originalDim];
            random.nextBytes(matrices[i]);
        }
        query = new byte[originalDim];
        random.nextBytes(query);
        result = new int[nDims];
        pollution = new long[pollutionMb * 1024 * 1024 / Long.BYTES];
        for (int i = 0; i < pollution.length; i++) {
            pollution[i] = random.nextLong();
        }
    }

    private long pollute() {
        long sum = 0;
        for (int i = 0; i < pollution.length; i += 8) { // one read per 64-byte cache line
            sum += pollution[i];
        }
        return sum;
    }

    /** One query: optional cache pollution, then the projections it needs across all segments. */
    @Benchmark
    public void projectQuery(Blackhole bh) {
        bh.consume(pollute());
        int projections = sharing == Sharing.SHARED_ONCE ? 1 : segments;
        for (int s = 0; s < projections; s++) {
            byte[] w = matrices[sharing == Sharing.DISTINCT ? s : 0];
            impl.matrixVectorMultiply(w, nDims, originalDim, query, result);
            bh.consume(result);
        }
    }

    /** The cache pollution alone, to subtract from {@link #projectQuery}. */
    @Benchmark
    public void pollutionOnly(Blackhole bh) {
        bh.consume(pollute());
    }
}
