/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.blockhash.Ordinator64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsAppend = { "--enable-preview", "--add-modules", "jdk.incubator.vector" })
public class HashBenchmark {
    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String unique : HashBenchmark.class.getField("unique").getAnnotationsByType(Param.class)[0].value()) {
                HashBenchmark bench = new HashBenchmark();
                bench.unique = Integer.parseInt(unique);
                bench.initTestData();
                bench.longHash();
                bench.bytesRefHash();
                bench.longLongHash();
                bench.longObjectHash();
                bench.ordinator();
                bench.ordinatorArray();
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    private static final int ITERATIONS = 10_000_000;

    @Param({ "5", "1000", "10000", "100000", "1000000" })
    public int unique;

    private long[] testLongs;
    private BytesRef[] testBytes;
    private int[] targetInts;
    private long[] targetLongs;
    private Object[] targetObject;

    @Setup
    public void initTestData() {
        testLongs = LongStream.range(0, ITERATIONS).map(l -> l % unique).toArray();
        BytesRef[] uniqueBytes = IntStream.range(0, unique).mapToObj(i -> new BytesRef(Integer.toString(i))).toArray(BytesRef[]::new);
        testBytes = IntStream.range(0, ITERATIONS).mapToObj(i -> uniqueBytes[i % unique]).toArray(BytesRef[]::new);
        targetInts = new int[ITERATIONS];
        targetLongs = new long[ITERATIONS];
        targetObject = new Object[ITERATIONS];
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void longHash() {
        LongHash hash = new LongHash(16, BigArrays.NON_RECYCLING_INSTANCE);
        for (int i = 0; i < testLongs.length; i++) {
            targetLongs[i] = hash.add(testLongs[i]);
        }
        if (hash.size() != unique) {
            throw new AssertionError();
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void bytesRefHash() {
        BytesRefHash hash = new BytesRefHash(16, BigArrays.NON_RECYCLING_INSTANCE);
        for (int i = 0; i < testLongs.length; i++) {
            targetLongs[i] = hash.add(testBytes[i]);
        }
        if (hash.size() != unique) {
            throw new AssertionError();
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void longLongHash() {
        LongLongHash hash = new LongLongHash(16, BigArrays.NON_RECYCLING_INSTANCE);
        for (int i = 0; i < testLongs.length; i++) {
            targetLongs[i] = hash.add(testLongs[i], testLongs[i]);
        }
        if (hash.size() != unique) {
            throw new AssertionError();
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void longObjectHash() {
        LongObjectPagedHashMap<Object> hash = new LongObjectPagedHashMap<>(16, BigArrays.NON_RECYCLING_INSTANCE);
        Object o = new Object();
        for (int i = 0; i < testLongs.length; i++) {
            targetObject[i] = hash.put(testLongs[i], o);
        }
        if (hash.size() != unique) {
            throw new AssertionError();
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void ordinator() {
        Ordinator64 hash = new Ordinator64(
            new PageCacheRecycler(Settings.EMPTY),
            new NoopCircuitBreaker("bench"),
            new Ordinator64.IdSpace()
        );
        for (int i = 0; i < testLongs.length; i++) {
            targetInts[i] = hash.add(testLongs[i]);
        }
        if (hash.currentSize() != unique) {
            throw new AssertionError("expected " + hash.currentSize() + " to be " + unique);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void ordinatorArray() {
        Ordinator64 hash = new Ordinator64(
            new PageCacheRecycler(Settings.EMPTY),
            new NoopCircuitBreaker("bench"),
            new Ordinator64.IdSpace()
        );
        hash.add(testLongs, targetInts, testLongs.length);
        if (hash.currentSize() != unique) {
            throw new AssertionError();
        }
    }
}
