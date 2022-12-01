/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.common.util;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongDoubleDoubleArray;
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

import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class BigArrayAvgTuplesBenchmark {

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;

    LongDoubleDoubleArray triple;

    int[] indices;

    @Param(value = { "1000", "10000", "100000" })
    int size;

    static final int STRIDE = 1000;

    @Setup
    public void setup() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        counts = bigArrays.newLongArray(size, false);
        sums = bigArrays.newDoubleArray(size, false);
        compensations = bigArrays.newDoubleArray(size, false);
        triple = bigArrays.newLongDoubleDoubleArray(size, false);

        // setup a non-linear access pattern
        indices = new int[size];
        int index = 0;
        final int len = size / STRIDE;
        for (int i = 0; i < STRIDE; i++) {
            for (int j = 0; j < len; j++) {
                indices[index++] = i + (j * STRIDE);
            }
        }

        // trivial benchmark assertions
        if (triple.size() != counts.size()) {
            throw new AssertionError("Triple size=" + triple.size() + ", counts size=" + counts.size());
        }
        if (size % STRIDE != 0) {
            throw new AssertionError("stride [" + STRIDE + "] not a multiple of size [" + size + "]");
        }
    }

    @Benchmark
    public void testThreeSeparateArrays(Blackhole bh) {
        threeSeparateArrays(counts, sums, compensations, indices);
        bh.consume(counts);
        bh.consume(sums);
        bh.consume(compensations);
    }

    private static LongArray threeSeparateArrays(LongArray counts, DoubleArray sums, DoubleArray compensations, int[] indices) {
        int len = (int) counts.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            counts.increment(index, i + 1);
            sums.increment(index, i + 2);
            compensations.increment(index, i + 3);
        }
        return counts;
    }

    @Benchmark
    public void testLongDoubleDoubleArraySet(Blackhole bh) {
        longDoubleDoubleArraySet(triple, indices);
        bh.consume(triple);
    }

    private static void longDoubleDoubleArraySet(LongDoubleDoubleArray triple, int[] indices) {
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.set(index, triple.getLong0(index) + i + 1, triple.getDouble0(index) + i + 2, triple.getDouble1(index) + i + 3);
        }
    }

    @Benchmark
    public void testLongDoubleDoubleArrayInc(Blackhole bh) {
        longDoubleDoubleArrayInc(triple, indices);
        bh.consume(triple);
    }

    private static void longDoubleDoubleArrayInc(LongDoubleDoubleArray triple, int[] indices) {
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.increment(index, i + 1, i + 2, i + 3);
        }
    }

    // -- main and test below
    /*
    public static void main(String... args) {
        var test = new BigArrayAvgTuplesBenchmark();
        test.size = 1_000_000;
        test.setup();
        if (args.length != 0) {
            if (args[0].equals("r")) {
                threeSeparateArrays(test.counts, test.sums, test.compensations, test.indices);
            } else {
                longDoubleDoubleArraySet(test.triple, test.indices);
            }
        } else {
            // assert test implementation
            assertTestImpl();
        }
    }

    private static void assertTestImpl() {
        {
            BigArrayAvgTuplesBenchmark test1 = new BigArrayAvgTuplesBenchmark();
            test1.size = 1_000_000;
            test1.setup();
            threeSeparateArrays(test1.counts, test1.sums, test1.compensations, test1.indices);
            longDoubleDoubleArraySet(test1.triple, test1.indices);
            assertValues(test1);
        }
        {
            BigArrayAvgTuplesBenchmark test2 = new BigArrayAvgTuplesBenchmark();
            test2.size = 10000;
            test2.setup();
            threeSeparateArrays(test2.counts, test2.sums, test2.compensations, test2.indices);
            longDoubleDoubleArrayInc(test2.triple, test2.indices);
            assertValues(test2);
        }
    }

    private static void assertValues(BigArrayAvgTuplesBenchmark test) {
        for (int i = 0; i < test.size; i++) {
            if (test.triple.getLong0(i) != test.counts.get(i)) {
                throw new AssertionError(test.triple.getLong0(i) + " != " + test.counts.get(i));
            }
            if (test.triple.getDouble0(i) != test.sums.get(i)) {
                throw new AssertionError(test.triple.getDouble0(i) + " != " + test.sums.get(i));
            }
            if (test.triple.getDouble1(i) != test.compensations.get(i)) {
                throw new AssertionError(test.triple.getDouble0(i) + " != " + test.compensations.get(i));
            }
        }
    }
    */
}
