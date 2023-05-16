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
import org.elasticsearch.common.util.LongDoubleArray;
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

@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class BigArrayLongDoubleBenchmark {

    LongArray counts;
    DoubleArray sums;

    LongDoubleArray longDoubleArray;

    int[] indices;

    @Param(value = { "10", "100", "1000", "10000", "100000" })
    int size;

    @Param(value = { "linear", "non-linear" })
    String accessPattern;

    static final int STRIDE = 1000;

    @Setup
    public void setup() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        counts = bigArrays.newLongArray(size, false);
        sums = bigArrays.newDoubleArray(size, false);
        longDoubleArray = bigArrays.newLongDoubleArray(size, false);

        indices = new int[size];
        if (accessPattern.equals("non-linear") && size > STRIDE) { // setup a non-linear access pattern
            int index = 0;
            final int len = size / STRIDE;
            for (int i = 0; i < STRIDE; i++) {
                for (int j = 0; j < len; j++) {
                    indices[index++] = i + (j * STRIDE);
                }
            }
        } else { // or linear access pattern - date histo
            for (int i = 0; i < size; i++) {
                indices[i] = i;
            }
        }

        // trivial benchmark assertions
        if (longDoubleArray.size() != counts.size()) {
            throw new AssertionError("longDoubleArray size=" + longDoubleArray.size() + ", counts size=" + counts.size());
        }
        if (size > STRIDE && size % STRIDE != 0) {
            throw new AssertionError("stride [" + STRIDE + "] not a multiple of size [" + size + "]");
        }
    }

    @Benchmark
    public double testGetTwoSeparateArrays() {
        return getTwoSeparateArrays(counts, sums, indices);
    }

    private static double getTwoSeparateArrays(LongArray counts, DoubleArray sums, int[] indices) {
        int len = (int) counts.size();
        double result = 0;
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            long count = counts.get(index);
            double sum = sums.get(index);
            result += sum / count;
        }
        return result;
    }

    @Benchmark
    public void testSetTwoSeparateArrays(Blackhole bh) {
        setTwoSeparateArrays(counts, sums, indices);
        bh.consume(counts);
        bh.consume(sums);
    }

    private static LongArray setTwoSeparateArrays(LongArray counts, DoubleArray sums, int[] indices) {
        int len = (int) counts.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            counts.increment(index, i + 1);
            sums.increment(index, i + 2);
        }
        return counts;
    }

    @Benchmark
    public double testGetLongDoubleArray() {
        return getLongDoubleArray(longDoubleArray, indices);
    }

    private static double getLongDoubleArray(LongDoubleArray longDoubleArray, int[] indices) {
        int len = (int) longDoubleArray.size();
        double result = 0;
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            long count = longDoubleArray.getLong(index);
            double sum = longDoubleArray.getDouble(index);
            result += sum / count;
        }
        return result;
    }

    @Benchmark
    public void testSetLongDoubleArray(Blackhole bh) {
        setLongDoubleArray(longDoubleArray, indices);
        bh.consume(longDoubleArray);
    }

    private static LongDoubleArray setLongDoubleArray(LongDoubleArray longDoubleArray, int[] indices) {
        int len = (int) longDoubleArray.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            longDoubleArray.set(index, longDoubleArray.getLong(index) + i + 1, longDoubleArray.getDouble(index) + i + 2);
        }
        return longDoubleArray;
    }

    // -- main and test below
    /*
    public static void main(String... args) {
        if (args.length != 0) {
            var test = new BigArrayLongDoubleBenchmark();
            test.size = 1_000_000;
            test.accessPattern = "non-linear";
            test.setup();
            if (args[0].equals("r")) {
                setTwoSeparateArrays(test.counts, test.sums, test.indices);
            } else {
                setLongDoubleArray(test.longDoubleArray, test.indices);
            }
        } else {
            // assert test implementation
            assertTestImpl();
        }
    }

    private static void assertTestImpl() {
        for (int size : java.util.List.of(10, 100, 1_000, 10_000, 100_000, 1_000_000)) {
            for (String accessPattern : java.util.List.of("linear", "non-linear")) {
                BigArrayLongDoubleBenchmark test = new BigArrayLongDoubleBenchmark();
                test.size = size;
                test.accessPattern = accessPattern;
                test.setup();
                setTwoSeparateArrays(test.counts, test.sums, test.indices);
                setLongDoubleArray(test.longDoubleArray, test.indices);
                assertValues(test);
                double d1 = getTwoSeparateArrays(test.counts, test.sums, test.indices);
                double d2 = getLongDoubleArray(test.longDoubleArray, test.indices);
                if (d1 != d2) {
                    throw new AssertionError("d1=" + d1 + ", d1=" + d2);
                }
            }
        }
    }

    private static void assertValues(BigArrayLongDoubleBenchmark test) {
        for (int i = 0; i < test.size; i++) {
            if (test.longDoubleArray.getLong(i) != test.counts.get(i)) {
                throw new AssertionError(test.longDoubleArray.getLong(i) + " != " + test.counts.get(i));
            }
            if (test.longDoubleArray.getDouble(i) != test.sums.get(i)) {
                throw new AssertionError(test.longDoubleArray.getDouble(i) + " != " + test.sums.get(i));
            }
        }
    }
    */
}
