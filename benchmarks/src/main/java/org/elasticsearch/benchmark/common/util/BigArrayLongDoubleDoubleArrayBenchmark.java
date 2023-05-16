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

@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class BigArrayLongDoubleDoubleArrayBenchmark {

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;

    LongDoubleDoubleArray triple;

    int[] indices;

    @Param(value = { "10", "1000", "10000", "100000" })
    int size;

    @Param(value = { "linear", "non-linear" })
    String accessPattern;

    static final int STRIDE = 1000;

    @Setup
    public void setup() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        counts = bigArrays.newLongArray(size, false);
        sums = bigArrays.newDoubleArray(size, false);
        compensations = bigArrays.newDoubleArray(size, false);
        triple = bigArrays.newLongDoubleDoubleArray(size, false);

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
        if (triple.size() != counts.size()) {
            throw new AssertionError("Triple size=" + triple.size() + ", counts size=" + counts.size());
        }
        if (size > STRIDE && size % STRIDE != 0) {
            throw new AssertionError("stride [" + STRIDE + "] not a multiple of size [" + size + "]");
        }
    }

    @Benchmark
    public double testGetThreeSeparateArrays() {
        return getThreeSeparateArrays(counts, sums, compensations, indices);
    }

    private static double getThreeSeparateArrays(LongArray counts, DoubleArray sums, DoubleArray compensations, int[] indices) {
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
    public void testIncThreeSeparateArrays(Blackhole bh) {
        incThreeSeparateArrays(counts, sums, compensations, indices);
        bh.consume(counts);
        bh.consume(sums);
        bh.consume(compensations);
    }

    private static LongArray incThreeSeparateArrays(LongArray counts, DoubleArray sums, DoubleArray compensations, int[] indices) {
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
    public double testGetLongDoubleDoubleArray() {
        return getLongDoubleDoubleArray(triple, indices);
    }

    private static double getLongDoubleDoubleArray(LongDoubleDoubleArray triple, int[] indices) {
        int len = (int) triple.size();
        double result = 0;
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            long count = triple.getLong0(index);
            double sum = triple.getDouble0(index);
            result += sum / count;
        }
        return result;
    }

    @Benchmark
    public double testGetLongDoubleDoubleArrayHolder() {
        return getLongDoubleDoubleArrayHolder(triple, indices);
    }

    private static double getLongDoubleDoubleArrayHolder(LongDoubleDoubleArray triple, int[] indices) {
        LongDoubleDoubleArray.Holder holder = new LongDoubleDoubleArray.Holder();
        double result = 0;
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.get(index, holder);
            long count = holder.getLong0();
            double sum = holder.getDouble0();
            result += sum / count;
        }
        return result;
    }

    @Benchmark
    public void testSetLongDoubleDoubleArray(Blackhole bh) {
        setLongDoubleDoubleArray(triple, indices);
        bh.consume(triple);
    }

    private static LongDoubleDoubleArray setLongDoubleDoubleArray(LongDoubleDoubleArray triple, int[] indices) {
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.set(index, triple.getLong0(index) + i + 1, triple.getDouble0(index) + i + 2, triple.getDouble1(index) + i + 3);
        }
        return triple;
    }

    @Benchmark
    public void testSetLongDoubleDoubleArrayHolder(Blackhole bh) {
        setLongDoubleDoubleArrayHolder(triple, indices);
        bh.consume(triple);
    }

    private static LongDoubleDoubleArray setLongDoubleDoubleArrayHolder(LongDoubleDoubleArray triple, int[] indices) {
        LongDoubleDoubleArray.Holder holder = new LongDoubleDoubleArray.Holder();
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.get(index, holder);
            triple.set(index, holder.getLong0() + i + 1, holder.getDouble0() + i + 2, holder.getDouble1() + i + 3);
        }
        return triple;
    }

    @Benchmark
    public void testIncLongDoubleDoubleArray(Blackhole bh) {
        incLongDoubleDoubleArray(triple, indices);
        bh.consume(triple);
    }

    private static LongDoubleDoubleArray incLongDoubleDoubleArray(LongDoubleDoubleArray triple, int[] indices) {
        int len = (int) triple.size();
        for (int i = 0; i < len; i++) {
            int index = indices[i];
            triple.increment(index, i + 1, i + 2, i + 3);
        }
        return triple;
    }

    // -- main and test below
    /*
    public static void main(String... args) {
        if (args.length != 0) {
            var test = new BigArrayLongDoubleDoubleArrayBenchmark();
            test.size = 1_000_000;
            test.accessPattern = "non-linear";
            test.setup();
            if (args[0].equals("r")) {
                incThreeSeparateArrays(test.counts, test.sums, test.compensations, test.indices);
            } else {
                setLongDoubleDoubleArray(test.triple, test.indices);
            }
        } else {
            // assert test implementation
            assertTestImpl();
        }
    }

    private static void assertTestImpl() {
        for (int size : java.util.List.of(10, 100, 1_000, 10_000, 100_000, 1_000_000)) {
            for (String accessPattern : java.util.List.of("linear", "non-linear")) {
                {
                    BigArrayLongDoubleDoubleArrayBenchmark test1 = new BigArrayLongDoubleDoubleArrayBenchmark();
                    test1.size = size;
                    test1.accessPattern = accessPattern;
                    test1.setup();
                    incThreeSeparateArrays(test1.counts, test1.sums, test1.compensations, test1.indices);
                    setLongDoubleDoubleArray(test1.triple, test1.indices);
                    assertValues(test1);
                }
                {
                    BigArrayLongDoubleDoubleArrayBenchmark test2 = new BigArrayLongDoubleDoubleArrayBenchmark();
                    test2.size = size;
                    test2.accessPattern = accessPattern;
                    test2.setup();
                    incThreeSeparateArrays(test2.counts, test2.sums, test2.compensations, test2.indices);
                    incLongDoubleDoubleArray(test2.triple, test2.indices);
                    assertValues(test2);
                }
                {
                    BigArrayLongDoubleDoubleArrayBenchmark test3 = new BigArrayLongDoubleDoubleArrayBenchmark();
                    test3.size = size;
                    test3.accessPattern = accessPattern;
                    test3.setup();
                    incThreeSeparateArrays(test3.counts, test3.sums, test3.compensations, test3.indices);
                    setLongDoubleDoubleArrayHolder(test3.triple, test3.indices);
                    assertValues(test3);
                }
            }
        }
    }

    private static void assertValues(BigArrayLongDoubleDoubleArrayBenchmark test) {
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
