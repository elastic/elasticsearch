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

    static final int PAGE_SIZE = 1 << 14; // 16k

    static final int PAGE_ELEMENTS = PAGE_SIZE / Long.BYTES;

    int fakeOffset;

    // Notes:
    // M1 L1 cache size is 64k or 128k depending on performance of efficient core.
    // M1 L2 cache size is 12MB.

    @Setup
    public void setup() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        // 2048 * 250 = 512,000 (8 byte) elements = 4,096,000 bytes = 3.9MB per bigArray instance
        counts = bigArrays.newLongArray(PAGE_ELEMENTS * 250, false);
        sums = bigArrays.newDoubleArray(PAGE_ELEMENTS * 250, false);
        compensations = bigArrays.newDoubleArray(PAGE_ELEMENTS * 250, false);
        fakeOffset = 0;

        triple = bigArrays.newLongDoubleDoubleArray((PAGE_ELEMENTS * 250) * 3, false);
    }

    // Touches all elements of all pages in all arrays, in a ping-pong like fashion.
    // The inner loop touches one element per on each bigArray page, on each iteration.
    // The outer loop does the inner a number of time, by striding over each page-index.
    @Benchmark
    public void testReal(Blackhole bh) {
        for (int i = 0; i < PAGE_ELEMENTS; i++) {
            for (int j = 0; j < 1000; j++) {
                int offset = j * PAGE_ELEMENTS;
                counts.increment(offset + i, 1);
                sums.increment(offset + i, 2);
                compensations.increment(offset + i, 3);
            }
        }
        bh.consume(counts);
        bh.consume(sums);
        bh.consume(compensations);
    }

    @Benchmark
    public void testLongDoubleDoubleArray(Blackhole bh) {
        for (int i = 0; i < PAGE_ELEMENTS; i++) {
            for (int j = 0; j < 250; j++) {
                int offset = j * PAGE_ELEMENTS;
                triple.set(offset + i, 1, 2, 3);
            }
        }
        bh.consume(counts);
        bh.consume(sums);
        bh.consume(compensations);
    }

    // public static void main(String... args) {
    // var test = new BigArrayAvgTuplesBenchmark();
    // test.setup();
    // test.testReal(null);
    // test.testFake(null);
    // }
}

/*

Output on chegar's Mac M1

Benchmark                            Mode  Cnt        Score        Error  Units
BigArrayAvgTuplesBenchmark.testFake  avgt   30  1812699.723 ±  49472.941  ns/op
BigArrayAvgTuplesBenchmark.testReal  avgt   30  3225329.426 ± 181804.312  ns/op


 */
