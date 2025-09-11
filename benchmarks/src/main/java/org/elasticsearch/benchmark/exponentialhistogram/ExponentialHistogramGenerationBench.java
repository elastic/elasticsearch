/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.exponentialhistogram;

import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramGenerator;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class ExponentialHistogramGenerationBench {

    @Param({ "100", "500", "1000", "5000", "10000", "20000" })
    int bucketCount;

    @Param({ "NORMAL", "GAUSSIAN" })
    String distribution;

    Random random;
    ExponentialHistogramGenerator histoGenerator;

    double[] data = new double[1000000];

    int index;

    @Setup
    public void setUp() {
        random = ThreadLocalRandom.current();
        histoGenerator = ExponentialHistogramGenerator.create(bucketCount, ExponentialHistogramCircuitBreaker.noop());

        DoubleSupplier nextRandom = () -> distribution.equals("GAUSSIAN") ? random.nextGaussian() : random.nextDouble();

        // Make sure that we start with a non-empty histogram, as this distorts initial additions
        for (int i = 0; i < 10000; ++i) {
            histoGenerator.add(nextRandom.getAsDouble());
        }

        for (int i = 0; i < data.length; ++i) {
            data[i] = nextRandom.getAsDouble();
        }

        index = 0;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void add() {
        if (index >= data.length) {
            index = 0;
        }
        histoGenerator.add(data[index++]);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(".*" + ExponentialHistogramGenerationBench.class.getSimpleName() + ".*")
            .warmupIterations(5)
            .measurementIterations(5)
            .addProfiler(GCProfiler.class)
            .addProfiler(StackProfiler.class)
            .build();

        new Runner(opt).run();
    }
}
