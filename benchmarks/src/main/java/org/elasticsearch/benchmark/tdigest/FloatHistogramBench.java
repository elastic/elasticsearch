/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.benchmark.tdigest;

import org.elasticsearch.tdigest.FloatHistogram;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Explores the value of using a large buffer for the MergingDigest. The rationale is that the internal
 * sort is extremely fast while the merging function in the t-digest can be quite slow, if only because
 * computing the asin function involved in the merge is expensive. This argues for collecting more samples
 * before sorting and merging them into the digest.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class FloatHistogramBench {
    private Random gen = new Random();
    private double[] data;

    @Param({ "20", "50", "100" })
    public int binsPerDecade;

    private FloatHistogram fh;

    @Setup
    public void setup() {
        data = new double[10000000];
        for (int i = 0; i < data.length; i++) {
            data[i] = gen.nextDouble();
        }
        fh = new FloatHistogram(0.1, 10000, binsPerDecade);

        for (int i = 0; i < 10000; ++i) {
            fh.add(gen.nextDouble());
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        int index = 0;
    }

    @Benchmark
    public void add(ThreadState state) {
        if (state.index >= data.length) {
            state.index = 0;
        }
        fh.add(data[state.index++]);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(".*" + FloatHistogramBench.class.getSimpleName() + ".*")
            .resultFormat(ResultFormatType.CSV)
            .result("overall-results.csv")
            .addProfiler(StackProfiler.class)
            .addProfiler(GCProfiler.class)
            .build();

        new Runner(opt).run();
    }

}
