/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.benchmark.tdigest;

import org.elasticsearch.tdigest.MergingDigest;
import org.elasticsearch.tdigest.ScaleFunction;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
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
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class MergeBench {
    private Random gen = new Random();
    private double[] data;

    //    @Param({"20", "50", "100", "200", "500"})
    @Param({"50", "100"})
    public int compression;

    //    @Param({"1", "2", "5", "10"})
    @Param({"2", "5", "10"})
    public int factor;

    //    @Param({"K_1", "K_2", "K_3"})
    @Param({"K_2"})
    public String scaleFunction;

    private MergingDigest td;

    @Setup
    public void setup() {
        data = new double[10000000];
        for (int i = 0; i < data.length; i++) {
            data[i] = gen.nextDouble();
        }
        td = new MergingDigest(compression, (factor + 1) * compression, compression);
        td.setScaleFunction(ScaleFunction.valueOf(scaleFunction));

        // First values are very cheap to add, we are more interested in the steady state,
        // when the summary is full. Summaries are expected to contain about 0.6*compression
        // centroids, hence the 5 * compression * (factor+1)
        for (int i = 0; i < 5 * compression * (factor + 1); ++i) {
            td.add(gen.nextDouble());
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        int index = 0;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void add(ThreadState state) {
        if (state.index >= data.length) {
            state.index = 0;
        }
        td.add(data[state.index++]);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MergeBench.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .resultFormat(ResultFormatType.CSV)
                .addProfiler(StackProfiler.class)
                .build();

        new Runner(opt).run();
    }

}
