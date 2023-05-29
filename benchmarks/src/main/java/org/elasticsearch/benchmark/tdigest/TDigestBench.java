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

import org.elasticsearch.tdigest.AVLTreeDigest;
import org.elasticsearch.tdigest.MergingDigest;
import org.elasticsearch.tdigest.TDigest;
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
import java.util.function.Supplier;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class TDigestBench {

    public enum TDigestFactory {
        MERGE {
            @Override
            TDigest create(double compression) {
                return new MergingDigest(compression, (int) (10 * compression));
            }
        },
        AVL_TREE {
            @Override
            TDigest create(double compression) {
                return new AVLTreeDigest(compression);
            }
        };

        abstract TDigest create(double compression);
    }

    @Param({ "100", "300" })
    double compression;

    @Param({ "MERGE", "AVL_TREE" })
    TDigestFactory tdigestFactory;

    @Param({ "NORMAL", "GAUSSIAN" })
    String distribution;

    Random random;
    TDigest tdigest;
    // AbstractDistribution distribution;

    double[] data = new double[1000000];

    @Setup
    public void setUp() {
        random = ThreadLocalRandom.current();
        tdigest = tdigestFactory.create(compression);

        Supplier<Double> nextRandom = () -> distribution.equals("GAUSSIAN") ? random.nextGaussian() : random.nextDouble();
        for (int i = 0; i < 10000; ++i) {
            tdigest.add(nextRandom.get());
        }

        for (int i = 0; i < data.length; ++i) {
            data[i] = nextRandom.get();
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
        tdigest.add(data[state.index++]);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(".*" + TDigestBench.class.getSimpleName() + ".*")
            .warmupIterations(5)
            .measurementIterations(5)
            .addProfiler(GCProfiler.class)
            .addProfiler(StackProfiler.class)
            .build();

        new Runner(opt).run();
    }
}
