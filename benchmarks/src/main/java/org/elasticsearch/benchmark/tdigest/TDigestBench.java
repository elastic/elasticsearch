/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.tdigest;

import org.elasticsearch.tdigest.AVLTreeDigest;
import org.elasticsearch.tdigest.MergingDigest;
import org.elasticsearch.tdigest.TDigest;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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

            @Override
            TDigest create() {
                return create(100);
            }
        },
        AVL_TREE {
            @Override
            TDigest create(double compression) {
                return new AVLTreeDigest(compression);
            }

            @Override
            TDigest create() {
                return create(20);
            }
        };

        abstract TDigest create(double compression);
        abstract TDigest create();
    }
//
//    public enum DistributionFactory {
//        UNIFORM {
//            @Override
//            AbstractDistribution create(Random random) {
//                return new Uniform(0, 1, random);
//            }
//        },
//        SEQUENTIAL {
//            @Override
//            AbstractDistribution create(Random random) {
//                return new AbstractContinousDistribution() {
//                    double base = 0;
//
//                    @Override
//                    public double nextDouble() {
//                        base += Math.PI * 1e-5;
//                        return base;
//                    }
//                };
//            }
//        },
//        REPEATED {
//            @Override
//            AbstractDistribution create(final Random random) {
//                return new AbstractContinousDistribution() {
//                    @Override
//                    public double nextDouble() {
//                        return random.nextInt(10);
//                    }
//                };
//            }
//        },
//        GAMMA {
//            @Override
//            AbstractDistribution create(Random random) {
//                return new Gamma(0.1, 0.1, random);
//            }
//        },
//        NORMAL {
//            @Override
//            AbstractDistribution create(Random random) {
//                return new Normal(0.1, 0.1, random);
//            }
//        };
//
//        abstract AbstractDistribution create(Random random);
//    }

    @Param({"100", "300"})
    double compression;

    @Param({"MERGE", "AVL_TREE"})
    TDigestFactory tdigestFactory;

//    @Param({"NORMAL", "GAMMA"})
//    DistributionFactory distributionFactory;

    Random random;
    TDigest tdigest;
//    AbstractDistribution distribution;

    double[] data = new double[1000000];

    @Setup
    public void setUp() {
        random = ThreadLocalRandom.current();
        tdigest = tdigestFactory.create(compression);
//        distribution = distributionFactory.create(random);
        // first values are cheap to add, so pre-fill the t-digest to have more realistic results
        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < 10000; ++i) {
            tdigest.add(random.nextDouble());
        }

        for (int i = 0; i < data.length; ++i) {
            data[i] = random.nextDouble();
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        int index = 0;
    }

    @Benchmark
    public void timeAdd(MergeBench.ThreadState state) {
        if (state.index >= data.length) {
            state.index = 0;
        }
        tdigest.add(data[state.index++]);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TDigestBench.class.getSimpleName() + ".*")
                .resultFormat(ResultFormatType.CSV)
                .result("overall-results.csv")
                .addProfiler(GCProfiler.class)
                .addProfiler(StackProfiler.class)
                .build();

        new Runner(opt).run();
    }
}
