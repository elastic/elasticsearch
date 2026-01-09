/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.common.settings;

import org.elasticsearch.common.settings.SecureString;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class SecureStringCloneBenchmark {

    private SecureString secureString;

    @Param({ "8", "32", "128", "512" })
    private int stringLength;

    @Setup
    public void setup() {
        char[] chars = new char[stringLength];
        Arrays.fill(chars, 'a');
        secureString = new SecureString(chars);
    }

    @Benchmark
    @Threads(1)
    public void cloneSecureStringOneThread(Blackhole blackhole) {
        blackhole.consume(secureString.clone());
    }

    @Benchmark
    @Threads(4)
    public void cloneSecureStringFourThreads(Blackhole blackhole) {
        blackhole.consume(secureString.clone());
    }

    @Benchmark
    @Threads(8)
    public void cloneSecureStringEightThreads(Blackhole blackhole) {
        blackhole.consume(secureString.clone());
    }

    @Benchmark
    @Threads(32)
    public void cloneSecureStringThirtyTwoThreads(Blackhole blackhole) {
        blackhole.consume(secureString.clone());
    }

}
