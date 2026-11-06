/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Measures the cost per call of a timer call site in two forms. {@code inert} calls {@code start} and {@code stop}
 * unconditionally, and a timer that does not measure time is {@link Timer#INERT}. {@code nullGuarded} checks for null
 * around {@code start} and {@code stop}, and a timer that does not measure time is null. Collector setup is excluded.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code timing}: {@code on} uses a timer that measures elapsed time, {@code off} uses one that does not.</li>
 *   <li>{@code receivers}: {@code stable} keeps one timer at the call site. {@code mixed} alternates a measuring and a
 *   non-measuring timer, so the JIT cannot specialize the call site. {@code mixed} ignores {@code timing}.</li>
 *   <li>{@code workTokens}: the work inside each timed call, through {@link Blackhole#consumeCPU}.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class TimerCallSiteBenchmark {

    private static final int CALLS = 1024;

    private static final TimerMetric METRIC = TimerMetric.of("Bench", "calls", "call_time_nanos", TimingPolicy.ADAPTIVE, "a timed call");

    @Param({ "on", "off" })
    public String timing;

    @Param({ "stable", "mixed" })
    public String receivers;

    @Param({ "0", "10" })
    public long workTokens;

    private final Timer[] guardedTimers = new Timer[2];
    private final Timer[] inertTimers = new Timer[2];
    private int mask;

    @Setup
    public void setup() {
        final Timer enabled = ProfileCollector.create(ProfileCollector.Mode.ELAPSED).timer(METRIC);
        final Timer disabled = ProfileCollector.create(ProfileCollector.Mode.COUNTERS).timer(METRIC);
        if (receivers.equals("mixed")) {
            guardedTimers[0] = enabled;
            guardedTimers[1] = null;
            inertTimers[0] = enabled;
            inertTimers[1] = disabled;
            mask = 1;
        } else {
            final boolean on = timing.equals("on");
            guardedTimers[0] = on ? enabled : null;
            inertTimers[0] = on ? enabled : disabled;
            mask = 0;
        }
    }

    @Benchmark
    @OperationsPerInvocation(CALLS)
    public void nullGuarded(Blackhole blackhole) {
        for (int i = 0; i < CALLS; i++) {
            final Timer timer = guardedTimers[i & mask];
            if (timer != null) {
                timer.start();
            }
            try {
                Blackhole.consumeCPU(workTokens);
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        }
        blackhole.consume(guardedTimers);
    }

    @Benchmark
    @OperationsPerInvocation(CALLS)
    public void inert(Blackhole blackhole) {
        for (int i = 0; i < CALLS; i++) {
            final Timer timer = inertTimers[i & mask];
            timer.start();
            try {
                Blackhole.consumeCPU(workTokens);
            } finally {
                timer.stop();
            }
        }
        blackhole.consume(inertTimers);
    }
}
