/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compares charging the request breaker directly with charging it through the breaker of a query, as ES|QL drivers and
 * {@link BigArrays} do once their reserve is used up. The request breaker is the node's, from
 * {@link HierarchyCircuitBreakerService}. Run with {@code -t} to compare contended charges from many threads.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class QueryCircuitBreakerBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /** {@code request} charges the node's request breaker, {@code query} the breaker of a query on top of it. */
    @Param({ "request", "query" })
    private String breaker;

    private QueryScheduler.QueryPart part;
    private CircuitBreaker target;

    @Setup(Level.Trial)
    public void setup() {
        HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CircuitBreaker request = service.getBreaker(CircuitBreaker.REQUEST);
        QueryScheduler scheduler = new QueryScheduler(System::nanoTime, request);
        part = scheduler.part(new TaskId("coordinator", 1), 1);
        target = breaker.equals("query") ? part.breaker() : request;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        part.close();
    }

    /** One charge of a page and its release. */
    @Benchmark
    public void chargeAndRelease() {
        target.addEstimateBytesAndMaybeBreak(16384, "<reused_arrays>");
        target.addWithoutBreaking(-16384);
    }

    /** The same from 8 threads, like the drivers of one query. */
    @Benchmark
    @Threads(8)
    public void chargeAndReleaseContended() {
        target.addEstimateBytesAndMaybeBreak(16384, "<reused_arrays>");
        target.addWithoutBreaking(-16384);
    }
}
