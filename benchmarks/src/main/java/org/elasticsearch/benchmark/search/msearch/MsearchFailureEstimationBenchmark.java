/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.msearch;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class MsearchFailureEstimationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Number of {@link ShardSearchFailure} entries in the {@link SearchPhaseExecutionException}. */
    @Param({ "1", "28", "178" })
    public int shardFailureCount;

    public SearchPhaseExecutionException failure;

    @Setup
    public void setup() {
        ShardSearchFailure[] failures = new ShardSearchFailure[shardFailureCount];
        for (int i = 0; i < shardFailureCount; i++) {
            failures[i] = new ShardSearchFailure(new EsRejectedExecutionException("rejected execution of search on [index][" + i + "]"));
        }
        failure = new SearchPhaseExecutionException("query", "all shards failed", failures);
    }

    @Benchmark
    public void estimate(Blackhole bh) {
        bh.consume(TransportMultiSearchAction.estimateFailureBytes(failure));
    }
}
