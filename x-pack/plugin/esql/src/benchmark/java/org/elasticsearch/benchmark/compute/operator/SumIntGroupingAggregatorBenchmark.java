/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for the {@link org.elasticsearch.compute.aggregation.SumIntGroupingAggregatorFunction} aggregator with many groups.
 */
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 2, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms4g", "-Xmx4g" })
public class SumIntGroupingAggregatorBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    private static final int ROWS_PER_BLOCK = 4096;
    private static final long SEED = 42;

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private LocalCircuitBreaker localBreaker;
    private DriverContext driverContext;

    @Param({ "10000", "100000", "1000000", "10000000", "100000000" })
    int groups;

    @Param({ "sequential", "random" })
    String distribution;

    List<IntVector> groupVectors;
    IntVector values;
    IntVector lastValues;

    @Setup(Level.Trial)
    public void setup() {
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            clusterSettings
        );
        PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
        var bigArrays = new BigArrays(recycler, breakerService, "request");
        CircuitBreaker breaker = breakerService.getBreaker("request");
        localBreaker = new LocalCircuitBreaker(
            breaker,
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE.getBytes(),
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE.getBytes()
        );
        var blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build().newChildFactory(localBreaker);
        driverContext = new DriverContext(bigArrays, blockFactory, null);
        SplittableRandom random = new SplittableRandom(SEED);
        groupVectors = generateGroups(random);
        values = generateValues(random, ROWS_PER_BLOCK);
        int remainder = groups % ROWS_PER_BLOCK;
        lastValues = remainder == 0 ? values : generateValues(random, remainder);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        for (IntVector groupVector : groupVectors) {
            groupVector.close();
        }
        values.close();
        if (lastValues != values) {
            lastValues.close();
        }
        localBreaker.close();
    }

    @Benchmark
    public void benchmark() {
        try (var sum = new SumIntAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(1))) {
            for (var groups : groupVectors) {
                IntVector pageValues = groups.getPositionCount() == ROWS_PER_BLOCK ? values : lastValues;
                Page page = new Page(groups.asBlock(), pageValues.asBlock());
                try (var addInput = sum.prepareProcessRawInputPage(null, page)) {
                    addInput.add(0, groups);
                }
            }
        }
    }

    private List<IntVector> generateGroups(SplittableRandom random) {
        int[] groupIds = new int[groups];
        for (int i = 0; i < groups; i++) {
            groupIds[i] = i;
        }
        switch (distribution) {
            case "sequential" -> {
            }
            case "random" -> {
                for (int i = groups - 1; i > 0; i--) {
                    int j = random.nextInt(i + 1);
                    int tmp = groupIds[i];
                    groupIds[i] = groupIds[j];
                    groupIds[j] = tmp;
                }
            }
            default -> throw new IllegalArgumentException("unknown distribution: " + distribution);
        }
        List<IntVector> vectors = new ArrayList<>(Math.ceilDiv(groups, ROWS_PER_BLOCK));
        for (int start = 0; start < groups; start += ROWS_PER_BLOCK) {
            int positionCount = Math.min(ROWS_PER_BLOCK, groups - start);
            try (var builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
                for (int i = 0; i < positionCount; i++) {
                    builder.appendInt(i, groupIds[start + i]);
                }
                vectors.add(builder.build());
            }
        }
        return vectors;
    }

    private IntVector generateValues(SplittableRandom random, int positionCount) {
        try (var builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendInt(i, random.nextInt(1, 1001));
            }
            return builder.build();
        }
    }
}
