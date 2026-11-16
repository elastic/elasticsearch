/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
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
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * The Top-N operator over a long key and a value column, in three regimes: without a query scope, which is the operator
 * as it runs when spilling is off; under a scope whose budget it never crosses, which is the cost of the checks spilling
 * adds when it is not triggered; and under a scope crossed at the first page, so the values go to runs of an in-memory
 * store and come back for the output, which is the cost of the value-run path itself, framing and codec, without the
 * disk. The disk's share is a workload measurement, outside this benchmark.
 */
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms4g", "-Xmx4g" })
public class TopNSpillBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    private static final int ROWS_PER_PAGE = 4096;
    private static final long SEED = 42;
    /** The own-state minimum spilling applies in production, below which an operator never spills. */
    private static final long PRODUCTION_MIN_SPILL_BYTES = 16L * 1024 * 1024;

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private BlockFactory nodeFactory;
    private LocalCircuitBreaker localBreaker;
    private DriverContext driverContext;
    private MemorySpillStore store;
    private QueryScope scope;
    private QueryScope.Lease lease;
    private QueryScope.DriverAllocation allocation;
    private List<Page> pages;

    @Param({ "100000", "1000000" })
    int rows;

    @Param({ "1000", "100000" })
    int limit;

    @Param({ "8", "256" })
    int valueBytes;

    /** {@code resident}: no scope; {@code scope}: a budget never crossed; {@code run}: the values in runs from the first page. */
    @Param({ "resident", "scope", "run" })
    String mode;

    @Setup(Level.Trial)
    public void setup() {
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            clusterSettings
        );
        PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
        BigArrays bigArrays = new BigArrays(recycler, breakerService, "request");
        CircuitBreaker breaker = breakerService.getBreaker("request");
        nodeFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();
        LocalCircuitBreaker.SizeSettings sizes = new LocalCircuitBreaker.SizeSettings(
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE.getBytes(),
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE.getBytes()
        );
        if (mode.equals("resident")) {
            localBreaker = new LocalCircuitBreaker(breaker, sizes.overReservedBytes(), sizes.maxOverReservedBytes());
            driverContext = new DriverContext(bigArrays, nodeFactory.newChildFactory(localBreaker), null);
        } else {
            store = new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, Long.MAX_VALUE, "benchmark", new Random(SEED));
            // whole reads: the store's short reads are a test of the readers' framing, not a cost to measure
            store.setShortReads(false);
            boolean run = mode.equals("run");
            long budget = run ? 1 : Long.MAX_VALUE / 4;
            // the own-state minimum of production in the scope mode, so the operator never acquires a writer there
            long minSpillBytes = run ? 0 : PRODUCTION_MIN_SPILL_BYTES;
            scope = QueryScope.create("benchmark", nodeFactory, budget, minSpillBytes, store, EsExecutors.DIRECT_EXECUTOR_SERVICE, s -> {});
            lease = scope.tryAcquire();
            allocation = scope.newDriverAllocation(sizes);
            driverContext = new DriverContext(allocation.bigArrays(), allocation.blockFactory(), sizes, "benchmark", scope);
        }
        pages = generatePages(new SplittableRandom(SEED));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        for (Page page : pages) {
            page.releaseBlocks();
        }
        if (scope != null) {
            // the driver's allocation holds a reservation until it closes, the local breaker's over-reservation and the
            // view's credit, so the account is checked once it is closed
            allocation.close();
            long used = scope.breaker().used();
            if (used != 0) {
                throw new IllegalStateException("the query's account holds [" + used + "] bytes after the invocations: a leak");
            }
            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            scope.addCloseListener(closed);
            lease.close();
            closed.get(30, TimeUnit.SECONDS);
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        } else {
            localBreaker.close();
        }
    }

    @Benchmark
    public long benchmark() {
        TopNOperator.TopNOperatorFactory factory = new TopNOperator.TopNOperatorFactory(
            limit,
            List.of(ElementType.LONG, ElementType.BYTES_REF),
            List.of(TopNEncoder.DEFAULT_UNSORTABLE, TopNEncoder.DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            ROWS_PER_PAGE,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            null
        );
        long positions = 0;
        try (Operator operator = factory.get(driverContext)) {
            for (Page page : pages) {
                while (operator.needsInput() == false) {
                    // a call after a compaction or a roll started answers no once and does the housekeeping that lets
                    // the next answer yes; the store completes inline, so the loop never waits on it
                    Thread.onSpinWait();
                }
                operator.addInput(page.shallowCopy());
            }
            operator.finish();
            while (operator.isFinished() == false) {
                Page out = operator.getOutput();
                if (out != null) {
                    positions += out.getPositionCount();
                    out.releaseBlocks();
                }
            }
        }
        return positions;
    }

    private List<Page> generatePages(SplittableRandom random) {
        BlockFactory factory = driverContext.blockFactory();
        List<Page> result = new ArrayList<>();
        byte[] value = new byte[valueBytes];
        for (int start = 0; start < rows; start += ROWS_PER_PAGE) {
            int count = Math.min(ROWS_PER_PAGE, rows - start);
            try (
                LongBlock.Builder keys = factory.newLongBlockBuilder(count);
                BytesRefBlock.Builder values = factory.newBytesRefBlockBuilder(count)
            ) {
                for (int i = 0; i < count; i++) {
                    keys.appendLong(random.nextLong());
                    for (int b = 0; b < valueBytes; b++) {
                        value[b] = (byte) ('a' + random.nextInt(26));
                    }
                    values.appendBytesRef(new BytesRef(value));
                }
                result.add(new Page(keys.build(), values.build()));
            }
        }
        return result;
    }
}
