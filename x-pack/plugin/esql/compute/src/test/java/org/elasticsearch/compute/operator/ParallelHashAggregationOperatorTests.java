/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.FakeCpuClock;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class ParallelHashAggregationOperatorTests extends ComputeTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";
    private static final String SMALL_TEST_EXECUTOR = "esql_small_test_executor";

    private TestThreadPool threadPool;
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());
    private final List<MemorySpillStore> stores = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        return driverContext(blockFactory());
    }

    protected final DriverContext crankyDriverContext() {
        return driverContext(crankyBlockFactory());
    }

    Executor randomWorkerExecutor() {
        return threadPool.executor(randomFrom(ESQL_TEST_EXECUTOR, SMALL_TEST_EXECUTOR));
    }

    protected DriverContext driverContext(BlockFactory blockFactory) {
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        driverContexts.add(driverContext);
        return driverContext;
    }

    public void testSmall() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(1, 256)
        );
        DriverContext driverContext = driverContext();
        runTest(between(100, 1000), randomBoolean(), randomBoolean(), driverContext.blockFactory(), driverContext, config);
    }

    public void testLarge() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), randomBoolean(), randomBoolean(), driverContext.blockFactory(), driverContext(), config);
    }

    public void testRejectionButOkay() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            threadPool.executor(SMALL_TEST_EXECUTOR),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), randomBoolean(), randomBoolean(), driverContext.blockFactory(), driverContext(), config);
    }

    public void testCranky() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 1024)
        );
        try {
            runTest(
                between(1000, 100 * 1000),
                randomBoolean(),
                randomBoolean(),
                driverContext().blockFactory(),
                crankyDriverContext(),
                config
            );
        } catch (CircuitBreakingException ignored) {

        }
    }

    public void testSinglePassStatus() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            1024
        );
        DriverContext driverContext = driverContext();
        var status = runTest(4096, true, randomBoolean(), driverContext.blockFactory(), driverContext, config);
        assertThat(status.completedOperators(), hasSize(3));
        OperatorStatus operatorStatus = status.completedOperators().get(1);
        assertThat(operatorStatus.operator(), equalTo("ParallelHashAggregationOperator"));
    }

    public void testMemoryThresholdPromotes() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            Integer.MAX_VALUE,
            1
        );
        DriverContext driverContext = driverContext();
        var status = runTest(4096, true, randomBoolean(), driverContext.blockFactory(), driverContext, config);
        assertThat(status.completedOperators(), hasSize(3));
        assertThat(status.completedOperators().get(1).operator(), equalTo("ParallelHashAggregationOperator"));
    }

    public void testTwoPassesStatus() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            1024
        );
        DriverContext driverContext = driverContext();
        var status = runTest(4096, false, true, driverContext.blockFactory(), driverContext, config);
        assertThat(status.completedOperators(), hasSize(3));
        OperatorStatus operatorStatus = status.completedOperators().get(1);
        assertThat(operatorStatus.operator(), equalTo("ParallelHashAggregationOperator"));
        HashAggregationOperator.Status hashStatus = (HashAggregationOperator.Status) operatorStatus.status();
        var partitioningStatus = (ParallelHashAggregationOperator.PartitioningStatus) hashStatus.extraFields.get(0);
        assertThat(partitioningStatus.partitionedBlocksReceived(), greaterThan(0));
    }

    /**
     * Every generation spills: a budget of a few kilobytes is crossed before the first split, so the registry writes
     * each one to a slice run and phase two restores them all.
     */
    public void testSpillingSinglePass() {
        StoreAndCodec sc = newStoreAndCodec();
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofKb(between(1, 64)), 0, sc.store(), sc.codec());
        var status = runTest(
            between(10_000, 50_000),
            true,
            randomBoolean(),
            driverContext.blockFactory(),
            driverContext,
            null,
            spillingConfig()
        );
        assertSpilled(partitioningStatus(status));
    }

    /**
     * The second stage receives partitioned blocks from the first and admits them as they arrive: under a tiny budget
     * every received generation is written out before the next block is taken.
     */
    public void testSpillingReceivedGenerations() {
        StoreAndCodec sc = newStoreAndCodec();
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofKb(between(1, 64)), 0, sc.store(), sc.codec());
        DriverContext finalContext = spillingDriverContext(driverContext);
        var status = runTest(
            between(10_000, 50_000),
            false,
            true,
            driverContext.blockFactory(),
            driverContext,
            finalContext,
            spillingConfig()
        );
        ParallelHashAggregationOperator.PartitioningStatus partitioning = partitioningStatus(status);
        assertThat(partitioning.partitionedBlocksReceived(), greaterThan(0));
        assertSpilled(partitioning);
    }

    /**
     * A handful of keys over a large input: every generation lands in the same few partitions, so a partition
     * restores many slices, more than the read-ahead, in order.
     */
    public void testSpillingSkewed() {
        StoreAndCodec sc = newStoreAndCodec();
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofKb(between(1, 64)), 0, sc.store(), sc.codec());
        // the smallest key range makes four keys, which must reach the partitioning threshold
        var config = new HashAggregationOperator.ParallelConfig(randomWorkerExecutor(), between(1, 8), between(1, 64), between(2, 4));
        var status = runTest(
            between(5_000, 20_000),
            true,
            randomBoolean(),
            driverContext.blockFactory(),
            driverContext,
            null,
            config,
            between(1, 4),
            between(16, 256)
        );
        ParallelHashAggregationOperator.PartitioningStatus partitioning = partitioningStatus(status);
        assertSpilled(partitioning);
        assertThat(partitioning.spill().spillRuns(), greaterThan(PartitionedHashAggregations.READ_AHEAD * 4));
    }

    /**
     * Past the metadata bound the aggregation fails with a message that says so, rather than keeping a handle per
     * generation without limit.
     */
    public void testTooManyGenerationsFail() {
        StoreAndCodec sc = newStoreAndCodec();
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, sc.store(), sc.codec());
        var config = new HashAggregationOperator.ParallelConfig(randomWorkerExecutor(), between(1, 4), 1, 1);
        int rows = PartitionedHashAggregations.MAX_SPILLED_GENERATIONS + between(10, 100);
        Exception e = expectThrows(
            Exception.class,
            () -> runTest(rows, true, false, driverContext.blockFactory(), driverContext, null, config, rows * 2L, 1)
        );
        ElasticsearchStatusException cause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(e, ElasticsearchStatusException.class);
        assertNotNull("not the generation bound: " + ExceptionsHelper.stackTrace(e), cause);
        assertThat(cause.getMessage(), containsString("generations"));
    }

    /**
     * A write that the store fails takes the aggregation down, with the store's failure as the cause.
     */
    public void testWriteFailureFailsTheAggregation() {
        StoreAndCodec sc = newStoreAndCodec();
        sc.store().setFailureInjection(operation -> operation.equals("finish") ? new java.io.IOException("the store said no") : null);
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, sc.store(), sc.codec());
        Exception e = expectThrows(
            Exception.class,
            () -> runTest(between(1_000, 10_000), true, false, driverContext.blockFactory(), driverContext, null, spillingConfig())
        );
        assertThat(e.getMessage(), containsString("the store said no"));
    }

    /**
     * Closing the operator while a generation's write is held by the store: the write completes after the close and
     * everything it holds goes back, including the generation it published into a closed registry.
     */
    public void testCloseWithWriteInFlight() throws Exception {
        MemorySpillStore store = newStore(MemorySpillStore.Completion.DEFERRED);
        // a store that holds its completions needs a codec pool: a codec task waits on the append it is held on, and
        // the releasing thread must not be the one waiting
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, threadPool.generic());
        var config = new HashAggregationOperator.ParallelConfig(EsExecutors.DIRECT_EXECUTOR_SERVICE, 1, 1, between(1, 64));
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT));
        HashAggregationOperator hash = new HashAggregationOperator(
            AggregatorMode.SINGLE,
            aggregatorFactories(AggregatorMode.SINGLE, 1),
            dc -> BlockHash.build(groupSpecs, dc.blockFactory(), 128, false),
            1024,
            0.5,
            1024,
            null,
            driverContext,
            config,
            false
        );
        ParallelHashAggregationOperator op = new ParallelHashAggregationOperator(config, hash);
        hash.close();
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            rows.add(new Row(new Key((long) i, i), new int[] { i }));
        }
        try {
            for (Page page : inputPages(driverContext.blockFactory(), rows, 1)) {
                op.addInput(page);
            }
            op.finish();
            assertThat("a write is held by the store", store.heldOperations(), greaterThan(0));
        } finally {
            op.close();
        }
        driverContext.finish();
        PlainActionFuture<Void> asyncActionsDone = new PlainActionFuture<>();
        driverContext.waitForAsyncActions(asyncActionsDone);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (asyncActionsDone.isDone() == false) {
            store.releaseAll();
            assertTrue("the write did not complete", System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
        asyncActionsDone.get();
    }

    /**
     * A split that spills hands its generation to the write before it clears the worker's batch: the replacement
     * aggregators the clear builds may trip the breaker, and a generation nobody owns by then would keep its charge on
     * the request breaker for good.
     */
    public void testASplitWhoseClearTripsTheBreakerLeaksNothing() throws Exception {
        MemorySpillStore store = newStore(MemorySpillStore.Completion.INLINE);
        DriverContext driverContext = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        GroupingAggregator.Factory sum = new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(1)
        );
        AtomicBoolean failReplacement = new AtomicBoolean();
        // what a state's initial allocation does under pressure, at the one point of the split where it can: the clear
        GroupingAggregator.Factory factory = new GroupingAggregator.Factory() {
            @Override
            public GroupingAggregator apply(DriverContext context) {
                if (failReplacement.get()) {
                    throw new CircuitBreakingException("replacement aggregator", CircuitBreaker.Durability.TRANSIENT);
                }
                return sum.apply(context);
            }

            @Override
            public String describe() {
                return sum.describe();
            }
        };
        var config = new HashAggregationOperator.ParallelConfig(EsExecutors.DIRECT_EXECUTOR_SERVICE, 1, 1, 1024);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG));
        HashAggregationOperator op = new HashAggregationOperator(
            AggregatorMode.SINGLE,
            List.of(factory),
            dc -> BlockHash.build(groupSpecs, dc.blockFactory(), 128, false),
            1024,
            0.5,
            1024,
            null,
            driverContext,
            config,
            false
        );
        PartitionedHashAggregations registry = new PartitionedHashAggregations(
            driverContext.blockFactory().parent().breaker(),
            driverContext.queryScope(),
            driverContext,
            op.spawnWorker(),
            1,
            new PartitionedHashAggregations.Events() {
                @Override
                public void generationPublished() {}

                @Override
                public void restoreLanded() {}

                @Override
                public void failed(Exception e) {
                    throw new AssertionError(e);
                }
            }
        );
        try {
            int rows = between(100, 1000);
            try (
                var keys = driverContext.blockFactory().newLongBlockBuilder(rows);
                var values = driverContext.blockFactory().newIntBlockBuilder(rows)
            ) {
                for (int i = 0; i < rows; i++) {
                    keys.appendLong(i);
                    values.appendInt(i);
                }
                op.addInput(new Page(keys.build(), values.build()));
            }
            failReplacement.set(true);
            expectThrows(CircuitBreakingException.class, () -> registry.split(driverContext.breaker(), op));
            assertThat("the generation was written before the clear failed", registry.spillStatus().spillRuns(), equalTo(1));
        } finally {
            registry.close();
            op.close();
            driverContext.finish();
        }
        closeSpillingScopes();
        assertThat("the query holds nothing once closed", driverContext.queryScope().breaker().used(), equalTo(0L));
    }

    private HashAggregationOperator.ParallelConfig spillingConfig() {
        return new HashAggregationOperator.ParallelConfig(randomWorkerExecutor(), between(1, 8), between(1, 64), between(64, 512));
    }

    private MemorySpillStore newStore() {
        return newStore(MemorySpillStore.Completion.INLINE);
    }

    /**
     * A store completing inline or on the pool, with a codec executor that is safe for it.
     */
    private record StoreAndCodec(MemorySpillStore store, Executor codec) {}

    private StoreAndCodec newStoreAndCodec() {
        if (randomBoolean()) {
            return new StoreAndCodec(newStore(MemorySpillStore.Completion.INLINE), codecExecutor());
        }
        return new StoreAndCodec(newStore(MemorySpillStore.Completion.EXECUTOR), threadPool.generic());
    }

    private MemorySpillStore newStore(MemorySpillStore.Completion completion) {
        MemorySpillStore store = new MemorySpillStore(
            completion,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        stores.add(store);
        return store;
    }

    /**
     * The codec runs on the calling thread only when the store completes inline: otherwise a codec task waiting on an
     * append could occupy the thread that completes it.
     */
    private Executor codecExecutor() {
        return randomBoolean() ? EsExecutors.DIRECT_EXECUTOR_SERVICE : threadPool.generic();
    }

    private static ParallelHashAggregationOperator.PartitioningStatus partitioningStatus(DriverStatus status) {
        OperatorStatus operatorStatus = status.completedOperators().get(1);
        assertThat(operatorStatus.operator(), equalTo("ParallelHashAggregationOperator"));
        HashAggregationOperator.Status hashStatus = (HashAggregationOperator.Status) operatorStatus.status();
        return (ParallelHashAggregationOperator.PartitioningStatus) hashStatus.extraFields.get(0);
    }

    private static void assertSpilled(ParallelHashAggregationOperator.PartitioningStatus partitioning) {
        assertThat(partitioning.spill().spillRuns(), greaterThan(0));
        assertThat(partitioning.spill().spilledBytes(), greaterThan(0L));
        assertThat(partitioning.spill().readBackBytes(), greaterThan(0L));
        assertThat(partitioning.spill().readBackBytes(), greaterThanOrEqualTo(partitioning.spill().spilledBytes() / 2));
    }

    public void testWorkerCpuIsInDriverCpu() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            threadPool.generic(),
            randomIntBetween(2, 32),
            1,
            randomIntBetween(256, 4 * 1024)
        );
        // Only the workers' threads advance this clock, by a fixed step per reading, so any CPU time in the driver's status
        // is CPU time the workers measured, however coarse the real thread CPU clock is.
        AtomicLong workerClock = new AtomicLong();
        CpuTracker cpuTracker = new CpuTracker(
            () -> Thread.currentThread().getName().contains("[generic]") ? workerClock.addAndGet(1000) : 0
        );
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(
            blockFactory.bigArrays(),
            blockFactory,
            null,
            null,
            DriverContext.WarningsMode.COLLECT,
            cpuTracker
        );
        driverContexts.add(driverContext);
        var status = runTest(between(10 * 1024, 50 * 1000), true, randomBoolean(), blockFactory, driverContext, config);
        assertThat(status.cpuNanos(), greaterThan(0L));
    }

    /**
     * A worker that holds its lock when the driver closes the operator closes itself once its task ends, on its own
     * thread. The driver context parks the worker inside its first measurement until the operator is closed, and the
     * worker's release is the only CPU time that passes on the fake clock.
     */
    public void testWorkerClosedAfterOperatorIsInDriverCpu() {
        FakeCpuClock clock = new FakeCpuClock();
        CountDownLatch workerMetering = new CountDownLatch(1);
        CountDownLatch operatorClosed = new CountDownLatch(1);
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(
            blockFactory.bigArrays(),
            blockFactory,
            null,
            null,
            DriverContext.WarningsMode.COLLECT,
            new CpuTracker(clock)
        ) {
            @Override
            public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
                return super.meteredCpu(() -> {
                    if (Thread.currentThread().getName().contains("[generic]") && workerMetering.getCount() > 0) {
                        workerMetering.countDown();
                        safeAwait(operatorClosed);
                    }
                    return work.get();
                });
            }

            @Override
            public void releaseChildBlockFactory(BlockFactory childFactory) {
                clock.burn(1000);
                super.releaseChildBlockFactory(childFactory);
            }
        };
        driverContexts.add(driverContext);
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(threadPool.generic(), 1, 1, 1024);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG));
        HashAggregationOperator hash = new HashAggregationOperator(
            AggregatorMode.SINGLE,
            aggregatorFactories(AggregatorMode.SINGLE, 0),
            dc -> BlockHash.build(groupSpecs, dc.blockFactory(), 128, false),
            1024,
            0.5,
            1024,
            null,
            driverContext,
            config,
            false
        );
        ParallelHashAggregationOperator op = new ParallelHashAggregationOperator(config, hash);
        hash.close();
        op.finish();
        try {
            safeAwait(workerMetering);
            op.close();
        } finally {
            operatorClosed.countDown();
        }
        driverContext.finish();
        PlainActionFuture<Void> asyncActionsDone = new PlainActionFuture<>();
        driverContext.waitForAsyncActions(asyncActionsDone);
        safeGet(asyncActionsDone);
        assertThat(driverContext.cpuNanos(), equalTo(1000L));
    }

    record Key(Long longValue, Integer intValue) {}

    record Row(Key key, int[] sums) {}

    DriverStatus runTest(
        int numValues,
        boolean singlePass,
        boolean allowPartitionedOutput,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        return runTest(numValues, singlePass, allowPartitionedOutput, sourceBlockFactory, driverContext, null, parallelConfig);
    }

    DriverStatus runTest(
        int numValues,
        boolean singlePass,
        boolean allowPartitionedOutput,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        @Nullable DriverContext finalDriverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        return runTest(
            numValues,
            singlePass,
            allowPartitionedOutput,
            sourceBlockFactory,
            driverContext,
            finalDriverContext,
            parallelConfig,
            numValues * 2L
        );
    }

    DriverStatus runTest(
        int numValues,
        boolean singlePass,
        boolean allowPartitionedOutput,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        @Nullable DriverContext finalDriverContext,
        HashAggregationOperator.ParallelConfig parallelConfig,
        long keyRange
    ) {
        return runTest(
            numValues,
            singlePass,
            allowPartitionedOutput,
            sourceBlockFactory,
            driverContext,
            finalDriverContext,
            parallelConfig,
            keyRange,
            between(1, 1024)
        );
    }

    /**
     * @param finalDriverContext the context of the second stage's driver, or null for one over the first's block factory
     * @param keyRange keys are drawn from {@code [0, keyRange]}: small for skew, many generations over few partitions
     * @param maxPageSize the largest input page
     */
    DriverStatus runTest(
        int numValues,
        boolean singlePass,
        boolean allowPartitionedOutput,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        @Nullable DriverContext finalDriverContext,
        HashAggregationOperator.ParallelConfig parallelConfig,
        long keyRange,
        int maxPageSize
    ) {
        int aggregations = between(0, 4);
        boolean nullKeys = randomBoolean();
        List<Row> inputRows = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            int[] sums = new int[aggregations];
            for (int v = 0; v < aggregations; v++) {
                sums[v] = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Key key = new Key(
                nullKeys && randomInt(100) < 20 ? null : randomLongBetween(0, keyRange),
                nullKeys && randomInt(100) < 20 ? null : randomIntBetween(0, (int) Math.min(Integer.MAX_VALUE, keyRange))
            );
            inputRows.add(new Row(key, sums));
        }
        Map<Key, long[]> expected = expected(inputRows, aggregations);
        List<Page> inputPages = inputPages(sourceBlockFactory, inputRows, aggregations, maxPageSize);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT));
        List<Page> outputPages = new ArrayList<>();
        final DriverStatus status;
        final Function<DriverContext, BlockHash> blockHashSupplier;
        if (randomBoolean()) {
            blockHashSupplier = dc -> BlockHash.build(groupSpecs, dc.blockFactory(), between(128, 1024), false);
        } else {
            blockHashSupplier = dc -> BlockHash.buildPackedValuesBlockHash(groupSpecs, dc.blockFactory(), between(128, 1024));
        }
        try (SourceOperator sourceOperator = new CannedSourceOperator(inputPages.iterator())) {
            if (singlePass) {
                status = runSinglePassAggregation(
                    driverContext,
                    sourceOperator,
                    blockHashSupplier,
                    aggregations,
                    parallelConfig,
                    outputPages
                );
            } else {
                status = runTwoPassesAggregation(
                    driverContext,
                    finalDriverContext,
                    sourceOperator,
                    blockHashSupplier,
                    aggregations,
                    parallelConfig,
                    allowPartitionedOutput,
                    outputPages
                );
            }
            Map<Key, long[]> actual = new HashMap<>();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(2 + aggregations));
                LongBlock longBlock = page.getBlock(0);
                IntBlock intBlock = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long[] counts = new long[aggregations];
                    for (int a = 0; a < aggregations; a++) {
                        counts[a] = ((LongBlock) page.getBlock(2 + a)).getLong(i);
                    }
                    Key key = new Key(longBlock.isNull(i) ? null : longBlock.getLong(i), intBlock.isNull(i) ? null : intBlock.getInt(i));
                    assertNull(actual.put(key, counts));
                }
            }
            assertThat(actual.keySet(), equalTo(expected.keySet()));
            for (Key k : actual.keySet()) {
                assertArrayEquals(k.toString(), actual.get(k), expected.get(k));
            }
        } finally {
            Releasables.close(outputPages);
        }
        return status;
    }

    private DriverStatus runSinglePassAggregation(
        DriverContext driverContext,
        SourceOperator sourceOperator,
        Function<DriverContext, BlockHash> blockHashSupplier,
        int numAggregations,
        HashAggregationOperator.ParallelConfig parallelConfig,
        List<Page> outputPages
    ) {
        HashAggregationOperator hashOperator = new HashAggregationOperator(
            AggregatorMode.SINGLE,
            aggregatorFactories(AggregatorMode.SINGLE, numAggregations),
            blockHashSupplier,
            randomIntBetween(1, 1024),
            randomDouble(),
            randomIntBetween(128, 4096),
            null,
            driverContext,
            parallelConfig,
            randomBoolean()
        );
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                sourceOperator,
                List.of(hashOperator),
                new TestResultPageSinkOperator(outputPages::add),
                TimeValue.timeValueNanos(randomIntBetween(1, 1000_000_000)),
                () -> {}
            )
        ) {
            new TestDriverRunner().run(d);
            return d.status();
        }
    }

    private DriverStatus runTwoPassesAggregation(
        DriverContext driverContext,
        @Nullable DriverContext finalDriverContext,
        SourceOperator rawInputSource,
        Function<DriverContext, BlockHash> blockHashSupplier,
        int numAggregations,
        HashAggregationOperator.ParallelConfig parallelConfig,
        boolean allowPartitionedOutput,
        List<Page> outputPages
    ) {
        final List<Page> partialPages = new ArrayList<>();
        try {
            HashAggregationOperator initialHash = new HashAggregationOperator(
                AggregatorMode.INITIAL,
                aggregatorFactories(AggregatorMode.INITIAL, numAggregations),
                blockHashSupplier,
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                driverContext,
                parallelConfig,
                allowPartitionedOutput
            );
            try (Driver d = TestDriverFactory.create(driverContext, rawInputSource, List.of(initialHash), new PageConsumerOperator(p -> {
                p.allowPassingToDifferentDriver();
                partialPages.add(p);
            }), TimeValue.timeValueNanos(randomIntBetween(1, 1000_000_000)), () -> {})) {
                new TestDriverRunner().run(d);
            }
            try (SourceOperator partialInputSource = new CannedSourceOperator(partialPages.iterator())) {
                DriverContext finalDriveContext = finalDriverContext != null
                    ? finalDriverContext
                    : driverContext(driverContext.blockFactory().parent());
                HashAggregationOperator finalHash = new HashAggregationOperator(
                    AggregatorMode.FINAL,
                    aggregatorFactories(AggregatorMode.FINAL, numAggregations),
                    blockHashSupplier,
                    randomIntBetween(1, 1024),
                    randomDouble(),
                    randomIntBetween(128, 4096),
                    null,
                    finalDriveContext,
                    parallelConfig,
                    true
                );
                try (
                    Driver d = TestDriverFactory.create(
                        finalDriveContext,
                        partialInputSource,
                        List.of(finalHash),
                        new TestResultPageSinkOperator(outputPages::add),
                        TimeValue.timeValueNanos(randomIntBetween(1, 1000_000_000)),
                        () -> {}
                    )
                ) {
                    new TestDriverRunner().run(d);
                    return d.status();
                }
            }
        } finally {
            Releasables.close(partialPages);
        }
    }

    private List<GroupingAggregator.Factory> aggregatorFactories(AggregatorMode mode, int aggregations) {
        List<GroupingAggregator.Factory> factories = new ArrayList<>();
        for (int c = 0; c < aggregations; c++) {
            if (mode.isInputPartial()) {
                int valueChanel = 2 + c * 2;
                factories.add(
                    new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(mode, List.of(valueChanel, valueChanel + 1))
                );
            } else {
                factories.add(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(mode, List.of(2 + c)));
            }
        }
        return factories;
    }

    static Map<Key, long[]> expected(List<Row> rows, int countAggregations) {
        Map<Key, long[]> expected = new HashMap<>();
        for (Row row : rows) {
            expected.compute(row.key, (k, v) -> {
                long[] sum = new long[countAggregations];
                for (int i = 0; i < countAggregations; i++) {
                    sum[i] = (v != null ? v[i] : 0) + row.sums[i];
                }
                return sum;
            });
        }
        return expected;
    }

    static List<Page> inputPages(BlockFactory blockFactory, List<Row> rows, int aggregations) {
        return inputPages(blockFactory, rows, aggregations, 1024);
    }

    static List<Page> inputPages(BlockFactory blockFactory, List<Row> rows, int aggregations, int maxPageSize) {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < rows.size();) {
                int end = Math.min(rows.size(), start + between(1, maxPageSize));
                pages.add(inputPage(blockFactory, rows.subList(start, end), aggregations));
                start = end;
            }
            success = true;
            return pages;
        } finally {
            if (success == false) {
                Releasables.close(pages);
            }
        }
    }

    static Page inputPage(BlockFactory blockFactory, List<Row> rows, int sumAggregations) {
        Block[] blocks = new Block[2 + sumAggregations];
        List<IntBlock.Builder> sumBuilders = new ArrayList<>(sumAggregations);
        boolean success = false;
        try (
            LongBlock.Builder longKeys = blockFactory.newLongBlockBuilder(rows.size());
            IntBlock.Builder intKeys = blockFactory.newIntBlockBuilder(rows.size())
        ) {
            for (int a = 0; a < sumAggregations; a++) {
                sumBuilders.add(blockFactory.newIntBlockBuilder(rows.size()));
            }
            for (Row row : rows) {
                if (row.key.longValue == null) {
                    longKeys.appendNull();
                } else {
                    longKeys.appendLong(row.key.longValue);
                }
                if (row.key.intValue == null) {
                    intKeys.appendNull();
                } else {
                    intKeys.appendInt(row.key.intValue);
                }
                for (int a = 0; a < sumAggregations; a++) {
                    sumBuilders.get(a).appendInt(row.sums[a]);
                }
            }
            blocks[0] = longKeys.build();
            blocks[1] = intKeys.build();
            for (int a = 0; a < sumAggregations; a++) {
                blocks[2 + a] = sumBuilders.get(a).build();
            }
            success = true;
            return new Page(blocks);
        } finally {
            Releasables.close(sumBuilders);
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_TEST_EXECUTOR,
                between(1, 32),
                randomIntBetween(1, 1024),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            ),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                SMALL_TEST_EXECUTOR,
                between(1, 2),
                randomIntBetween(1, 4),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() throws Exception {
        try {
            // the scopes complete on this pool: close them, then the stores, before it stops
            closeSpillingScopes();
            for (MemorySpillStore store : stores) {
                PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
                store.shutdown(shutdown);
                // a store that holds its completions needs them released for the shutdown to land
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (shutdown.isDone() == false) {
                    store.releaseAll();
                    assertTrue("the store did not shut down in time", System.nanoTime() < deadline);
                    Thread.onSpinWait();
                }
                shutdown.get();
            }
        } finally {
            if (threadPool != null) {
                terminate(threadPool);
                threadPool = null;
            }
        }
    }
}
