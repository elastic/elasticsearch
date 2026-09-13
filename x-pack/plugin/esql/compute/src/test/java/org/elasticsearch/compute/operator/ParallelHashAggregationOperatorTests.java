/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
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
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class ParallelHashAggregationOperatorTests extends ComputeTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";
    private static final String SMALL_TEST_EXECUTOR = "esql_small_test_executor";

    private TestThreadPool threadPool;
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());

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

    record Key(long longValue, int intValue) {}

    record Row(Key key, int[] sums) {}

    DriverStatus runTest(
        int numValues,
        boolean singlePass,
        boolean allowPartitionedOutput,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        int aggregations = between(0, 4);
        List<Row> inputRows = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            int[] sums = new int[aggregations];
            for (int v = 0; v < aggregations; v++) {
                sums[v] = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Key key = new Key(randomLongBetween(0, numValues * 2L), randomIntBetween(0, numValues * 2));
            inputRows.add(new Row(key, sums));
        }
        Map<Key, long[]> expected = expected(inputRows, aggregations);
        List<Page> inputPages = inputPages(sourceBlockFactory, inputRows, aggregations);
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
                    assertNull(actual.put(new Key(longBlock.getLong(i), intBlock.getInt(i)), counts));
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
                DriverContext finalDriveContext = driverContext(driverContext.blockFactory().parent());
                HashAggregationOperator finalHash = new HashAggregationOperator(
                    AggregatorMode.FINAL,
                    aggregatorFactories(AggregatorMode.FINAL, numAggregations),
                    blockHashSupplier,
                    randomIntBetween(1, 1024),
                    randomDouble(),
                    randomIntBetween(128, 4096),
                    null,
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
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < rows.size();) {
                int end = Math.min(rows.size(), start + between(1, 1024));
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
                longKeys.appendLong(row.key.longValue);
                intKeys.appendInt(row.key.intValue);
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
    public void shutdownThreadPool() {
        if (threadPool != null) {
            terminate(threadPool);
            threadPool = null;
        }
    }
}
