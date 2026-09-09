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
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
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

import static org.hamcrest.Matchers.equalTo;
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
        runTest(between(100, 1000), driverContext.blockFactory(), driverContext, config);
    }

    public void testLarge() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), driverContext.blockFactory(), driverContext(), config);
    }

    public void testRejectionButOkay() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            threadPool.executor(SMALL_TEST_EXECUTOR),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), driverContext.blockFactory(), driverContext(), config);
    }

    public void testCranky() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 1024)
        );
        try {
            runTest(between(1000, 100 * 1000), driverContext().blockFactory(), crankyDriverContext(), config);
        } catch (CircuitBreakingException ignored) {

        }
    }

    public void testStatus() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            1024
        );
        DriverContext driverContext = driverContext();
        var status = runTest(4096, driverContext.blockFactory(), driverContext, config);
        assertThat(status.completedOperators(), hasSize(3));
        OperatorStatus operatorStatus = status.completedOperators().get(1);
        assertThat(operatorStatus.operator(), equalTo("ParallelHashAggregationOperator"));
    }

    record Key(long longValue, int intValue) {}

    record Row(Key key, long[] counts) {}

    DriverStatus runTest(
        int numValues,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        int countAggregations = between(0, 4);
        List<Row> inputRows = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            long[] counts = new long[countAggregations];
            for (int v = 0; v < countAggregations; v++) {
                counts[v] = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Key key = new Key(randomLongBetween(0, numValues * 2L), randomIntBetween(0, numValues * 2));
            inputRows.add(new Row(key, counts));
        }
        List<GroupingAggregator.Factory> aggregatorFactories = new ArrayList<>(countAggregations);
        for (int a = 0; a < countAggregations; a++) {
            final int valueChannel = 2 + 2 * a;
            aggregatorFactories.add(
                CountAggregatorFunction.supplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(valueChannel, valueChannel + 1))
            );
        }
        Map<Key, long[]> expected = expected(inputRows, countAggregations);
        List<Page> inputPages = inputPages(sourceBlockFactory, inputRows, countAggregations);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT));
        List<Page> outputPages = new ArrayList<>();
        final DriverStatus status;
        try (SourceOperator sourceOperator = new CannedSourceOperator(inputPages.iterator())) {
            HashAggregationOperator hashOperator = new HashAggregationOperator(
                AggregatorMode.FINAL,
                aggregatorFactories,
                dc -> BlockHash.build(groupSpecs, dc.blockFactory(), between(128, 1024), false),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                driverContext,
                parallelConfig
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
                status = d.status();
            }
            Map<Key, long[]> actual = new HashMap<>();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(2 + countAggregations));
                LongBlock longBlock = page.getBlock(0);
                IntBlock intBlock = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long[] counts = new long[countAggregations];
                    for (int a = 0; a < countAggregations; a++) {
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

    static Map<Key, long[]> expected(List<Row> rows, int countAggregations) {
        Map<Key, long[]> expected = new HashMap<>();
        for (Row row : rows) {
            expected.compute(row.key, (k, v) -> {
                if (v == null) {
                    return row.counts;
                } else {
                    long[] sum = new long[countAggregations];
                    for (int i = 0; i < countAggregations; i++) {
                        sum[i] = v[i] + row.counts[i];
                    }
                    return sum;
                }
            });
        }
        return expected;
    }

    static List<Page> inputPages(BlockFactory blockFactory, List<Row> rows, int countAggregations) {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < rows.size();) {
                int end = Math.min(rows.size(), start + between(1, 1024));
                pages.add(inputPage(blockFactory, rows.subList(start, end), countAggregations));
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

    static Page inputPage(BlockFactory blockFactory, List<Row> rows, int countAggregations) {
        Block[] blocks = new Block[2 + countAggregations * 2];
        List<LongBlock.Builder> countBuilders = new ArrayList<>(countAggregations);
        boolean success = false;
        try (
            LongBlock.Builder longKeys = blockFactory.newLongBlockBuilder(rows.size());
            IntBlock.Builder intKeys = blockFactory.newIntBlockBuilder(rows.size())
        ) {
            for (int a = 0; a < countAggregations; a++) {
                countBuilders.add(blockFactory.newLongBlockBuilder(rows.size()));
            }
            for (Row row : rows) {
                longKeys.appendLong(row.key.longValue);
                intKeys.appendInt(row.key.intValue);
                for (int a = 0; a < countAggregations; a++) {
                    countBuilders.get(a).appendLong(row.counts[a]);
                }
            }
            blocks[0] = longKeys.build();
            blocks[1] = intKeys.build();
            for (int a = 0; a < countAggregations; a++) {
                blocks[2 + 2 * a] = countBuilders.get(a).build();
                blocks[2 + 2 * a + 1] = blockFactory.newConstantBooleanBlockWith(true, rows.size());
            }
            success = true;
            return new Page(blocks);
        } finally {
            Releasables.close(countBuilders);
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
