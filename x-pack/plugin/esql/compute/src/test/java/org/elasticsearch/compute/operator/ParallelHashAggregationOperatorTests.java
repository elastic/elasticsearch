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
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongIntBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    DriverStatus runTest(
        int numValues,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        List<Tuple<Long, Integer>> inputValues = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            inputValues.add(Tuple.tuple(randomLongBetween(0, numValues * 2L), randomIntBetween(0, numValues * 2)));
        }
        var specs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT));
        List<Page> outputPages = new ArrayList<>();
        final DriverStatus status;
        try {
            HashAggregationOperator hashOperator = new HashAggregationOperator(
                AggregatorMode.SINGLE,
                List.of(),
                dc -> BlockHash.build(specs, dc.blockFactory(), between(128, 1024), false),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                driverContext,
                parallelConfig
            );
            try (
                SourceOperator sourceOperator = new LongIntBlockSourceOperator(sourceBlockFactory, inputValues);
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
            Set<Tuple<Long, Integer>> expected = new HashSet<>(inputValues);
            inputValues.clear();
            Set<Tuple<Long, Integer>> actual = new HashSet<>();
            for (Page page : outputPages) {
                LongBlock longBlock = page.getBlock(0);
                IntBlock intBlock = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertTrue(actual.add(Tuple.tuple(longBlock.getLong(i), intBlock.getInt(i))));
                }
            }
            assertThat(actual, equalTo(expected));
        } finally {
            Releasables.close(outputPages);
        }
        return status;
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
