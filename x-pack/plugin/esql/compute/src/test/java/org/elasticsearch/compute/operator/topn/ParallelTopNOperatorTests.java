/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end smoke tests for the parallel-workers path in {@link TopNOperator}.
 * Driven through a real {@link Driver} so {@link org.elasticsearch.compute.operator.Operator#isBlocked()} is exercised.
 */
public class ParallelTopNOperatorTests extends ComputeTestCase {

    private static final int WORKER_COUNT = 4;
    private static final int MAX_IN_FLIGHT = 8;

    private ExecutorService backingPool;
    private final Set<String> observedThreads = ConcurrentHashMap.newKeySet();

    @After
    public void shutdownExecutor() throws InterruptedException {
        if (backingPool != null) {
            backingPool.shutdown();
            assertTrue("executor did not terminate in time", backingPool.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    public void testParallelLongTopNRandomThreshold() throws Exception {
        long threshold = randomLongBetween(50, 500);
        int rowsPerPage = randomIntBetween(5, 50);
        runAndAssertCorrect(1_000, rowsPerPage, threshold);
    }

    public void testParallelLongTopNZeroThreshold() throws Exception {
        runAndAssertCorrect(1_000, randomIntBetween(5, 50), 0);
    }

    public void testParallelLongTopNNearEndThreshold() throws Exception {
        runAndAssertCorrect(1_000, 10, 950);
    }

    public void testParallelUsesMultipleThreads() throws Exception {
        runAndAssertCorrect(2_000, 10, 0);
        assertThat(
            "expected more than one executor thread to run worker tasks; saw " + observedThreads,
            observedThreads.size(),
            greaterThan(1)
        );
    }

    private void runAndAssertCorrect(int totalRows, int rowsPerPage, long promotionThreshold) throws Exception {
        backingPool = Executors.newFixedThreadPool(WORKER_COUNT);
        java.util.concurrent.Executor recordingExecutor = task -> backingPool.execute(() -> {
            observedThreads.add(Thread.currentThread().getName());
            task.run();
        });

        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        int topCount = 10;
        List<Long> values = new ArrayList<>(totalRows);
        for (long v = 0; v < totalRows; v++) {
            values.add(v);
        }
        Collections.shuffle(values, random());

        List<Page> results = new ArrayList<>();
        boolean success = false;
        try {
            TopNOperator topN = new TopNOperator(
                blockFactory,
                blockFactory.breaker(),
                topCount,
                List.of(ElementType.LONG),
                List.of(TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                null,
                driverContext,
                new TopNOperator.ParallelWorkerConfig(recordingExecutor, WORKER_COUNT, MAX_IN_FLIGHT, promotionThreshold)
            );
            try (
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    new SequenceLongBlockSourceOperator(blockFactory, values, rowsPerPage),
                    List.of(topN),
                    new TestResultPageSinkOperator(results::add)
                )
            ) {
                new TestDriverRunner().run(driver);
            }

            List<Long> actual = new ArrayList<>(topCount);
            for (Page out : results) {
                LongBlock block = out.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    actual.add(block.getLong(block.getFirstValueIndex(i)));
                }
            }

            List<Long> expected = new ArrayList<>(topCount);
            for (long v = 0; v < topCount; v++) {
                expected.add(v);
            }
            assertThat(actual, equalTo(expected));
            success = true;
        } finally {
            if (success) {
                Releasables.close(() -> results.forEach(Page::releaseBlocks));
            } else {
                Releasables.closeExpectNoException(() -> results.forEach(Page::releaseBlocks));
            }
        }
    }
}
