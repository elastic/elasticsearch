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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
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
 * Smoke tests for the parallel-final-merge path in {@link TopNOperator}: push enough rows
 * to trigger promotion, then verify the top-K output matches what a sequential sort would
 * produce. Drives the operator directly from the test thread (no Driver).
 *
 * <p>The {@link #testParallelUsesMultipleThreads()} variant additionally asserts that the
 * post-promotion work actually distributes across multiple executor threads — otherwise the
 * parallel path would be a no-op.
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

    /**
     * Correctness check across a random small threshold: the boundary between sequential
     * and parallel modes lands at a different point on each CI run.
     */
    public void testParallelLongTopNRandomThreshold() throws Exception {
        long threshold = randomLongBetween(50, 500);
        int rowsPerPage = randomIntBetween(5, 50);
        runAndAssertCorrect(1_000, rowsPerPage, threshold);
    }

    /** Promote on the very first page so almost all work runs in parallel. */
    public void testParallelLongTopNZeroThreshold() throws Exception {
        runAndAssertCorrect(1_000, randomIntBetween(5, 50), 0);
    }

    /** Threshold near the end of the input — promotion fires with only a handful of pages left. */
    public void testParallelLongTopNNearEndThreshold() throws Exception {
        runAndAssertCorrect(1_000, 10, 950);
    }

    /**
     * Push enough post-promotion pages that we can verify the work distributes across
     * more than one executor thread.
     */
    public void testParallelUsesMultipleThreads() throws Exception {
        // Threshold 0 → every page runs in parallel. 2000 rows across small pages gives
        // many pages and many round-robin cycles, so multiple threads should pick up tasks.
        runAndAssertCorrect(2_000, 10, 0);
        assertThat(
            "expected more than one executor thread to run worker tasks; saw " + observedThreads,
            observedThreads.size(),
            greaterThan(1)
        );
    }

    private void runAndAssertCorrect(int totalRows, int rowsPerPage, long promotionThreshold) throws Exception {
        backingPool = Executors.newFixedThreadPool(WORKER_COUNT);
        // Wrap so we can observe which threads actually ran worker tasks.
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

        List<Long> actual = new ArrayList<>(topCount);
        try (
            TopNOperator op = new TopNOperator(
                blockFactory,
                blockFactory.breaker(),
                topCount,
                List.of(ElementType.LONG),
                List.of(TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),    // ASC, nulls last
                Integer.MAX_VALUE,                                      // maxPageRows
                Long.MAX_VALUE,                                         // jumboPageBytes (no splitting)
                TopNOperator.InputOrdering.NOT_SORTED,
                null,
                driverContext,
                new TopNOperator.ParallelWorkerConfig(recordingExecutor, WORKER_COUNT, MAX_IN_FLIGHT, promotionThreshold)
            )
        ) {
            for (int start = 0; start < totalRows; start += rowsPerPage) {
                int end = Math.min(start + rowsPerPage, totalRows);
                try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(end - start)) {
                    for (int i = start; i < end; i++) {
                        b.appendLong(values.get(i));
                    }
                    while (op.needsInput() == false) {
                        Thread.sleep(1);
                    }
                    op.addInput(new Page(b.build()));
                }
            }
            op.finish();

            // getOutput() returns null until all workers drain and the merge produces a page.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out == null) {
                    if (System.nanoTime() > deadline) {
                        fail("operator did not finish within 30s");
                    }
                    Thread.sleep(1);
                    continue;
                }
                try {
                    LongBlock block = out.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        actual.add(block.getLong(block.getFirstValueIndex(i)));
                    }
                } finally {
                    out.releaseBlocks();
                }
            }
        }

        List<Long> expected = new ArrayList<>(topCount);
        for (long v = 0; v < topCount; v++) {
            expected.add(v);
        }
        assertThat(actual, equalTo(expected));
    }
}
