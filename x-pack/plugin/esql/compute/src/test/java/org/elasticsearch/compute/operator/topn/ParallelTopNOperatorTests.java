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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Smoke test for the parallel-final-merge code path in {@link TopNOperator}. Verifies that
 * after the row threshold trips and the operator promotes itself to use a worker pool, the
 * end-to-end output still matches the top-K of the shuffled input.
 *
 * <p>Intentionally minimal: drives the operator directly from the test thread (no Driver
 * loop) and spin-polls until in-flight work has drained. For broader coverage, the existing
 * {@link TopNOperatorTests} continues to exercise the sequential code path.
 */
public class ParallelTopNOperatorTests extends ComputeTestCase {

    private ExecutorService executor;

    @After
    public void shutdownExecutor() throws InterruptedException {
        if (executor != null) {
            executor.shutdown();
            assertTrue("executor did not terminate in time", executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    public void testParallelLongTopN() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        int topCount = 10;
        int totalRows = 1_000;        // well above the promotion threshold below
        int rowsPerPage = 20;
        long promotionThresholdRows = 100;

        // Build a shuffled list of distinct longs [0, totalRows).
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
                null
            ).enableParallelFinalMerge(driverContext, executor, 4, 8, promotionThresholdRows)
        ) {
            // Push pages.
            for (int start = 0; start < totalRows; start += rowsPerPage) {
                int end = Math.min(start + rowsPerPage, totalRows);
                try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(end - start)) {
                    for (int i = start; i < end; i++) {
                        b.appendLong(values.get(i));
                    }
                    while (op.needsInput() == false) {
                        Thread.sleep(1);   // wait for backpressure to clear
                    }
                    op.addInput(new Page(b.build()));
                }
            }
            op.finish();

            // Drain output. After finish(), getOutput() returns null until all workers
            // drain and the merge produces its first page.
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

        // Expected = top-K (ascending) of the shuffled input = [0, 1, 2, ..., K-1].
        List<Long> expected = new ArrayList<>(topCount);
        for (long v = 0; v < topCount; v++) {
            expected.add(v);
        }
        assertThat(actual, equalTo(expected));
    }
}
