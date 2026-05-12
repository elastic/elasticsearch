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
 * Smoke test for the parallel-final-merge path in {@link TopNOperator}: pushes enough
 * rows to trigger promotion, then verifies the top-K output matches what a sequential
 * sort would produce. Drives the operator directly from the test thread (no Driver).
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
        // Push past PROMOTION_THRESHOLD_ROWS so the operator actually promotes.
        int totalRows = (int) (TopNOperator.PROMOTION_THRESHOLD_ROWS + 10_000);
        int rowsPerPage = 1_000;

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
                new TopNOperator.ParallelWorkerConfig(executor, 4, 8)
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
