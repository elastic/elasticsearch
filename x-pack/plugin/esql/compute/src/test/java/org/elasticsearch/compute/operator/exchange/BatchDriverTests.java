/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class BatchDriverTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(BatchDriverTests.class);

    /**
     * Test with an empty batch (0 pages) using a marker page.
     */
    public void testEmptyBatch() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 1;
            List<List<Page>> batches = List.of(new ArrayList<>());
            List<List<Page>> expectedOutputBatches = List.of(new ArrayList<>());
            runBatchTest(driverContext, threadPool, batches, expectedOutputBatches, numBatches);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with a batch containing exactly one page.
     */
    public void testSinglePageSingleBatch() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 1;
            // Create a single batch with exactly 1 page, 3 positions
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 1, 1, 3, 3);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), numBatches);

        } finally {
            terminate(threadPool);
        }
    }

    public void testMultiPageSingleBatch() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 1;
            // Create a single batch with 2 pages, 3 positions each
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 2, 2, 3, 3);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), numBatches);

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with multiple batches, each containing exactly one page.
     */
    public void testSinglePageMultiBatch() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 3;
            // Create 3 batches with exactly 1 page each, 3 positions per page
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 1, 1, 3, 3);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), numBatches);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with multiple batches, each containing multiple pages.
     */
    public void testMultiPageMultiBatch() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 3;
            // Create 3 batches with exactly 2 pages each, 3 positions per page
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 2, 2, 3, 3);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), numBatches);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with mixed batches: empty, single-page, and multi-page batches.
     */
    public void testMixedBatches() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            // Batch 0: empty, Batch 1: 1 page, Batch 2: 3 pages, Batch 3: 1 page
            TestData testData = createTestBatchesWithPageCounts(driverContext, 2, 0, 1, 3, 1);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), 4);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test that verifies we fail when receiving a page for batch 2 while still processing batch 1,
     * without calling finish() on the exchange sink.
     */
    public void testOutOfOrderBatchPages() throws Exception {
        assertOutOfOrderBatchPagesDetected(false);
    }

    /**
     * Test that verifies we fail when receiving a page for batch 2 while still processing batch 1,
     * even after finish() is called on the exchange sink.
     */
    public void testOutOfOrderBatchPagesAfterFinish() throws Exception {
        assertOutOfOrderBatchPagesDetected(true);
    }

    /**
     * Test with 5 batches where a filter operator drops everything in batches 2 and 3.
     * This tests that batch output tracking works correctly even when some batches produce no output due to filtering.
     */
    public void testFilterBatches() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 5;
            List<List<Page>> batches = new ArrayList<>();
            List<List<Page>> expectedOutputBatches = new ArrayList<>();

            // Create 5 batches, each with 2 pages
            // Add a second column: value 1 for batches 2 and 3, value 0 for others
            for (int batchId = 0; batchId < numBatches; batchId++) {
                List<Page> batchPages = new ArrayList<>();
                List<Page> expectedOutputPages = new ArrayList<>();

                for (int pageIdx = 0; pageIdx < 2; pageIdx++) {
                    // First column: data values
                    IntBlock dataBlock = driverContext.blockFactory()
                        .newIntBlockBuilder(3)
                        .appendInt(batchId * 100 + pageIdx * 10)
                        .appendInt(batchId * 100 + pageIdx * 10 + 1)
                        .appendInt(batchId * 100 + pageIdx * 10 + 2)
                        .build();

                    // Second column: filter column (1 for batches 2 and 3, 0 for others)
                    int filterValue = (batchId == 2 || batchId == 3) ? 1 : 0;
                    IntBlock filterBlock = driverContext.blockFactory()
                        .newIntBlockBuilder(3)
                        .appendInt(filterValue)
                        .appendInt(filterValue)
                        .appendInt(filterValue)
                        .build();

                    Page inputPage = new Page(dataBlock, filterBlock);

                    // Expected output: only for batches 0, 1, 4 (batches 2 and 3 are filtered out)
                    if (batchId != 2 && batchId != 3) {
                        // After addOneOperator: first column values + 1, second column unchanged
                        IntBlock expectedDataBlock = driverContext.blockFactory()
                            .newIntBlockBuilder(3)
                            .appendInt(batchId * 100 + pageIdx * 10 + 1)
                            .appendInt(batchId * 100 + pageIdx * 10 + 2)
                            .appendInt(batchId * 100 + pageIdx * 10 + 3)
                            .build();
                        IntBlock expectedFilterBlock = driverContext.blockFactory()
                            .newIntBlockBuilder(3)
                            .appendInt(filterValue)
                            .appendInt(filterValue)
                            .appendInt(filterValue)
                            .build();
                        Page expectedOutputPage = new Page(expectedDataBlock, expectedFilterBlock);
                        expectedOutputPages.add(expectedOutputPage);
                    }

                    batchPages.add(inputPage);
                }

                batches.add(batchPages);
                expectedOutputBatches.add(expectedOutputPages);
            }

            // Track processed batches
            List<List<Page>> actualOutputBatches = new ArrayList<>();
            for (int i = 0; i < numBatches; i++) {
                actualOutputBatches.add(new ArrayList<>());
            }

            // Set up exchange
            ExchangeSetup exchange = setupExchange(driverContext, threadPool);

            // Create feed batch runnable
            AtomicInteger batchesSent = new AtomicInteger(0);
            Runnable feedBatch = createFeedBatchRunnable(batches, exchange.exchangeSink, numBatches, batchesSent);

            // Create operators: addOneOperator and filterOperator
            EvalOperator addOneOperator = createAddOneOperator(driverContext);

            // Create filter operator that drops rows where the filter column (block 1) equals 1
            FilterOperator filterOperator = new FilterOperator(createNotEqualsOneEvaluator(driverContext, 1));

            List<Page> allOutputPages = new ArrayList<>();
            TestResultPageSinkOperator sinkOperator = createSinkOperatorWithBatchDetection(allOutputPages, actualOutputBatches, numBatches);

            // Create BatchDriver with addOneOperator and filterOperator
            BatchDriver batchDriver = new BatchDriver(
                "test-session",
                "test",
                "test-cluster",
                "test-node",
                System.currentTimeMillis(),
                System.nanoTime(),
                driverContext,
                () -> "test",
                exchange.sourceOperator,
                List.of(addOneOperator, filterOperator),
                BatchDriver.wrapSink(new MarkerFilteringSinkOperator(sinkOperator, feedBatch)),
                TimeValue.timeValueSeconds(1),
                () -> {}
            );

            // Run driver and wait (batch feeding runs in a separate thread)
            runDriverAndWait(threadPool, batchDriver, feedBatch);

            // Verify batch processing completion
            verifyBasicBatchResults(batchesSent, numBatches);

            // Verify output data - batches 2 and 3 should have no output (filtered out)
            assertThat("Batch 0 should have 2 output pages", actualOutputBatches.get(0).size(), equalTo(2));
            assertThat("Batch 1 should have 2 output pages", actualOutputBatches.get(1).size(), equalTo(2));
            assertThat("Batch 2 should have no output pages (filtered out)", actualOutputBatches.get(2).size(), equalTo(0));
            assertThat("Batch 3 should have no output pages (filtered out)", actualOutputBatches.get(3).size(), equalTo(0));
            assertThat("Batch 4 should have 2 output pages", actualOutputBatches.get(4).size(), equalTo(2));

            // Verify each batch's output (only for batches that weren't filtered)
            for (int batchId = 0; batchId < numBatches; batchId++) {
                List<Page> expectedPages = expectedOutputBatches.get(batchId);
                List<Page> actualPages = actualOutputBatches.get(batchId);

                assertThat("Batch " + batchId + " should have expected number of pages", actualPages.size(), equalTo(expectedPages.size()));

                for (int pageIdx = 0; pageIdx < expectedPages.size(); pageIdx++) {
                    Page expectedPage = expectedPages.get(pageIdx);
                    Page actualPage = actualPages.get(pageIdx);

                    assertThat(
                        "Batch " + batchId + " page " + pageIdx + " position count",
                        actualPage.getPositionCount(),
                        equalTo(expectedPage.getPositionCount())
                    );

                    // After addOneOperator, a new block is added, so we have:
                    // Block 0: original data block
                    // Block 1: original filter block
                    // Block 2: new block with data values + 1
                    // After filterOperator, all blocks are filtered but structure remains
                    assertThat("Batch " + batchId + " page " + pageIdx + " should have 3 blocks", actualPage.getBlockCount(), equalTo(3));
                    assertThat(
                        "Batch " + batchId + " page " + pageIdx + " should have 2 blocks in expected",
                        expectedPage.getBlockCount(),
                        equalTo(2)
                    );

                    // Verify data column (block 2) - values should be incremented by 1
                    IntBlock expectedDataBlock = (IntBlock) expectedPage.getBlock(0);
                    IntBlock actualDataBlock = (IntBlock) actualPage.getBlock(2);
                    for (int p = 0; p < expectedPage.getPositionCount(); p++) {
                        int expectedValue = expectedDataBlock.getInt(expectedDataBlock.getFirstValueIndex(p));
                        int actualValue = actualDataBlock.getInt(actualDataBlock.getFirstValueIndex(p));
                        assertThat(
                            "Batch " + batchId + " page " + pageIdx + " position " + p + " data column",
                            actualValue,
                            equalTo(expectedValue)
                        );
                    }

                    // Verify filter column (block 1) - should be unchanged (0 for batches 0,1,4)
                    IntBlock expectedFilterBlock = (IntBlock) expectedPage.getBlock(1);
                    IntBlock actualFilterBlock = (IntBlock) actualPage.getBlock(1);
                    for (int p = 0; p < expectedPage.getPositionCount(); p++) {
                        int expectedFilterValue = expectedFilterBlock.getInt(expectedFilterBlock.getFirstValueIndex(p));
                        int actualFilterValue = actualFilterBlock.getInt(actualFilterBlock.getFirstValueIndex(p));
                        assertThat(
                            "Batch " + batchId + " page " + pageIdx + " position " + p + " filter column",
                            actualFilterValue,
                            equalTo(expectedFilterValue)
                        );
                    }
                }
            }

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with batches where MvExpandOperator creates more output pages than input pages.
     * This tests that batch output works correctly even when the pipeline splits pages or creates more pages.
     */
    public void testMvExpandCreatesMorePages() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = 3;
            List<List<Page>> batches = new ArrayList<>();
            List<List<Page>> expectedOutputBatches = new ArrayList<>();

            // Create batches with multivalued blocks that will be expanded and split into multiple pages
            // Use a small pageSize (2) so that expanded data gets split across multiple pages
            int pageSize = 2;

            for (int batchId = 0; batchId < numBatches; batchId++) {
                List<Page> batchPages = new ArrayList<>();
                List<Page> expectedOutputPages = new ArrayList<>();

                // Create 1 page per batch with multivalued blocks that expand to 10 rows total
                // With pageSize=2, this will create 5 output pages (2+2+2+2+2)
                int positions = 2;
                IntBlock.Builder mvBlockBuilder = driverContext.blockFactory().newIntBlockBuilder(positions);

                // Position 0: [batchId*100, batchId*100+1, batchId*100+2, batchId*100+3] - expands to 4 rows
                mvBlockBuilder.beginPositionEntry();
                mvBlockBuilder.appendInt(batchId * 100);
                mvBlockBuilder.appendInt(batchId * 100 + 1);
                mvBlockBuilder.appendInt(batchId * 100 + 2);
                mvBlockBuilder.appendInt(batchId * 100 + 3);
                mvBlockBuilder.endPositionEntry();

                // Position 1: [batchId*100+10, batchId*100+11, batchId*100+12, batchId*100+13, batchId*100+14, batchId*100+15] - expands to
                // 6 rows
                mvBlockBuilder.beginPositionEntry();
                mvBlockBuilder.appendInt(batchId * 100 + 10);
                mvBlockBuilder.appendInt(batchId * 100 + 11);
                mvBlockBuilder.appendInt(batchId * 100 + 12);
                mvBlockBuilder.appendInt(batchId * 100 + 13);
                mvBlockBuilder.appendInt(batchId * 100 + 14);
                mvBlockBuilder.appendInt(batchId * 100 + 15);
                mvBlockBuilder.endPositionEntry();

                IntBlock mvBlock = mvBlockBuilder.build();

                // Second column: filter column (value 0 for all)
                IntBlock filterBlock = driverContext.blockFactory().newIntBlockBuilder(positions).appendInt(0).appendInt(0).build();

                Page inputPage = new Page(mvBlock, filterBlock);
                batchPages.add(inputPage);

                // Expected output: after MvExpand, we'll have 10 rows total (4 + 6)
                // With pageSize=2, this will be split into 5 pages: [2, 2, 2, 2, 2]
                // After addOneOperator, values will be incremented by 1
                // We'll create expected pages matching the split
                for (int pageIdx = 0; pageIdx < 5; pageIdx++) {
                    int rowsInPage = 2;
                    IntBlock.Builder expectedDataBuilder = driverContext.blockFactory().newIntBlockBuilder(rowsInPage);
                    IntBlock.Builder expectedFilterBuilder = driverContext.blockFactory().newIntBlockBuilder(rowsInPage);

                    // Calculate which values should be in this page
                    int startRow = pageIdx * 2;
                    for (int r = 0; r < rowsInPage; r++) {
                        int rowIndex = startRow + r;
                        int expectedValue;
                        if (rowIndex < 4) {
                            // First 4 rows from position 0 expansion
                            expectedValue = batchId * 100 + rowIndex + 1; // +1 from addOneOperator
                        } else {
                            // Next 6 rows from position 1 expansion
                            expectedValue = batchId * 100 + 10 + (rowIndex - 4) + 1; // +1 from addOneOperator
                        }
                        expectedDataBuilder.appendInt(expectedValue);
                        expectedFilterBuilder.appendInt(0);
                    }

                    Page expectedOutputPage = new Page(expectedDataBuilder.build(), expectedFilterBuilder.build());
                    expectedOutputPages.add(expectedOutputPage);
                }

                batches.add(batchPages);
                expectedOutputBatches.add(expectedOutputPages);
            }

            // Track processed batches
            List<List<Page>> actualOutputBatches = new ArrayList<>();
            for (int i = 0; i < numBatches; i++) {
                actualOutputBatches.add(new ArrayList<>());
            }

            // Set up exchange
            ExchangeSetup exchange = setupExchange(driverContext, threadPool);

            // Create feed batch runnable
            AtomicInteger batchesSent = new AtomicInteger(0);
            Runnable feedBatch = createFeedBatchRunnable(batches, exchange.exchangeSink, numBatches, batchesSent);

            // Create operators: MvExpandOperator (expands multivalued blocks) and addOneOperator
            // Use small pageSize (2) to force splitting into multiple pages
            // 1 input page with 10 expanded rows will create 5 output pages (2+2+2+2+2)
            MvExpandOperator mvExpandOperator = new MvExpandOperator(0, pageSize);
            EvalOperator addOneOperator = createAddOneOperator(driverContext);

            List<Page> allOutputPages = new ArrayList<>();
            TestResultPageSinkOperator sinkOperator = createSinkOperatorWithBatchDetection(allOutputPages, actualOutputBatches, numBatches);

            // Create BatchDriver with MvExpandOperator and addOneOperator
            BatchDriver batchDriver = new BatchDriver(
                "test-session",
                "test",
                "test-cluster",
                "test-node",
                System.currentTimeMillis(),
                System.nanoTime(),
                driverContext,
                () -> "test",
                exchange.sourceOperator,
                List.of(mvExpandOperator, addOneOperator),
                BatchDriver.wrapSink(new MarkerFilteringSinkOperator(sinkOperator, feedBatch)),
                TimeValue.timeValueSeconds(1),
                () -> {}
            );

            // Run driver and wait (batch feeding runs in a separate thread)
            runDriverAndWait(threadPool, batchDriver, feedBatch);

            // Verify batch processing completion
            verifyBasicBatchResults(batchesSent, numBatches);

            // Verify that we got more output pages than input pages due to MvExpand splitting
            int totalInputPages = batches.stream().mapToInt(List::size).sum();
            int totalOutputPages = allOutputPages.size();
            assertThat(
                "Output should have more pages than input due to MvExpand splitting",
                totalOutputPages,
                greaterThan(totalInputPages)
            );
            // Each batch has 1 input page, should create 5 output pages (10 rows / pageSize 2)
            assertThat("Should have 5 output pages per batch", totalOutputPages, equalTo(numBatches * 5));

            // Verify each batch's output
            for (int batchId = 0; batchId < numBatches; batchId++) {
                List<Page> expectedPages = expectedOutputBatches.get(batchId);
                List<Page> actualPages = actualOutputBatches.get(batchId);

                // Each batch should have 5 output pages (split from 1 input page)
                assertThat("Batch " + batchId + " should have 5 output pages", actualPages.size(), equalTo(5));
                assertThat("Batch " + batchId + " should have 5 expected pages", expectedPages.size(), equalTo(5));

                // Verify each page has the correct number of rows and values
                for (int pageIdx = 0; pageIdx < expectedPages.size(); pageIdx++) {
                    Page expectedPage = expectedPages.get(pageIdx);
                    Page actualPage = actualPages.get(pageIdx);

                    assertThat(
                        "Batch " + batchId + " page " + pageIdx + " should have 2 positions",
                        actualPage.getPositionCount(),
                        equalTo(2)
                    );

                    // Verify data values (block 2 after addOneOperator)
                    if (actualPage.getBlockCount() >= 3) {
                        IntBlock expectedDataBlock = (IntBlock) expectedPage.getBlock(0);
                        IntBlock actualDataBlock = (IntBlock) actualPage.getBlock(2);
                        IntBlock expectedFilterBlock = (IntBlock) expectedPage.getBlock(1);
                        IntBlock actualFilterBlock = (IntBlock) actualPage.getBlock(1);

                        for (int p = 0; p < expectedPage.getPositionCount(); p++) {
                            int expectedValue = expectedDataBlock.getInt(expectedDataBlock.getFirstValueIndex(p));
                            int actualValue = actualDataBlock.getInt(actualDataBlock.getFirstValueIndex(p));
                            assertThat(
                                "Batch " + batchId + " page " + pageIdx + " position " + p + " data value",
                                actualValue,
                                equalTo(expectedValue)
                            );

                            int expectedFilterValue = expectedFilterBlock.getInt(expectedFilterBlock.getFirstValueIndex(p));
                            int actualFilterValue = actualFilterBlock.getInt(actualFilterBlock.getFirstValueIndex(p));
                            assertThat(
                                "Batch " + batchId + " page " + pageIdx + " position " + p + " filter value",
                                actualFilterValue,
                                equalTo(expectedFilterValue)
                            );
                        }
                    }
                }
            }

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test that the driver handles all batches sent upfront correctly.
     */
    public void testBatchProcessingRandomAllBatchesUpfront() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            // Create test data: random number of batches (2-10), 0-10 pages each
            int numBatches = between(2, 10);
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 0, 10, 1, 10);
            List<List<Page>> batches = testData.batches();
            List<List<Page>> expectedOutputBatches = testData.expectedOutputBatches();

            // Track processed batches
            List<List<Page>> actualOutputBatches = new ArrayList<>();
            for (int i = 0; i < numBatches; i++) {
                actualOutputBatches.add(new ArrayList<>());
            }

            // Set up exchange
            ExchangeSetup exchange = setupExchange(driverContext, threadPool);

            // Create feed batch runnable that feeds ALL batches upfront
            AtomicInteger batchesSent = new AtomicInteger(0);
            Runnable feedAllBatches = () -> {
                try {
                    // Feed all batches immediately
                    for (int batchId = 0; batchId < numBatches; batchId++) {
                        if (exchange.exchangeSink.isFinished()) {
                            logger.warn("[TEST] feedAllBatches: Exchange sink already finished, cannot feed batch {}", batchId);
                            break;
                        }

                        List<Page> batchPages = batches.get(batchId);
                        logger.info("[TEST] feedAllBatches: Feeding batch {} with {} pages", batchId, batchPages.size());

                        if (batchPages.isEmpty()) {
                            // Empty batch - send marker page
                            Page marker = Page.createBatchMarkerPage(batchId, 0);
                            waitForExchangeSink(exchange.exchangeSink);
                            exchange.exchangeSink.addPage(marker);
                        } else {
                            for (int pageIdx = 0; pageIdx < batchPages.size(); pageIdx++) {
                                Page page = batchPages.get(pageIdx);
                                boolean isLastPageInBatch = (pageIdx == batchPages.size() - 1);
                                Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(batchId, pageIdx, isLastPageInBatch));

                                waitForExchangeSink(exchange.exchangeSink);
                                exchange.exchangeSink.addPage(pageWithMetadata);
                                logger.debug(
                                    "[TEST] feedAllBatches: Added page {}/{} for batch {} (isLastPageInBatch={})",
                                    pageIdx + 1,
                                    batchPages.size(),
                                    batchId,
                                    isLastPageInBatch
                                );
                            }
                        }
                        batchesSent.incrementAndGet();
                        logger.info("[TEST] feedAllBatches: Finished feeding batch {}", batchId);
                    }

                    // Finish the sink after all batches are sent
                    logger.info("[TEST] feedAllBatches: All batches sent, finishing sink");
                    exchange.exchangeSink.finish();
                } catch (Exception e) {
                    logger.error("[TEST] feedAllBatches: Error feeding batches", e);
                    throw new AssertionError("Error feeding batches", e);
                }
            };

            // Create operators
            EvalOperator addOneOperator = createAddOneOperator(driverContext);
            List<Page> allOutputPages = new ArrayList<>();
            TestResultPageSinkOperator sinkOperator = createSinkOperatorWithBatchDetection(allOutputPages, actualOutputBatches, numBatches);

            // Create BatchDriver - no callback needed since all batches are fed upfront
            BatchDriver batchDriver = createBatchDriver(driverContext, exchange.sourceOperator, addOneOperator, sinkOperator, () -> {});

            // Start driver first
            ThreadContext threadContext = threadPool.getThreadContext();
            PlainActionFuture<Void> driverFuture = new PlainActionFuture<>();
            Driver.start(threadContext, threadPool.executor(ThreadPool.Names.SEARCH), batchDriver, 1000, driverFuture);

            // Feed all batches immediately in a separate thread (not via executor to avoid deadlock)
            Thread batchFeedingThread = new Thread(() -> {
                try {
                    feedAllBatches.run();
                } catch (Exception e) {
                    logger.error("[TEST] Error in batch feeding thread", e);
                    throw new AssertionError("Error in batch feeding thread", e);
                }
            }, "batch-feeding-thread");
            batchFeedingThread.start();

            // Wait for driver to complete - batches sent upfront should now be handled correctly
            driverFuture.actionGet(30, TimeUnit.SECONDS);

            // Wait for batch feeding thread to finish
            batchFeedingThread.join(30000);
            assertThat("Batch feeding thread should have completed", batchFeedingThread.isAlive(), equalTo(false));

            // Verify batch processing completion
            verifyBasicBatchResults(batchesSent, numBatches);

            // Verify data correctness
            verifyDataCorrectness(expectedOutputBatches, actualOutputBatches, numBatches, 1);

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A randomized test for BatchDriver with many batches.
     */
    public void testBatchProcessingRandom() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int numBatches = between(100, 10000);
            TestData testData = createSimpleTestBatches(driverContext, numBatches, 0, 10, 1, 10);
            runBatchTest(driverContext, threadPool, testData.batches(), testData.expectedOutputBatches(), numBatches);
        } finally {
            terminate(threadPool);
        }
    }

    // ========================================
    // Helper Methods
    // ========================================

    private ThreadPool threadPool() {
        // TestThreadPool already includes SEARCH executor by default, no need to add it
        return new TestThreadPool(getTestClass().getSimpleName());
    }

    private DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        return new DriverContext(bigArrays, blockFactory, LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS);
    }

    /**
     * Set up exchange handlers and operators.
     */
    private ExchangeSetup setupExchange(DriverContext driverContext, ThreadPool threadPool) {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(10, threadPool.executor(ThreadPool.Names.SEARCH));
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(driverContext.blockFactory(), 10, System::currentTimeMillis);
        sourceHandler.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());

        ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(sourceHandler.createExchangeSource());
        ExchangeSink exchangeSink = sinkHandler.createExchangeSink(() -> {});

        return new ExchangeSetup(sourceHandler, sinkHandler, sourceOperator, exchangeSink);
    }

    /**
     * Create a feedBatch runnable that feeds batches to the exchange.
     * Supports both regular batches and empty batches (using markers).
     */
    private Runnable createFeedBatchRunnable(
        List<List<Page>> batches,
        ExchangeSink exchangeSink,
        int numBatches,
        AtomicInteger batchesSent
    ) {
        AtomicInteger currentBatchIndex = new AtomicInteger(0);
        return () -> {
            int batchId = currentBatchIndex.getAndIncrement();
            logger.info("[TEST] feedBatch: Starting to feed batch {}", batchId);

            if (batchId >= batches.size()) {
                logger.warn("[TEST] feedBatch: Attempted to feed batch {} but only {} batches available", batchId, batches.size());
                exchangeSink.finish();
                return;
            }

            if (exchangeSink.isFinished()) {
                logger.warn("[TEST] feedBatch: Exchange sink already finished, cannot feed batch {}", batchId);
                return;
            }

            try {
                List<Page> batchPages = batches.get(batchId);
                logger.info("[TEST] feedBatch: Feeding batch {} with {} pages", batchId, batchPages.size());

                if (batchPages.isEmpty()) {
                    // Empty batch - send marker page
                    Page marker = Page.createBatchMarkerPage(batchId, 0);
                    waitForExchangeSink(exchangeSink);
                    exchangeSink.addPage(marker);
                } else {
                    for (int pageIdx = 0; pageIdx < batchPages.size(); pageIdx++) {
                        Page page = batchPages.get(pageIdx);
                        boolean isLastPageInBatch = (pageIdx == batchPages.size() - 1);
                        Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(batchId, pageIdx, isLastPageInBatch));

                        waitForExchangeSink(exchangeSink);
                        exchangeSink.addPage(pageWithMetadata);
                        logger.debug(
                            "[TEST] feedBatch: Added page {}/{} for batch {} (isLastPageInBatch={})",
                            pageIdx + 1,
                            batchPages.size(),
                            batchId,
                            isLastPageInBatch
                        );
                    }
                }
                batchesSent.incrementAndGet();
                logger.info("[TEST] feedBatch: Finished feeding batch {}", batchId);
            } catch (Exception e) {
                logger.error("[TEST] feedBatch: Error feeding batch " + batchId, e);
                throw new AssertionError("Error feeding batch " + batchId, e);
            }
        };
    }

    /**
     * Wait for exchange sink to be ready for writing.
     */
    private void waitForExchangeSink(ExchangeSink exchangeSink) throws Exception {
        IsBlockedResult blocked = exchangeSink.waitForWriting();
        if (blocked.listener().isDone() == false) {
            PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
            blocked.listener().addListener(waitFuture);
            waitFuture.actionGet(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Create a sink operator that collects output pages and categorizes them by batch ID.
     */
    private TestResultPageSinkOperator createSinkOperatorWithBatchDetection(
        List<Page> allOutputPages,
        List<List<Page>> actualOutputBatches,
        int numBatches
    ) {
        return new TestResultPageSinkOperator(page -> {
            allOutputPages.add(page);
            int detectedBatchId = -1;
            // Determine which batch this page belongs to by checking the values
            if (page.getBlockCount() > 0 && page.getBlock(0) instanceof IntBlock) {
                IntBlock block = (IntBlock) page.getBlock(0);
                if (block.getPositionCount() > 0 && block.isNull(0) == false) {
                    int value = block.getInt(block.getFirstValueIndex(0));
                    detectedBatchId = value / 100; // Extract batch ID from value
                }
            }
            if (detectedBatchId >= 0 && detectedBatchId < numBatches) {
                actualOutputBatches.get(detectedBatchId).add(page);
            }
        });
    }

    /**
     * Start the driver and wait for completion.
     * The driver runs in the executor thread pool, while batch feeding runs in a separate thread
     * to help catch timing-related issues.
     */
    private void runDriverAndWait(ThreadPool threadPool, BatchDriver batchDriver, Runnable feedBatch) throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        PlainActionFuture<Void> future = new PlainActionFuture<>();

        // Start driver first (Driver.start() is asynchronous and returns immediately)
        Driver.start(threadContext, threadPool.executor(ThreadPool.Names.SEARCH), batchDriver, 1000, future);

        // Start batch feeding in a separate thread to create concurrent execution
        // This helps catch timing-related issues that might be hidden when everything runs sequentially
        Thread batchFeedingThread = new Thread(() -> {
            try {
                // Feed first batch - this will trigger the callback chain
                threadPool.executor(ThreadPool.Names.SEARCH).execute(feedBatch);
            } catch (Exception e) {
                logger.error("[TEST] Error in batch feeding thread", e);
                throw new AssertionError("Error in batch feeding thread", e);
            }
        }, "batch-feeding-thread");
        batchFeedingThread.start();

        // Wait for driver to complete
        future.actionGet(30, TimeUnit.SECONDS);

        // Wait for batch feeding thread to finish
        batchFeedingThread.join(30000);
        assertThat("Batch feeding thread should have completed", batchFeedingThread.isAlive(), equalTo(false));
    }

    /**
     * Verify basic batch processing results.
     */
    private void verifyBasicBatchResults(AtomicInteger batchesSent, int numBatches) {
        assertThat("All batches should be sent", batchesSent.get(), equalTo(numBatches));
    }

    /**
     * Helper method to run a batch test with given batches and expected outputs.
     */
    private void runBatchTest(
        DriverContext driverContext,
        ThreadPool threadPool,
        List<List<Page>> batches,
        List<List<Page>> expectedOutputBatches,
        int numBatches
    ) throws Exception {
        // Track processed batches
        List<List<Page>> actualOutputBatches = new ArrayList<>();
        for (int i = 0; i < numBatches; i++) {
            actualOutputBatches.add(new ArrayList<>());
        }

        // Set up exchange
        ExchangeSetup exchange = setupExchange(driverContext, threadPool);

        // Create feed batch runnable
        AtomicInteger batchesSent = new AtomicInteger(0);
        Runnable feedBatch = createFeedBatchRunnable(batches, exchange.exchangeSink, numBatches, batchesSent);

        // Create operators
        EvalOperator addOneOperator = createAddOneOperator(driverContext);
        List<Page> allOutputPages = new ArrayList<>();
        TestResultPageSinkOperator sinkOperator = createSinkOperatorWithBatchDetection(allOutputPages, actualOutputBatches, numBatches);

        // Create BatchDriver with callback to feed next batch
        BatchDriver batchDriver = createBatchDriver(driverContext, exchange.sourceOperator, addOneOperator, sinkOperator, feedBatch);

        // Run driver and wait (batch feeding runs in a separate thread)
        runDriverAndWait(threadPool, batchDriver, feedBatch);

        // Verify basic results
        verifyBasicBatchResults(batchesSent, numBatches);

        // Verify data correctness: compare actual output data with expected values
        // Assumes addOneOperator was used, so output block is at index 1 (new block created by EvalOperator)
        verifyDataCorrectness(expectedOutputBatches, actualOutputBatches, numBatches, 1);
    }

    /**
     * Create simple test batches with int values following the pattern: batchId * 100 + pageIdx * 10 + offset.
     * Expected output is input values + 1.
     */
    private TestData createSimpleTestBatches(
        DriverContext driverContext,
        int numBatches,
        int minPagesPerBatch,
        int maxPagesPerBatch,
        int minPositionsPerPage,
        int maxPositionsPerPage
    ) {
        List<List<Page>> batches = new ArrayList<>();
        List<List<Page>> expectedOutputBatches = new ArrayList<>();

        for (int batchId = 0; batchId < numBatches; batchId++) {
            int pagesInBatch = between(minPagesPerBatch, maxPagesPerBatch);
            List<Page> batchPages = new ArrayList<>();
            List<Page> expectedOutputPages = new ArrayList<>();

            for (int pageIdx = 0; pageIdx < pagesInBatch; pageIdx++) {
                // Create input page with int values
                int numPositionsPerPage = between(minPositionsPerPage, maxPositionsPerPage);
                IntBlock.Builder inputBuilder = driverContext.blockFactory().newIntBlockBuilder(numPositionsPerPage);
                IntBlock.Builder expectedBuilder = driverContext.blockFactory().newIntBlockBuilder(numPositionsPerPage);

                // Append 'numPositionsPerPage' number of values
                for (int pos = 0; pos < numPositionsPerPage; pos++) {
                    int inputValue = batchId * 100 + pageIdx * 10 + pos;
                    inputBuilder.appendInt(inputValue);
                    expectedBuilder.appendInt(inputValue + 1);
                }

                IntBlock inputBlock = inputBuilder.build();
                IntBlock expectedOutputBlock = expectedBuilder.build();
                Page inputPage = new Page(inputBlock);
                Page expectedOutputPage = new Page(expectedOutputBlock);

                batchPages.add(inputPage);
                expectedOutputPages.add(expectedOutputPage);
            }

            batches.add(batchPages);
            expectedOutputBatches.add(expectedOutputPages);
        }

        return new TestData(batches, expectedOutputBatches);
    }

    /**
     * Create test batches with exact page counts per batch.
     * Values follow the pattern: batchId * 100 + pageIdx * 10 + pos. Expected output is input + 1.
     * A page count of 0 creates an empty batch (will use a marker page).
     *
     * @param positionsPerPage number of positions per page
     * @param pagesPerBatch exact page counts for each batch
     */
    private TestData createTestBatchesWithPageCounts(DriverContext driverContext, int positionsPerPage, int... pagesPerBatch) {
        List<List<Page>> batches = new ArrayList<>();
        List<List<Page>> expectedOutputBatches = new ArrayList<>();

        for (int batchId = 0; batchId < pagesPerBatch.length; batchId++) {
            List<Page> batchPages = new ArrayList<>();
            List<Page> expectedOutputPages = new ArrayList<>();

            for (int pageIdx = 0; pageIdx < pagesPerBatch[batchId]; pageIdx++) {
                IntBlock.Builder inputBuilder = driverContext.blockFactory().newIntBlockBuilder(positionsPerPage);
                IntBlock.Builder expectedBuilder = driverContext.blockFactory().newIntBlockBuilder(positionsPerPage);
                for (int pos = 0; pos < positionsPerPage; pos++) {
                    int inputValue = batchId * 100 + pageIdx * 10 + pos;
                    inputBuilder.appendInt(inputValue);
                    expectedBuilder.appendInt(inputValue + 1);
                }
                batchPages.add(new Page(inputBuilder.build()));
                expectedOutputPages.add(new Page(expectedBuilder.build()));
            }

            batches.add(batchPages);
            expectedOutputBatches.add(expectedOutputPages);
        }

        return new TestData(batches, expectedOutputBatches);
    }

    /**
     * Verify that actual output pages match expected output pages for all batches.
     * Assumes addOneOperator was used, so actual output block is at index 1 (new block created by EvalOperator).
     */
    private void verifyDataCorrectness(
        List<List<Page>> expectedOutputBatches,
        List<List<Page>> actualOutputBatches,
        int numBatches,
        int expectedOutputBlockIndex
    ) {
        for (int batchId = 0; batchId < numBatches; batchId++) {
            List<Page> expectedPages = expectedOutputBatches.get(batchId);
            List<Page> actualPages = actualOutputBatches.get(batchId);

            assertThat("Batch " + batchId + " should have pages", actualPages.size(), equalTo(expectedPages.size()));

            for (int pageIdx = 0; pageIdx < expectedPages.size(); pageIdx++) {
                Page expectedPage = expectedPages.get(pageIdx);
                Page actualPage = actualPages.get(pageIdx);

                assertThat(
                    "Batch " + batchId + " page " + pageIdx + " position count",
                    actualPage.getPositionCount(),
                    equalTo(expectedPage.getPositionCount())
                );

                if (expectedPage.getBlockCount() > 0 && actualPage.getBlockCount() > expectedOutputBlockIndex) {
                    IntBlock expectedBlock = (IntBlock) expectedPage.getBlock(0);
                    IntBlock actualBlock = (IntBlock) actualPage.getBlock(expectedOutputBlockIndex);

                    for (int p = 0; p < expectedPage.getPositionCount(); p++) {
                        int expectedValue = expectedBlock.getInt(expectedBlock.getFirstValueIndex(p));
                        int actualValue = actualBlock.getInt(actualBlock.getFirstValueIndex(p));
                        assertThat("Batch " + batchId + " page " + pageIdx + " position " + p, actualValue, equalTo(expectedValue));
                    }
                }
            }
        }
    }

    /**
     * Helper that sets up a BatchDriver, feeds batches out of order (batch 2 page while batch 1 is still
     * processing), and asserts that the driver fails with the expected IllegalStateException.
     *
     * @param callFinish whether to call finish() on the exchange sink after sending the out-of-order page
     */
    private void assertOutOfOrderBatchPagesDetected(boolean callFinish) throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            // Create test data: 3 batches
            TestData testData = createSimpleTestBatches(driverContext, 3, 2, 2, 3, 3);
            List<List<Page>> batches = testData.batches();

            // Set up exchange
            ExchangeSetup exchange = setupExchange(driverContext, threadPool);

            // Create operators
            EvalOperator addOneOperator = createAddOneOperator(driverContext);
            SinkOperator sinkOperator = new TestResultPageSinkOperator(page -> {});

            // Create BatchDriver - no callback needed for this error test
            BatchDriver batchDriver = createBatchDriver(driverContext, exchange.sourceOperator, addOneOperator, sinkOperator, () -> {});

            // Start driver
            ThreadContext threadContext = threadPool.getThreadContext();
            PlainActionFuture<Void> driverFuture = new PlainActionFuture<>();
            Driver.start(threadContext, threadPool.executor(ThreadPool.Names.SEARCH), batchDriver, 1000, driverFuture);

            // Feed batches in a way that triggers out-of-order error:
            // 1. Send batch 0 completely
            // 2. Send batch 1 pages but NOT the last page (so batch 1 is still processing)
            // 3. Send batch 2 page (should trigger error)
            // 4. Optionally call finish()
            Thread batchFeedingThread = new Thread(() -> {
                try {
                    // Send batch 0 completely
                    List<Page> batch0Pages = batches.get(0);
                    for (int pageIdx = 0; pageIdx < batch0Pages.size(); pageIdx++) {
                        Page page = batch0Pages.get(pageIdx);
                        boolean isLastPageInBatch = (pageIdx == batch0Pages.size() - 1);
                        Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(0, pageIdx, isLastPageInBatch));
                        waitForExchangeSink(exchange.exchangeSink);
                        exchange.exchangeSink.addPage(pageWithMetadata);
                    }

                    // Send batch 1 pages but NOT the last page (so batch 1 is still processing)
                    List<Page> batch1Pages = batches.get(1);
                    for (int pageIdx = 0; pageIdx < batch1Pages.size() - 1; pageIdx++) {
                        Page page = batch1Pages.get(pageIdx);
                        Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(1, pageIdx, false)); // Not last page
                        waitForExchangeSink(exchange.exchangeSink);
                        exchange.exchangeSink.addPage(pageWithMetadata);
                    }

                    // Now send a page for batch 2 while batch 1 is still processing (not ended)
                    // This should trigger the IllegalStateException
                    List<Page> batch2Pages = batches.get(2);
                    if (batch2Pages.isEmpty() == false) {
                        Page page = batch2Pages.get(0);
                        Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(2, 0, false));
                        waitForExchangeSink(exchange.exchangeSink);
                        exchange.exchangeSink.addPage(pageWithMetadata);
                    } else {
                        // If batch 2 is empty, send a marker
                        Page marker = Page.createBatchMarkerPage(2, 0);
                        waitForExchangeSink(exchange.exchangeSink);
                        exchange.exchangeSink.addPage(marker);
                    }

                    if (callFinish) {
                        exchange.exchangeSink.finish();
                    }
                } catch (Exception e) {
                    logger.error("[TEST] Error in batch feeding thread", e);
                    throw new AssertionError("Error in batch feeding thread", e);
                }
            }, "batch-feeding-thread");
            batchFeedingThread.start();

            // Wait for driver to fail with IllegalStateException
            Exception driverException = expectThrows(Exception.class, () -> { driverFuture.actionGet(30, TimeUnit.SECONDS); });

            // Unwrap the exception - actionGet wraps exceptions, so check the cause chain
            Throwable cause = ExceptionsHelper.unwrap(driverException, IllegalStateException.class);
            assertNotNull("Driver should throw IllegalStateException when receiving page for batch 2 while processing batch 1", cause);
            assertThat("Cause should be IllegalStateException", cause, instanceOf(IllegalStateException.class));
            assertThat(
                "Exception message should indicate batch mismatch",
                cause.getMessage(),
                containsString("Received page for batch 2 but currently processing batch 1")
            );

            // Wait for batch feeding thread to finish
            batchFeedingThread.join(30000);
            assertThat("Batch feeding thread should have completed", batchFeedingThread.isAlive(), equalTo(false));

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Helper method to create an ExpressionEvaluator that filters out rows where a specific column equals a value.
     * Returns true (keep) when the column value is NOT equal to the filter value.
     */
    private EvalOperator.ExpressionEvaluator createNotEqualsOneEvaluator(DriverContext driverContext, int columnIndex) {
        return new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Page actualPage = page;

                if (actualPage.getBlockCount() <= columnIndex) {
                    return driverContext.blockFactory().newConstantBooleanBlockWith(false, actualPage.getPositionCount());
                }

                Block filterBlock = actualPage.getBlock(columnIndex);
                if (filterBlock instanceof IntBlock == false) {
                    return driverContext.blockFactory().newConstantBooleanBlockWith(false, actualPage.getPositionCount());
                }

                IntBlock intBlock = (IntBlock) filterBlock;
                int positionCount = intBlock.getPositionCount();

                try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        if (intBlock.isNull(p)) {
                            builder.appendNull();
                        } else {
                            int filterValue = intBlock.getInt(intBlock.getFirstValueIndex(p));
                            // Keep rows where filter column is not 1 (i.e., filter out rows with value 1)
                            boolean keep = (filterValue != 1);
                            builder.appendBoolean(keep);
                        }
                    }
                    return builder.build();
                }
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
    }

    /**
     * Helper method to create an EvalOperator that adds 1 to int values.
     */
    private EvalOperator createAddOneOperator(DriverContext driverContext) {
        return new EvalOperator(driverContext, new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Page actualPage = page;

                if (actualPage.getBlockCount() == 0) {
                    return driverContext.blockFactory().newConstantNullBlock(actualPage.getPositionCount());
                }

                Block firstBlock = actualPage.getBlock(0);
                if (firstBlock instanceof IntBlock == false) {
                    return driverContext.blockFactory().newConstantNullBlock(actualPage.getPositionCount());
                }

                IntBlock intBlock = (IntBlock) firstBlock;
                int positionCount = intBlock.getPositionCount();

                try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        if (intBlock.isNull(p)) {
                            builder.appendNull();
                        } else {
                            int value = intBlock.getInt(intBlock.getFirstValueIndex(p));
                            builder.appendInt(value + 1);
                        }
                    }
                    return builder.build();
                }
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        });
    }

    /**
     * Helper method to create a BatchDriver with given operators.
     */
    private BatchDriver createBatchDriver(
        DriverContext driverContext,
        ExchangeSourceOperator sourceOperator,
        EvalOperator addOneOperator,
        SinkOperator sinkOperator,
        Runnable onBatchEnd
    ) {
        return new BatchDriver(
            "test-session",
            "test",
            "test-cluster",
            "test-node",
            System.currentTimeMillis(),
            System.nanoTime(),
            driverContext,
            () -> "test",
            sourceOperator,
            List.of(addOneOperator),
            BatchDriver.wrapSink(new MarkerFilteringSinkOperator(sinkOperator, onBatchEnd)),
            TimeValue.timeValueSeconds(1),
            () -> {}
        );
    }

    // ========================================
    // Inner Classes
    // ========================================

    /**
     * Result class for exchange setup.
     */
    private static class ExchangeSetup {
        final ExchangeSourceHandler sourceHandler;
        final ExchangeSinkHandler sinkHandler;
        final ExchangeSourceOperator sourceOperator;
        final ExchangeSink exchangeSink;

        ExchangeSetup(
            ExchangeSourceHandler sourceHandler,
            ExchangeSinkHandler sinkHandler,
            ExchangeSourceOperator sourceOperator,
            ExchangeSink exchangeSink
        ) {
            this.sourceHandler = sourceHandler;
            this.sinkHandler = sinkHandler;
            this.sourceOperator = sourceOperator;
            this.exchangeSink = exchangeSink;
        }
    }

    /**
     * Test data record containing batches and expected output batches.
     */
    private record TestData(List<List<Page>> batches, List<List<Page>> expectedOutputBatches) {}

    /**
     * A sink operator wrapper that filters out batch marker pages (empty batch signals).
     * This is needed because TestResultPageSinkOperator cannot handle pages with 0 blocks,
     * but PageToBatchPageOperator.flushBatch() sends marker pages for empty batches.
     * Also triggers onBatchEnd callback when a batch ends (detected via isLastPageInBatch).
     */
    private static class MarkerFilteringSinkOperator extends SinkOperator {
        private final SinkOperator delegate;
        private final Runnable onBatchEnd;

        MarkerFilteringSinkOperator(SinkOperator delegate, Runnable onBatchEnd) {
            this.delegate = delegate;
            this.onBatchEnd = onBatchEnd;
        }

        @Override
        public boolean needsInput() {
            return delegate.needsInput();
        }

        @Override
        protected void doAddInput(Page page) {
            BatchMetadata metadata = page.batchMetadata();

            // Detect batch end and trigger callback (before filtering)
            if (metadata != null && metadata.isLastPageInBatch()) {
                onBatchEnd.run();
            }

            // Filter out marker pages (pages with isBatchMarkerOnly=true)
            if (page.isBatchMarkerOnly()) {
                // Just release the marker page, don't pass to delegate
                page.releaseBlocks();
                return;
            }
            delegate.addInput(page);
        }

        @Override
        public void finish() {
            delegate.finish();
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

}
