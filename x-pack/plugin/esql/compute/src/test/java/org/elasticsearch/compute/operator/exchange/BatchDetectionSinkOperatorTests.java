/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link BatchDetectionSinkOperator}.
 */
public class BatchDetectionSinkOperatorTests extends ESTestCase {

    private ThreadPool threadPool;
    private DriverContext driverContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestClass().getSimpleName());
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        driverContext = new DriverContext(bigArrays, blockFactory);
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    /**
     * Test basic functionality: adding pages and detecting batch completion.
     */
    public void testBasicBatchDetection() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            List<BatchPage> collectedPages = new ArrayList<>();
            List<Long> completedBatches = new ArrayList<>();
            AtomicLong completedBatchId = new AtomicLong(-1);
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    collectedPages::add,
                    batchId -> completedBatches.add(batchId),
                    completedBatchId::set,
                    failureRef,
                    () -> false // No incomplete batches
                )
            ) {
                // Create a batch page with data
                IntBlock dataBlock = driverContext.blockFactory().newIntBlockBuilder(3).appendInt(1).appendInt(2).appendInt(3).build();
                Page dataPage = new Page(dataBlock);
                BatchPage batchPage = new BatchPage(dataPage, 0, true); // batchId=0, isLastPageInBatch=true

                // Add input
                sinkOperator.addInput(batchPage);

                // Verify page was collected
                assertThat(collectedPages.size(), equalTo(1));
                assertThat(collectedPages.get(0).batchId(), equalTo(0L));

                // Verify batch completion was detected
                assertThat(completedBatches.size(), equalTo(1));
                assertThat(completedBatches.get(0), equalTo(0L));
                assertThat(completedBatchId.get(), equalTo(0L));

                // No failure should have occurred
                assertThat(failureRef.get(), nullValue());
            }
        }
    }

    /**
     * Test that marker pages (empty pages with isLastPageInBatch=true) trigger batch completion.
     */
    public void testMarkerPageTriggersBatchCompletion() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            List<BatchPage> collectedPages = new ArrayList<>();
            List<Long> completedBatches = new ArrayList<>();
            AtomicLong completedBatchId = new AtomicLong(-1);
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    collectedPages::add,
                    batchId -> completedBatches.add(batchId),
                    completedBatchId::set,
                    failureRef,
                    () -> false
                )
            ) {
                // Create a marker page (empty page)
                BatchPage markerPage = BatchPage.createMarker(5);

                // Add input
                sinkOperator.addInput(markerPage);

                // Marker pages should NOT be collected (positionCount=0)
                assertThat(collectedPages.size(), equalTo(0));

                // But batch completion should still be triggered
                assertThat(completedBatches.size(), equalTo(1));
                assertThat(completedBatches.get(0), equalTo(5L));
                assertThat(completedBatchId.get(), equalTo(5L));
            }
        }
    }

    /**
     * Test that non-last pages don't trigger batch completion.
     */
    public void testNonLastPageDoesNotTriggerCompletion() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            List<BatchPage> collectedPages = new ArrayList<>();
            List<Long> completedBatches = new ArrayList<>();
            AtomicLong completedBatchId = new AtomicLong(-1);
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    collectedPages::add,
                    batchId -> completedBatches.add(batchId),
                    completedBatchId::set,
                    failureRef,
                    () -> false
                )
            ) {
                // Create a batch page that is NOT the last page in batch
                IntBlock dataBlock = driverContext.blockFactory().newIntBlockBuilder(2).appendInt(10).appendInt(20).build();
                Page dataPage = new Page(dataBlock);
                BatchPage batchPage = new BatchPage(dataPage, 1, false); // batchId=1, isLastPageInBatch=false

                // Add input
                sinkOperator.addInput(batchPage);

                // Page should be collected
                assertThat(collectedPages.size(), equalTo(1));

                // But batch completion should NOT be triggered
                assertThat(completedBatches.size(), equalTo(0));
                assertThat(completedBatchId.get(), equalTo(-1L));
            }
        }
    }

    /**
     * Test needsInput() returns true when source is not finished and no failure.
     */
    public void testNeedsInputWhenSourceNotFinished() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    page -> {},
                    batchId -> {},
                    id -> {},
                    failureRef,
                    () -> false
                )
            ) {
                // Source is not finished, no failure - should need input
                assertTrue("Should need input when source is not finished", sinkOperator.needsInput());
            }
        }
    }

    /**
     * Test needsInput() returns true when there are incomplete batches.
     */
    public void testNeedsInputWithIncompleteBatches() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    page -> {},
                    batchId -> {},
                    id -> {},
                    failureRef,
                    () -> true // Has incomplete batches
                )
            ) {
                // Has incomplete batches - should need input (to receive batch completion markers)
                assertTrue("Should need input when there are incomplete batches", sinkOperator.needsInput());
            }
        }
    }

    /**
     * Test needsInput() returns false when there's a failure.
     */
    public void testNeedsInputReturnsFalseOnFailure() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            AtomicReference<Exception> failureRef = new AtomicReference<>(new RuntimeException("test failure"));

            // Finish the source so it's not blocking needsInput
            exchange.exchangeSink.finish();

            // Wait for source to see the finish
            PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
            exchange.sourceOperator.isBlocked().listener().addListener(waitFuture);
            try {
                waitFuture.actionGet();
            } catch (Exception e) {
                // Ignore - source might already be done
            }

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    page -> {},
                    batchId -> {},
                    id -> {},
                    failureRef,
                    () -> true // Even with incomplete batches
                )
            ) {
                // When there's a failure and source is finished, should not need input
                // (the hasFailure check takes precedence)
                assertFalse("Should not need input when there's a failure and source is finished", sinkOperator.needsInput());
            }
        }
    }

    /**
     * Test isFinished() delegates to source operator.
     */
    public void testIsFinishedDelegatesToSource() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    page -> {},
                    batchId -> {},
                    id -> {},
                    failureRef,
                    () -> false
                )
            ) {
                // Initially source is not finished
                assertFalse("Should not be finished initially", sinkOperator.isFinished());

                // Finish the exchange
                exchange.exchangeSink.finish();

                // Wait for source to process the finish
                int maxWaits = 100;
                while (exchange.sourceOperator.isFinished() == false && maxWaits-- > 0) {
                    Thread.sleep(10);
                }

                // Now should be finished
                assertTrue("Should be finished after source finishes", sinkOperator.isFinished());
            }
        }
    }

    /**
     * Test multiple batches in sequence.
     */
    public void testMultipleBatches() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            List<BatchPage> collectedPages = new ArrayList<>();
            List<Long> completedBatches = new ArrayList<>();
            AtomicLong completedBatchId = new AtomicLong(-1);
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    collectedPages::add,
                    batchId -> completedBatches.add(batchId),
                    completedBatchId::set,
                    failureRef,
                    () -> false
                )
            ) {
                // Create and add pages for 3 batches
                for (int batchId = 0; batchId < 3; batchId++) {
                    // First page (not last)
                    IntBlock block1 = driverContext.blockFactory().newIntBlockBuilder(1).appendInt(batchId * 100).build();
                    BatchPage page1 = new BatchPage(new Page(block1), batchId, false);
                    sinkOperator.addInput(page1);

                    // Second page (last)
                    IntBlock block2 = driverContext.blockFactory().newIntBlockBuilder(1).appendInt(batchId * 100 + 1).build();
                    BatchPage page2 = new BatchPage(new Page(block2), batchId, true);
                    sinkOperator.addInput(page2);
                }

                // Should have collected 6 pages (2 per batch)
                assertThat(collectedPages.size(), equalTo(6));

                // Should have completed 3 batches
                assertThat(completedBatches.size(), equalTo(3));
                assertThat(completedBatches.get(0), equalTo(0L));
                assertThat(completedBatches.get(1), equalTo(1L));
                assertThat(completedBatches.get(2), equalTo(2L));

                // Last completed batch should be 2
                assertThat(completedBatchId.get(), equalTo(2L));
            }
        }
    }

    /**
     * Test that exceptions in batch done listener are captured in failureRef.
     */
    public void testBatchDoneListenerExceptionCaptured() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            List<BatchPage> collectedPages = new ArrayList<>();
            AtomicLong completedBatchId = new AtomicLong(-1);
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            RuntimeException listenerException = new RuntimeException("listener failed");

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    collectedPages::add,
                    batchId -> {
                        throw listenerException;
                    }, // Throw exception
                    completedBatchId::set,
                    failureRef,
                    () -> false
                )
            ) {
                // Create a batch page
                IntBlock dataBlock = driverContext.blockFactory().newIntBlockBuilder(1).appendInt(42).build();
                BatchPage batchPage = new BatchPage(new Page(dataBlock), 0, true);

                // Add input - this should trigger the exception
                sinkOperator.addInput(batchPage);

                // Page should still be collected
                assertThat(collectedPages.size(), equalTo(1));

                // Failure should be captured
                assertThat(failureRef.get(), equalTo(listenerException));

                // completedBatchId should NOT be updated since listener failed
                assertThat(completedBatchId.get(), equalTo(-1L));
            }
        }
    }

    /**
     * Test isBlocked() always returns NOT_BLOCKED.
     */
    public void testIsBlockedReturnsNotBlocked() throws Exception {
        try (ExchangeSetup exchange = setupExchange()) {
            AtomicReference<Exception> failureRef = new AtomicReference<>();

            try (
                BatchDetectionSinkOperator sinkOperator = new BatchDetectionSinkOperator(
                    exchange.sourceOperator,
                    page -> {},
                    batchId -> {},
                    id -> {},
                    failureRef,
                    () -> false
                )
            ) {
                // isBlocked should always return NOT_BLOCKED
                assertTrue("isBlocked should be done", sinkOperator.isBlocked().listener().isDone());
            }
        }
    }

    /**
     * Helper class for exchange setup.
     */
    private static class ExchangeSetup implements Closeable {
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

        @Override
        public void close() {
            // Finish the exchange sink to signal no more pages
            exchangeSink.finish();
            // Close the source operator
            sourceOperator.close();
        }
    }

    /**
     * Set up exchange handlers and operators.
     */
    private ExchangeSetup setupExchange() {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(10, threadPool.executor(ThreadPool.Names.SEARCH));
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(driverContext.blockFactory(), 10, System::currentTimeMillis);
        sourceHandler.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());

        ExchangeSourceOperator sourceOperator = new ExchangeSourceOperator(sourceHandler.createExchangeSource());
        ExchangeSink exchangeSink = sinkHandler.createExchangeSink(() -> {});

        return new ExchangeSetup(sourceHandler, sinkHandler, sourceOperator, exchangeSink);
    }
}
