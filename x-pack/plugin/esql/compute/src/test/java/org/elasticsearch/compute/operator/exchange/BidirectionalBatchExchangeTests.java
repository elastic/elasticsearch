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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class BidirectionalBatchExchangeTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(BidirectionalBatchExchangeTests.class);

    /**
     * Standard timeout for test operations (30 seconds).
     */
    private static final long TEST_TIMEOUT_SECONDS = 30;

    /**
     * Timeout for giant batch processing test
     * This test processes a large amount of batches and needs a longer timeout.
     */
    private static final long GIANT_BATCH_TEST_TIMEOUT_SECONDS = 300;

    /**
     * Test with only a single batch.
     */
    public void testSingleBatch() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = 1;
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 2, 2, 3, 3),
                TEST_TIMEOUT_SECONDS
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with an empty batch (0 pages) using a marker page.
     */
    public void testEmptyBatch() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = 1;
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 0, 0, 1, 1),
                TEST_TIMEOUT_SECONDS
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with a batch containing exactly one page.
     */
    public void testSinglePageBatch() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = 1;
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 3, 3),
                TEST_TIMEOUT_SECONDS
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with mixed batches: empty, single-page, and multi-page batches.
     */
    public void testMixedBatches() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = 4;
            runBidirectionalBatchTestOnDemand(threadPool, blockFactory, numBatches, batchId -> {
                // Batch 0: Empty batch
                if (batchId == 0) {
                    return new ArrayList<>();
                }
                // Batch 1: Single page with 2 positions
                if (batchId == 1) {
                    return createBatchWithPages(blockFactory, batchId, 1, 2).batches().get(0);
                }
                // Batch 2: Multi-page (3 pages) with 2 positions each
                if (batchId == 2) {
                    return createBatchWithPages(blockFactory, batchId, 3, 2).batches().get(0);
                }
                // Batch 3: Another single page with 1 position
                if (batchId == 3) {
                    return createBatchWithPages(blockFactory, batchId, 1, 1).batches().get(0);
                }
                throw new IllegalArgumentException("Unexpected batchId: " + batchId);
            }, TEST_TIMEOUT_SECONDS);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test that batches sent upfront are handled gracefully by blocking until the previous batch completes.
     * This test verifies that the system can handle batches sent upfront without throwing exceptions.
     */
    public void testBatchProcessingAllBatchesUpfrontSuccess() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);
        try {
            int numBatches = between(2, 10);
            TestData testData = createSimpleTestBatches(blockFactory, numBatches, 0, 10, 1, 10);
            List<List<Page>> batches = testData.batches();
            List<List<Page>> expectedOutputBatches = testData.expectedOutputBatches();

            // Track results
            AtomicInteger processedBatches = new AtomicInteger(0);
            List<Long> callbackBatchIds = new ArrayList<>();
            AtomicInteger batchesSent = new AtomicInteger(0);
            AtomicReference<List<Page>> allOutputPagesRef = new AtomicReference<>(new ArrayList<>());
            AtomicReference<Exception> serverException = new AtomicReference<>();

            // Set up batch exchange status listener
            PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

            // Generate unique session ID for this test
            String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

            // Create client holder and listeners before client construction
            AtomicReference<BidirectionalBatchExchangeClient> clientHolder = new AtomicReference<>();
            Consumer<BatchPage> resultPageCollector = createResultPageCollector(allOutputPagesRef);
            BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener = createBatchDoneListener(
                clientHolder,
                processedBatches,
                callbackBatchIds,
                numBatches
            );

            // Set up client on main thread
            BidirectionalBatchExchangeClient client = setupClient(
                infra,
                threadPool,
                batchExchangeStatusFuture,
                sessionId,
                resultPageCollector,
                batchDoneListener
            );
            clientHolder.set(client);

            // Start server thread, wait for initialization, and connect client
            Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, TEST_TIMEOUT_SECONDS, sessionId);

            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: {}", numBatches);

            // Client sends all batches upfront
            sendAllBatchesUpfront(client, batches, batchesSent);
            // finish() will be called in the batch done callback when the last batch completes

            // Wait for client completion and server thread
            waitForClientCompletion(client, batchExchangeStatusFuture, TEST_TIMEOUT_SECONDS);
            waitForServerThreadAndCheckExceptions(serverThread, serverException);

            // Cleanup
            client.close();

            // Verify results and release pages
            verifyResultsAndReleasePages(
                batchesSent,
                processedBatches,
                callbackBatchIds,
                allOutputPagesRef,
                expectedOutputBatches,
                numBatches
            );

            // Cleanup is done in the threads
            logger.info("[TEST] Test completed successfully");
        } finally {
            cleanupServices(infra, threadPool);
        }
    }

    /**
     * Test case where 0 pages are sent to the server (not even a marker).
     * This verifies that the driver can handle an empty exchange gracefully
     * and still complete the batch exchange status.
     */
    public void testZeroPagesSent() throws Exception {
        ThreadPool threadPool = threadPool();
        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory());
        try {
            // Track results
            AtomicInteger processedBatches = new AtomicInteger(0);
            List<Long> callbackBatchIds = new ArrayList<>();
            AtomicReference<List<Page>> allOutputPagesRef = new AtomicReference<>(new ArrayList<>());
            AtomicReference<Exception> serverException = new AtomicReference<>();

            // Set up batch exchange status listener
            PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

            // Generate unique session ID for this test
            String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

            // Create client holder and listeners before client construction
            int numBatchesForZeroPagesTest = 1; // We'll send one marker batch
            AtomicReference<BidirectionalBatchExchangeClient> clientHolder = new AtomicReference<>();
            Consumer<BatchPage> resultPageCollector = createResultPageCollector(allOutputPagesRef);
            BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener = createBatchDoneListener(
                clientHolder,
                processedBatches,
                callbackBatchIds,
                numBatchesForZeroPagesTest
            );

            // Set up client on main thread
            BidirectionalBatchExchangeClient client = setupClient(
                infra,
                threadPool,
                batchExchangeStatusFuture,
                sessionId,
                resultPageCollector,
                batchDoneListener
            );
            clientHolder.set(client);

            // Start server thread, wait for initialization, and connect client
            Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, TEST_TIMEOUT_SECONDS, sessionId);

            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: 1 (marker batch)");

            // Client sends NO pages - send a marker batch to represent "no data", finish will be called in callback
            logger.info("[TEST-CLIENT] Sending no pages, sending marker batch");
            client.sendBatchMarker(0);

            // Wait for client completion and server thread
            waitForClientCompletion(client, batchExchangeStatusFuture, TEST_TIMEOUT_SECONDS);
            waitForServerThreadAndCheckExceptions(serverThread, serverException);

            // Cleanup
            client.close();

            // Verify that one empty marker batch was processed (batch 0, which is empty)
            assertThat("One empty marker batch should be processed", processedBatches.get(), equalTo(1));
            assertThat("One batch callback should be called for the empty marker batch", callbackBatchIds.size(), equalTo(1));
            assertThat("The processed batch should be batch 0", callbackBatchIds.get(0), equalTo(0L));
            assertThat("No output pages should be received from empty batch", allOutputPagesRef.get().size(), equalTo(0));

            logger.info("[TEST] Test completed successfully");
        } finally {
            cleanupServices(infra, threadPool);
        }
    }

    /**
     * Test that verifies out-of-order batch pages are detected and the exception propagates to the client.
     * This test sends:
     * 1. Batch 0 completely
     * 2. Batch 1 pages but NOT the last page (so batch 1 is still processing)
     * 3. A page for batch 2 while batch 1 is still processing
     * The server should detect this and throw IllegalStateException, which should propagate to the client.
     */
    public void testOutOfOrderBatchPages() throws Exception {
        ThreadPool threadPool = threadPool();
        // Create test data with at least 3 batches
        int numBatches = 3;
        BlockFactory blockFactory = blockFactory();
        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);
        try {
            TestData testData = createSimpleTestBatches(blockFactory, numBatches, 2, 5, 1, 10);
            List<List<Page>> batches = testData.batches();

            // Track exceptions
            AtomicReference<Exception> serverException = new AtomicReference<>();

            // Set up batch exchange status listener
            PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

            // Generate unique session ID for this test
            String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

            // Set up batch done listener - batch 0 will complete, batch 1 won't (we don't send last page)
            // Since batch 0 is the last batch that will complete, finish in its callback after all batches are sent
            CountDownLatch batch0CompletedLatch = new CountDownLatch(1);
            AtomicReference<Boolean> allBatchesSent = new AtomicReference<>(false);
            AtomicReference<Boolean> finishCalled = new AtomicReference<>(false);
            AtomicReference<BidirectionalBatchExchangeClient> clientHolder = new AtomicReference<>();

            BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener = batchId -> {
                logger.info("[TEST-CLIENT] Batch {} completed", batchId);
                if (batchId == 0) {
                    batch0CompletedLatch.countDown();
                    // If all batches have been sent (including the problematic batch 2), finish the exchange
                    if (allBatchesSent.get() && finishCalled.compareAndSet(false, true)) {
                        logger.debug("[TEST-CLIENT] Last completing batch {} finished, finishing exchange", batchId);
                        BidirectionalBatchExchangeClient c = clientHolder.get();
                        if (c != null) {
                            c.finish();
                        }
                    }
                }
            };

            // Set up client on main thread
            BidirectionalBatchExchangeClient client = setupClient(
                infra,
                threadPool,
                batchExchangeStatusFuture,
                sessionId,
                batchPage -> {},  // No result page collector needed for this test
                batchDoneListener
            );
            clientHolder.set(client);

            // Start server thread, wait for initialization, and connect client
            Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, TEST_TIMEOUT_SECONDS, sessionId);

            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: {}", numBatches);

            // Send batch 0 completely
            List<Page> batch0Pages = batches.get(0);
            for (int pageIdx = 0; pageIdx < batch0Pages.size(); pageIdx++) {
                Page page = batch0Pages.get(pageIdx);
                boolean isLastPageInBatch = (pageIdx == batch0Pages.size() - 1);
                client.sendPage(new BatchPage(page, 0, isLastPageInBatch));
            }

            // Send batch 1 pages but NOT the last page (so batch 1 is still processing)
            List<Page> batch1Pages = batches.get(1);
            for (int pageIdx = 0; pageIdx < batch1Pages.size() - 1; pageIdx++) {
                Page page = batch1Pages.get(pageIdx);
                client.sendPage(new BatchPage(page, 1, false)); // Not last page
            }

            // Now send a page for batch 2 while batch 1 is still processing (not ended)
            // This should trigger the IllegalStateException on the server
            List<Page> batch2Pages = batches.get(2);
            if (batch2Pages.isEmpty() == false) {
                Page page = batch2Pages.get(0);
                client.sendPage(new BatchPage(page, 2, false));
            } else {
                // If batch 2 is empty, send a marker
                client.sendBatchMarker(2);
            }

            // Mark that all batches have been sent - finish() will be called in batch 0's callback
            // (batch 0 is the last batch that will complete successfully)
            allBatchesSent.set(true);

            // Wait for batch 0 to complete - finish will be called in its callback
            // If batch 0 already completed before we set allBatchesSent, the callback won't call finish,
            // so we need to check and call it here as a fallback
            try {
                boolean batch0AlreadyCompleted = batch0CompletedLatch.getCount() == 0;
                if (batch0AlreadyCompleted == false) {
                    batch0CompletedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                // Ensure finish is called - either in callback or here as fallback
                if (finishCalled.compareAndSet(false, true)) {
                    logger.debug("[TEST-CLIENT] Batch 0 completed, calling finish as fallback");
                    client.finish();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for batch 0 to complete", e);
            }

            // Wait for batch exchange status response (will contain failure)
            Exception clientException = null;
            try {
                batchExchangeStatusFuture.actionGet(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                // If we get here without exception, the test should fail
                throw new AssertionError("Expected batch exchange status to indicate failure");
            } catch (Exception statusException) {
                // Store the exception - this is expected
                logger.info("[TEST-CLIENT] Batch exchange status failed as expected: {}", statusException.getMessage());
                clientException = statusException;
            }

            // Wait for server thread to complete (server exception is expected in this test)
            serverThread.join(TEST_TIMEOUT_SECONDS * 1000);
            assertFalse("Server thread should have completed", serverThread.isAlive());

            // Verify that the server failed as expected
            Exception serverEx = serverException.get();
            assertNotNull("Server should have failed with exception", serverEx);
            assertThat("Server exception should be IllegalStateException", serverEx, instanceOf(IllegalStateException.class));
            assertThat(
                "Server exception message should indicate batch mismatch",
                serverEx.getMessage(),
                containsString("Received page for batch 2 but currently processing batch 1")
            );

            // Verify that the client detected the error
            assertNotNull("Client should detect exception when server receives page for batch 2 while processing batch 1", clientException);

            // Unwrap the exception - check the cause chain for IllegalStateException
            Throwable cause = ExceptionsHelper.unwrap(clientException, IllegalStateException.class);
            assertNotNull(
                "Client should detect IllegalStateException when server receives page for batch 2 while processing batch 1",
                cause
            );
            assertThat("Cause should be IllegalStateException", cause, instanceOf(IllegalStateException.class));
            assertThat(
                "Exception message should indicate batch mismatch",
                cause.getMessage(),
                containsString("Received page for batch 2 but currently processing batch 1")
            );

            // Cleanup
            client.close();

            logger.info("[TEST] Test completed successfully - exception was correctly propagated to client");
        } finally {
            cleanupServices(infra, threadPool);
        }
    }

    /**
     * A randomized test for BidirectionalBatchExchange.
     */
    public void testBatchProcessingRandom() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = between(1000, 10000);
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 0, 10, 1, 10),
                TEST_TIMEOUT_SECONDS
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A randomized test for BidirectionalBatchExchange with 100,000 batches.
     * Batches are generated on demand and verified incrementally
     * to avoid the test running out of memory.
     */
    public void testBatchProcessingRandomGiant() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            int numBatches = 20_000;
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 0, 10, 1, 10),
                GIANT_BATCH_TEST_TIMEOUT_SECONDS
            );
        } finally {
            terminate(threadPool);
        }
    }

    private MockTransportService newTransportService(ThreadPool threadPool) {
        List<Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.current(), threadPool, namedWriteableRegistry),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet()
        );
        service.getTaskManager().setTaskCancellationService(new TaskCancellationService(service));
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    /**
     * Set up and initialize the client side of the bidirectional batch exchange.
     *
     * @param infra test infrastructure containing exchange services and IDs
     * @param threadPool thread pool
     * @param batchExchangeStatusListener listener for batch exchange status completion
     * @return initialized client
     */
    private BidirectionalBatchExchangeClient setupClient(
        TestInfrastructure infra,
        ThreadPool threadPool,
        ActionListener<Void> batchExchangeStatusListener,
        String sessionId,
        Consumer<BatchPage> resultPageCollector,
        BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener
    ) throws Exception {
        logger.info("[TEST-CLIENT] Creating BidirectionalBatchExchangeClient with sessionId={}", sessionId);
        Task mockTask = mock(Task.class);
        logger.info("[TEST-CLIENT] Creating client driver context");
        DriverContext driverContext = driverContext();
        BidirectionalBatchExchangeClient client = new BidirectionalBatchExchangeClient(
            sessionId,
            "test-cluster",
            infra.clientExchangeService(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            10,
            infra.clientTransportService(),
            mockTask,
            infra.serverTransportService().getLocalNode(),
            batchExchangeStatusListener,
            driverContext.bigArrays(),
            driverContext.blockFactory().breaker(),  // Pass the breaker, not the blockFactory
            threadPool.getThreadContext(),
            resultPageCollector,
            batchDoneListener
        );
        logger.info("[TEST-CLIENT] Client initialized successfully");

        return client;
    }

    /**
     * Set up and initialize the server side of the bidirectional batch exchange.
     * Creates operators internally (e.g., addOneOperator).
     * The driver is started automatically and a future is returned.
     */

    /**
     * Functional interface for generating batches on demand.
     */
    @FunctionalInterface
    private interface BatchGenerator {
        List<Page> generateBatch(int batchId);
    }

    /**
     * Run a bidirectional batch test with on-demand batch generation and incremental validation.
     * This pattern reduces memory usage by generating batches on-demand and validating incrementally.
     */
    private void runBidirectionalBatchTestOnDemand(
        ThreadPool threadPool,
        BlockFactory blockFactory,
        int numBatches,
        BatchGenerator batchGenerator,
        long timeoutSeconds
    ) throws Exception {
        logger.info("[TEST] Starting bidirectional batch test: numBatches={}", numBatches);

        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);

        // Track results
        AtomicInteger processedBatches = new AtomicInteger(0);
        List<Long> callbackBatchIds = new ArrayList<>();
        AtomicInteger batchesSent = new AtomicInteger(0);

        // Track exceptions
        AtomicReference<Exception> serverException = new AtomicReference<>();

        // Per-batch data - stored and verified after each batch completes
        AtomicReference<List<Page>> currentBatchInputPages = new AtomicReference<>();
        AtomicReference<List<Page>> currentBatchOutputPages = new AtomicReference<>(new ArrayList<>());

        // Set up batch exchange status listener
        PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

        // Generate unique session ID for this test
        String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

        // Create holders for circular references
        AtomicReference<BidirectionalBatchExchangeClient> clientHolder = new AtomicReference<>();
        AtomicReference<Runnable> feedBatchHolder = new AtomicReference<>();
        AtomicInteger currentBatchIndex = new AtomicInteger(0);

        // Create result page collector
        Consumer<BatchPage> resultPageCollector = batchPage -> {
            BatchPage pageCopy = copyBatchPage(batchPage);
            synchronized (currentBatchOutputPages) {
                currentBatchOutputPages.get().add(pageCopy);
            }
        };

        // Create batch done listener
        BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener = batchId -> {
            int currentProcessed = processedBatches.incrementAndGet();
            callbackBatchIds.add(batchId);

            // Log progress every 1000 batches or on the last batch
            boolean isLastBatch = currentProcessed >= numBatches;
            if (currentProcessed % 1000 == 0 || isLastBatch) {
                logger.info(
                    "[TEST-CLIENT] Progress: {} batches processed out of {} ({}%)",
                    currentProcessed,
                    numBatches,
                    (currentProcessed * 100) / numBatches
                );
            }

            logger.debug("[TEST-CLIENT] Batch {} completed, verifying and triggering next batch", batchId);

            // Get stored input pages and received output pages
            List<Page> inputPages;
            List<Page> receivedPages;
            synchronized (currentBatchInputPages) {
                inputPages = currentBatchInputPages.get();
            }
            synchronized (currentBatchOutputPages) {
                // Create a copy to avoid holding the lock during verification
                receivedPages = new ArrayList<>(currentBatchOutputPages.get());
                currentBatchOutputPages.set(new ArrayList<>());
            }

            // Verify batch results
            if (inputPages != null) {
                if (inputPages.isEmpty() == false) {
                    verifyBatchResultsFromInput(inputPages, receivedPages, batchId, blockFactory);

                    // Release input pages after verification
                    for (Page page : inputPages) {
                        page.releaseBlocks();
                    }
                } else {
                    // Empty batch - verify we received no output pages
                    assertThat("Batch " + batchId + ": Empty batch should produce no output pages", receivedPages.size(), equalTo(0));
                }
            }

            // Release output pages after verification
            for (Page page : receivedPages) {
                page.releaseBlocks();
            }

            // Clear input pages for next batch (output pages already cleared above)
            synchronized (currentBatchInputPages) {
                currentBatchInputPages.set(null);
            }

            // Check if this was the last batch - if so, finish the exchange
            if (isLastBatch) {
                logger.debug("[TEST-CLIENT] Last batch {} completed, finishing exchange", batchId);
                BidirectionalBatchExchangeClient c = clientHolder.get();
                if (c != null) {
                    c.finish();
                }
            } else {
                // Feed the next batch asynchronously in the callback
                threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                    logger.debug("[TEST-CLIENT] Batch callback: Executing feedBatch for next batch after batch {}", batchId);
                    Runnable fb = feedBatchHolder.get();
                    if (fb != null) {
                        fb.run();
                    }
                });
            }
        };

        // Initialize client on main thread
        BidirectionalBatchExchangeClient client = setupClient(
            infra,
            threadPool,
            batchExchangeStatusFuture,
            sessionId,
            resultPageCollector,
            batchDoneListener
        );
        clientHolder.set(client);

        // Now create the feedBatch runnable (can reference client directly now)
        Runnable feedBatch = () -> {
            int batchId = currentBatchIndex.getAndIncrement();

            if (batchId >= numBatches) {
                logger.debug("[TEST-CLIENT] feedBatch: All {} batches sent, no more batches to send", numBatches);
                return;
            }

            try {
                // Generate batch on demand and store it for verification
                List<Page> batchPages = batchGenerator.generateBatch(batchId);

                // Store copied input pages for verification (will be released after verification)
                // Copy pages to prevent them from being released when sent
                List<Page> copiedBatchPages = copyPages(batchPages);
                synchronized (currentBatchInputPages) {
                    currentBatchInputPages.set(copiedBatchPages);
                }
                // Clear output pages collection for this batch (after storing input)
                synchronized (currentBatchOutputPages) {
                    currentBatchOutputPages.set(new ArrayList<>());
                }

                // Send the batch
                sendBatchFromPages(client, batchPages, batchId, batchesSent);

                logger.debug("[TEST-CLIENT] feedBatch: Finished feeding batch {}", batchId);
            } catch (Exception e) {
                logger.error("[TEST-CLIENT] feedBatch: Error feeding batch " + batchId, e);
                throw new AssertionError("Error feeding batch " + batchId, e);
            }
        };
        feedBatchHolder.set(feedBatch);

        // Start server thread, wait for initialization, and connect client
        Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, timeoutSeconds, sessionId);

        // Log number of batches to send
        logger.info("[TEST] Number of batches to send: {}", numBatches);

        // Client sends first batch - server driver will start when it receives the batch exchange status request
        feedBatch.run();

        // Wait for all batches to be processed (feedBatch will call finish() when done)
        assertBusy(
            () -> assertThat("All batches should be processed", processedBatches.get(), equalTo(numBatches)),
            timeoutSeconds,
            TimeUnit.SECONDS
        );

        // Wait for client to finish receiving all pages (including marker pages)
        assertBusy(
            () -> assertTrue("Client source should be finished", client.getServerToClientSourceHandler().isFinished()),
            timeoutSeconds,
            TimeUnit.SECONDS
        );

        // Wait for batch exchange status response (should indicate success)
        batchExchangeStatusFuture.actionGet(timeoutSeconds, TimeUnit.SECONDS);

        // Wait for server thread and check exceptions
        waitForServerThreadAndCheckExceptions(serverThread, serverException, timeoutSeconds * 1000);

        // Cleanup (finish() was already called in the batch done callback when the last batch completed)
        client.close();

        // Verify basic batch processing completion
        logger.info("[TEST] Verifying batch processing results");
        verifyBasicBatchResults(batchesSent, processedBatches, callbackBatchIds, numBatches);
        logger.info("[TEST] Test completed successfully");

        // Cleanup exchange services and transport services
        cleanupServices(infra, threadPool);
    }

    /**
     * Helper method to start server thread, wait for initialization, and connect client to server sink.
     * This is a common pattern used across multiple tests.
     */
    private Thread startServerAndConnectClient(
        TestInfrastructure infra,
        ThreadPool threadPool,
        AtomicReference<Exception> serverException,
        BidirectionalBatchExchangeClient client,
        long timeoutSeconds,
        String sessionId
    ) throws InterruptedException {
        // Create latch to synchronize server initialization
        CountDownLatch serverInitializedLatch = new CountDownLatch(1);

        // Start server thread FIRST - it will initialize and create sink handler
        Thread serverThread = createServerThread(infra, threadPool, serverException, timeoutSeconds, serverInitializedLatch, sessionId);
        serverThread.start();

        // Wait for server to initialize (create sink handler) BEFORE client connects
        try {
            serverInitializedLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for server to initialize", e);
        }

        // NOW client can safely connect to server sink (sink handler exists)
        // This will also send the batch exchange status request, which the server will wait for internally
        client.connectToServerSink();

        return serverThread;
    }

    /**
     * Create a batch done listener that finishes the exchange when all batches are processed.
     * Uses a client holder to allow the listener to be created before the client.
     */
    private BidirectionalBatchExchangeClient.BatchDoneListener createBatchDoneListener(
        AtomicReference<BidirectionalBatchExchangeClient> clientHolder,
        AtomicInteger processedBatches,
        List<Long> callbackBatchIds,
        int numBatches
    ) {
        return batchId -> {
            logger.debug("[TEST-CLIENT] Batch {} completed", batchId);
            int currentProcessed = processedBatches.incrementAndGet();
            callbackBatchIds.add(batchId);

            // When all batches have been processed, finish the exchange
            if (currentProcessed >= numBatches) {
                logger.debug("[TEST-CLIENT] Last batch {} completed, finishing exchange", batchId);
                BidirectionalBatchExchangeClient client = clientHolder.get();
                if (client != null) {
                    client.finish();
                }
            }
        };
    }

    /**
     * Copy a BatchPage with incremented block refs to keep blocks alive.
     */
    private BatchPage copyBatchPage(BatchPage batchPage) {
        Block[] blocks = new Block[batchPage.getBlockCount()];
        for (int i = 0; i < batchPage.getBlockCount(); i++) {
            blocks[i] = batchPage.getBlock(i);
            blocks[i].incRef(); // Increment ref count to keep blocks alive
        }
        return new BatchPage(new Page(blocks), batchPage.batchId(), batchPage.isLastPageInBatch());
    }

    /**
     * Create a result page collector consumer.
     */
    private Consumer<BatchPage> createResultPageCollector(AtomicReference<List<Page>> allOutputPagesRef) {
        return batchPage -> {
            List<Page> allOutputPages = allOutputPagesRef.get();
            BatchPage copiedPage = copyBatchPage(batchPage);
            allOutputPages.add(copiedPage);
        };
    }

    /**
     * Test infrastructure record containing transport services, exchange services, and exchange IDs.
     */
    private record TestInfrastructure(
        MockTransportService clientTransportService,
        MockTransportService serverTransportService,
        ExchangeService clientExchangeService,
        ExchangeService serverExchangeService
    ) {}

    /**
     * Set up test infrastructure: transport services, exchange services, and exchange IDs.
     */
    private TestInfrastructure setupTestInfrastructure(ThreadPool threadPool, BlockFactory blockFactory) {
        // Set up transport services for client and server
        MockTransportService clientTransportService = newTransportService(threadPool);
        MockTransportService serverTransportService = newTransportService(threadPool);

        // Connect both ways for bidirectional communication
        AbstractSimpleTransportTestCase.connectToNode(clientTransportService, serverTransportService.getLocalNode());
        AbstractSimpleTransportTestCase.connectToNode(serverTransportService, clientTransportService.getLocalNode());

        // Create separate exchange services for client and server, registered with their transport services
        ExchangeService clientExchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        clientExchangeService.registerTransportHandler(clientTransportService);
        ExchangeService serverExchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        serverExchangeService.registerTransportHandler(serverTransportService);

        return new TestInfrastructure(clientTransportService, serverTransportService, clientExchangeService, serverExchangeService);
    }

    /**
     * Cleanup exchange services and transport services.
     */
    private void cleanupServices(TestInfrastructure infra, ThreadPool threadPool) {
        if (infra.clientExchangeService() != null) {
            infra.clientExchangeService().close();
        }
        if (infra.serverExchangeService() != null) {
            infra.serverExchangeService().close();
        }
        if (infra.clientTransportService() != null) {
            infra.clientTransportService().close();
        }
        if (infra.serverTransportService() != null) {
            infra.serverTransportService().close();
        }
        terminate(threadPool);
    }

    /**
     * Wait for client to complete processing: source finished, driver finished, and batch exchange status received.
     */
    private void waitForClientCompletion(
        BidirectionalBatchExchangeClient client,
        PlainActionFuture<Void> batchExchangeStatusFuture,
        long timeoutSeconds
    ) throws Exception {
        // Wait for client to finish receiving all pages (including marker pages)
        assertBusy(() -> assertTrue("Client source should be finished", client.getServerToClientSourceHandler().isFinished()));

        // Wait for batch exchange status response (should indicate success)
        batchExchangeStatusFuture.actionGet(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Wait for server thread to complete and check for exceptions.
     */
    private void waitForServerThreadAndCheckExceptions(Thread serverThread, AtomicReference<Exception> serverException)
        throws InterruptedException {
        waitForServerThreadAndCheckExceptions(serverThread, serverException, TEST_TIMEOUT_SECONDS * 1000);
    }

    private void waitForServerThreadAndCheckExceptions(Thread serverThread, AtomicReference<Exception> serverException, long timeoutMs)
        throws InterruptedException {
        serverThread.join(timeoutMs);
        assertFalse("Server thread should have completed", serverThread.isAlive());

        if (serverException.get() != null) {
            throw new AssertionError("Server thread failed", serverException.get());
        }
    }

    /**
     * Verify results and release pages.
     */
    private void verifyResultsAndReleasePages(
        AtomicInteger batchesSent,
        AtomicInteger processedBatches,
        List<Long> callbackBatchIds,
        AtomicReference<List<Page>> allOutputPagesRef,
        List<List<Page>> expectedOutputBatches,
        int numBatches
    ) {
        // Verify batch processing completion
        logger.info("[TEST] Verifying batch processing results");
        verifyBasicBatchResults(batchesSent, processedBatches, callbackBatchIds, numBatches);

        // Verify we received result pages
        List<Page> receivedOutputPages = allOutputPagesRef.get();
        int expectedTotalPages = expectedOutputBatches.stream().mapToInt(List::size).sum();
        logger.info("[TEST] Received {} result pages, expected {} total pages", receivedOutputPages.size(), expectedTotalPages);

        // Verify data correctness
        logger.info("[TEST] Verifying data correctness");
        verifyBidirectionalBatchDataCorrectness(expectedOutputBatches, receivedOutputPages, numBatches);
        logger.info("[TEST] Data verification passed");

        // Release collected pages
        logger.info("[TEST] Releasing collected pages");
        for (Page page : receivedOutputPages) {
            page.releaseBlocks();
        }
    }

    private Thread createServerThread(
        TestInfrastructure infra,
        ThreadPool threadPool,
        AtomicReference<Exception> serverException,
        long driverTimeoutSeconds,
        CountDownLatch serverInitializedLatch,
        String sessionId
    ) {
        return new Thread(() -> {
            try {
                // In production workflow, the server would send a Transport Request to the client that includes the plan and exchange IDs.
                // Then the server will Reply with an acknowledgment and that it is ready to receive data.
                // Here, for testing purposes, we skip those steps and use the method signature to pass parameters directly.
                // And the serverInitializedLatch tells the main thread when the server is ready for the client to connect.
                logger.info("[TEST-SERVER] Creating BidirectionalBatchExchangeServer with sessionId={}", sessionId);
                Task mockTask = mock(Task.class);

                // Create operators for server (server creates its own operators)
                logger.info("[TEST-SERVER] Creating server driver context");
                DriverContext driverContext = driverContext();
                logger.info("[TEST-SERVER] Creating operators for server");
                EvalOperator addOneOperator = createAddOneOperator(driverContext);

                // Stage 1: Create BidirectionalBatchExchangeServer
                BidirectionalBatchExchangeServer server = new BidirectionalBatchExchangeServer(
                    sessionId,
                    infra.serverExchangeService(),
                    threadPool.executor(ThreadPool.Names.SEARCH),
                    10,
                    infra.serverTransportService(),
                    mockTask,
                    infra.clientTransportService().getLocalNode()
                );

                // Stage 2: Start with operators
                server.startWithOperators(driverContext, threadPool.getThreadContext(), List.of(addOneOperator), "test-cluster", () -> {});
                logger.info("[TEST-SERVER] Server initialized successfully");

                // Batch processing is already started in the constructor
                // This connects to the client's sink handler (client is already initialized on the main thread)
                logger.info("[TEST-SERVER] Getting driver future");
                PlainActionFuture<Void> driverFuture = server.getDriverFuture();
                logger.info("[TEST-SERVER] Driver future retrieved");

                // Signal that server has initialized (sink handler created) - client can now connect
                if (serverInitializedLatch != null) {
                    serverInitializedLatch.countDown();
                }

                // Wait for driver to complete
                driverFuture.actionGet(driverTimeoutSeconds, TimeUnit.SECONDS);

                // Cleanup
                server.close();
            } catch (Exception e) {
                logger.error("[TEST-SERVER] Error in server thread", e);
                serverException.set(e);
            }
        }, "server-thread");
    }

    /**
     * Generate pages for a single batch on demand.
     */
    private List<Page> generateBatchPages(
        BlockFactory blockFactory,
        int batchId,
        int minPagesPerBatch,
        int maxPagesPerBatch,
        int minPositionsPerPage,
        int maxPositionsPerPage
    ) {
        List<Page> batchPages = new ArrayList<>();
        int pagesInBatch = between(minPagesPerBatch, maxPagesPerBatch);

        for (int pageIdx = 0; pageIdx < pagesInBatch; pageIdx++) {
            int numPositionsPerPage = between(minPositionsPerPage, maxPositionsPerPage);
            PagePair pagePair = createPagePair(blockFactory, batchId, pageIdx, numPositionsPerPage);
            batchPages.add(pagePair.inputPage());
        }

        return batchPages;
    }

    /**
     * Generate expected output pages for a batch (input values + 1).
     */
    private List<Page> generateExpectedOutputPages(BlockFactory blockFactory, List<Page> inputPages) {
        List<Page> expectedOutputPages = new ArrayList<>();

        for (Page inputPage : inputPages) {
            if (inputPage.getBlockCount() == 0) {
                expectedOutputPages.add(new Page(blockFactory.newConstantNullBlock(inputPage.getPositionCount())));
                continue;
            }

            Block firstBlock = inputPage.getBlock(0);
            if (firstBlock instanceof IntBlock == false) {
                expectedOutputPages.add(new Page(blockFactory.newConstantNullBlock(inputPage.getPositionCount())));
                continue;
            }

            IntBlock intBlock = (IntBlock) firstBlock;
            int positionCount = intBlock.getPositionCount();
            IntBlock.Builder expectedBuilder = blockFactory.newIntBlockBuilder(positionCount);

            for (int p = 0; p < positionCount; p++) {
                if (intBlock.isNull(p)) {
                    expectedBuilder.appendNull();
                } else {
                    int value = intBlock.getInt(intBlock.getFirstValueIndex(p));
                    expectedBuilder.appendInt(value + 1);
                }
            }

            IntBlock expectedOutputBlock = expectedBuilder.build();
            Page expectedOutputPage = new Page(expectedOutputBlock);
            expectedOutputPages.add(expectedOutputPage);
        }

        return expectedOutputPages;
    }

    /**
     * Send all batches upfront from a list of batches.
     */
    private void sendAllBatchesUpfront(BidirectionalBatchExchangeClient client, List<List<Page>> batches, AtomicInteger batchesSent) {
        for (int batchId = 0; batchId < batches.size(); batchId++) {
            List<Page> batchPages = batches.get(batchId);
            sendBatchFromPages(client, batchPages, batchId, batchesSent);
        }
    }

    /**
     * Send a batch from a list of pages.
     */
    private void sendBatchFromPages(
        BidirectionalBatchExchangeClient client,
        List<Page> batchPages,
        int batchId,
        AtomicInteger batchesSent
    ) {
        if (batchPages.isEmpty()) {
            logger.debug("[TEST-CLIENT] Sending empty batch marker for batchId={}", batchId);
            client.sendBatchMarker(batchId);
        } else {
            for (int pageIdx = 0; pageIdx < batchPages.size(); pageIdx++) {
                Page page = batchPages.get(pageIdx);
                boolean isLastPageInBatch = (pageIdx == batchPages.size() - 1);
                logger.debug(
                    "[TEST-CLIENT] Sending page {}/{} for batch {} (isLastPageInBatch={}, positions={})",
                    pageIdx + 1,
                    batchPages.size(),
                    batchId,
                    isLastPageInBatch,
                    page.getPositionCount()
                );
                client.sendPage(new BatchPage(page, batchId, isLastPageInBatch));
            }
        }
        batchesSent.incrementAndGet();
        logger.debug("[TEST-CLIENT] Batch {} sent successfully", batchId);
    }

    /**
     * Copy pages by creating new Page instances with the same blocks and incrementing block refs.
     * This ensures the stored pages remain valid even after the originals are released.
     */
    private List<Page> copyPages(List<Page> pages) {
        if (pages == null || pages.isEmpty()) {
            return pages == null ? null : new ArrayList<>();
        }
        List<Page> copied = new ArrayList<>();
        for (Page page : pages) {
            Block[] blocks = new Block[page.getBlockCount()];
            for (int i = 0; i < page.getBlockCount(); i++) {
                blocks[i] = page.getBlock(i);
                blocks[i].incRef();
            }
            copied.add(new Page(blocks));
        }
        return copied;
    }

    /**
     * Verify results for a single batch using stored input pages.
     */
    private void verifyBatchResultsFromInput(List<Page> inputPages, List<Page> receivedPages, long batchId, BlockFactory blockFactory) {
        // Generate expected output from input pages
        List<Page> expectedOutputPages = generateExpectedOutputPages(blockFactory, inputPages);

        // Calculate expected total positions
        int expectedTotalPositions = expectedOutputPages.stream().mapToInt(Page::getPositionCount).sum();
        int actualTotalPositions = receivedPages.stream().mapToInt(Page::getPositionCount).sum();

        // Verify we got the right number of positions
        assertThat("Batch " + batchId + ": Total positions should match expected", actualTotalPositions, equalTo(expectedTotalPositions));

        // Verify values match by comparing directly from pages (no unnecessary copying)
        // Collect and sort expected values
        List<Integer> expectedValues = new ArrayList<>();
        for (Page page : expectedOutputPages) {
            if (page.getBlockCount() > 0 && page.getBlock(0) instanceof IntBlock) {
                IntBlock block = (IntBlock) page.getBlock(0);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    expectedValues.add(block.getInt(block.getFirstValueIndex(p)));
                }
            }
        }

        // Collect and sort actual values (from output block index 1, which is the new block from EvalOperator)
        List<Integer> actualValues = new ArrayList<>();
        for (Page page : receivedPages) {
            if (page.getBlockCount() > 1 && page.getBlock(1) instanceof IntBlock) {
                IntBlock block = (IntBlock) page.getBlock(1);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    actualValues.add(block.getInt(block.getFirstValueIndex(p)));
                }
            }
        }

        // Verify values match (order might differ due to page splitting, so we sort)
        expectedValues.sort(Integer::compareTo);
        actualValues.sort(Integer::compareTo);
        assertThat("Batch " + batchId + ": Values should match", actualValues, equalTo(expectedValues));
    }

    private ThreadPool threadPool() {
        // TestThreadPool already includes SEARCH executor by default, no need to add it
        return new TestThreadPool(getTestClass().getSimpleName());
    }

    private BlockFactory blockFactory() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        return new BlockFactory(breaker, bigArrays);
    }

    private DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        return new DriverContext(bigArrays, blockFactory);
    }

    /**
     * Verify basic batch processing results.
     */
    private void verifyBasicBatchResults(
        AtomicInteger batchesSent,
        AtomicInteger processedBatches,
        List<Long> callbackBatchIds,
        int numBatches
    ) {
        assertThat("All batches should be sent", batchesSent.get(), equalTo(numBatches));
        assertThat("All batches should be processed", processedBatches.get(), equalTo(numBatches));
        assertThat("All callbacks should be called", callbackBatchIds.size(), equalTo(numBatches));

        // Verify batch IDs in callbacks
        for (int i = 0; i < numBatches; i++) {
            assertThat("Callback batch ID " + i + " should match", callbackBatchIds.get(i), equalTo((long) i));
        }
    }

    /**
     * Test data record containing batches and expected output batches.
     */
    private record TestData(List<List<Page>> batches, List<List<Page>> expectedOutputBatches) {}

    /**
     * Create a single page pair (input and expected output) with the given number of positions.
     * Values follow the pattern: batchId * 100 + pageIdx * 10 + pos.
     * Expected output is input values + 1.
     */
    private record PagePair(Page inputPage, Page expectedOutputPage) {}

    private PagePair createPagePair(BlockFactory blockFactory, int batchId, int pageIdx, int positionsPerPage) {
        IntBlock.Builder inputBuilder = blockFactory.newIntBlockBuilder(positionsPerPage);
        IntBlock.Builder expectedBuilder = blockFactory.newIntBlockBuilder(positionsPerPage);

        for (int pos = 0; pos < positionsPerPage; pos++) {
            int inputValue = batchId * 100 + pageIdx * 10 + pos;
            inputBuilder.appendInt(inputValue);
            expectedBuilder.appendInt(inputValue + 1);
        }

        IntBlock inputBlock = inputBuilder.build();
        IntBlock expectedOutputBlock = expectedBuilder.build();
        Page inputPage = new Page(inputBlock);
        Page expectedOutputPage = new Page(expectedOutputBlock);

        return new PagePair(inputPage, expectedOutputPage);
    }

    /**
     * Create a single batch with a specific number of pages and positions per page.
     * Values follow the pattern: batchId * 100 + pageIdx * 10 + pos.
     * Expected output is input values + 1.
     */
    private TestData createBatchWithPages(BlockFactory blockFactory, int batchId, int numPages, int positionsPerPage) {
        List<List<Page>> batches = new ArrayList<>();
        List<List<Page>> expectedOutputBatches = new ArrayList<>();
        List<Page> batchPages = new ArrayList<>();
        List<Page> expectedOutputPages = new ArrayList<>();

        for (int pageIdx = 0; pageIdx < numPages; pageIdx++) {
            PagePair pagePair = createPagePair(blockFactory, batchId, pageIdx, positionsPerPage);
            batchPages.add(pagePair.inputPage());
            expectedOutputPages.add(pagePair.expectedOutputPage());
        }

        batches.add(batchPages);
        expectedOutputBatches.add(expectedOutputPages);
        return new TestData(batches, expectedOutputBatches);
    }

    /**
     * Create simple test batches with int values following the pattern: batchId * 100 + pageIdx * 10 + offset.
     * Expected output is input values + 1.
     */
    private TestData createSimpleTestBatches(
        BlockFactory blockFactory,
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
                int numPositionsPerPage = between(minPositionsPerPage, maxPositionsPerPage);
                PagePair pagePair = createPagePair(blockFactory, batchId, pageIdx, numPositionsPerPage);
                batchPages.add(pagePair.inputPage());
                expectedOutputPages.add(pagePair.expectedOutputPage());
            }

            batches.add(batchPages);
            expectedOutputBatches.add(expectedOutputPages);
        }

        return new TestData(batches, expectedOutputBatches);
    }

    /**
     * Verify data correctness for bidirectional batch exchange.
     * Since result pages don't have batch markers, we verify:
     * 1. Total pages received matches expected (approximately)
     * 2. Sample data values are correct
     * 3. All expected values are present in the received pages
     */
    private void verifyBidirectionalBatchDataCorrectness(
        List<List<Page>> expectedOutputBatches,
        List<Page> allOutputPages,
        int numBatches
    ) {
        // Calculate expected total positions
        int expectedTotalPositions = 0;
        for (List<Page> batchPages : expectedOutputBatches) {
            for (Page page : batchPages) {
                expectedTotalPositions += page.getPositionCount();
            }
        }

        // Calculate actual total positions
        int actualTotalPositions = allOutputPages.stream().mapToInt(Page::getPositionCount).sum();

        // Verify we got approximately the right number of positions
        // (pages might be split differently, so we allow some flexibility)
        assertThat(
            "Total positions should match expected (allowing for page splitting differences)",
            actualTotalPositions,
            equalTo(expectedTotalPositions)
        );

        // Collect all expected values
        List<Integer> expectedValues = new ArrayList<>();
        for (List<Page> batchPages : expectedOutputBatches) {
            for (Page page : batchPages) {
                if (page.getBlockCount() > 0 && page.getBlock(0) instanceof IntBlock) {
                    IntBlock block = (IntBlock) page.getBlock(0);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        expectedValues.add(block.getInt(block.getFirstValueIndex(p)));
                    }
                }
            }
        }

        // Collect all actual values (from output block index 1, which is the new block from EvalOperator)
        List<Integer> actualValues = new ArrayList<>();
        for (Page page : allOutputPages) {
            if (page.getBlockCount() > 1 && page.getBlock(1) instanceof IntBlock) {
                IntBlock block = (IntBlock) page.getBlock(1);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    actualValues.add(block.getInt(block.getFirstValueIndex(p)));
                }
            }
        }

        // Verify we have the same number of values
        assertThat("Should have same number of values", actualValues.size(), equalTo(expectedValues.size()));

        // Verify values match (order might differ due to page splitting, so we sort)
        expectedValues.sort(Integer::compareTo);
        actualValues.sort(Integer::compareTo);
        assertThat("Values should match", actualValues, equalTo(expectedValues));
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

}
