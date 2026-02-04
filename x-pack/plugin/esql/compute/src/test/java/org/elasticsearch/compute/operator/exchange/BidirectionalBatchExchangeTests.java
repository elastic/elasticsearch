/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

@TestLogging(
    value = "org.elasticsearch.compute.operator.exchange.BatchDriver:TRACE,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient:TRACE,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer:TRACE,"
        + "org.elasticsearch.compute.operator.exchange.BatchContext:TRACE",
    reason = "debugging batch driver and bidirectional exchange"
)
public class BidirectionalBatchExchangeTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(BidirectionalBatchExchangeTests.class);

    /**
     * Settings to use only 1 channel for bidirectional exchange to avoid out-of-order page delivery.
     * This simplifies testing by ensuring pages are delivered in order without needing complex sorting.
     */
    private static final Settings SINGLE_CLIENT_SETTINGS = Settings.builder()
        .put(ExchangeSourceHandler.CONCURRENT_CLIENTS_SETTING.getKey(), 1)
        .build();

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
                batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 3, 3),
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
     * Test with mixed batches: empty and single-page batches with varying positions.
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
                // Batch 2: Single page with 5 positions
                if (batchId == 2) {
                    return createBatchWithPages(blockFactory, batchId, 1, 5).batches().get(0);
                }
                // Batch 3: Single page with 1 position
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
            TestData testData = createSimpleTestBatches(blockFactory, numBatches, 1, 1, 1, 10);
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

            // Create client holder
            AtomicReference<BidirectionalBatchExchangeClient> clientHolder = new AtomicReference<>();

            // Set up client on main thread
            // Note: Using pull-based model - pages are polled from client.pollPage()
            BidirectionalBatchExchangeClient client = setupClient(infra, threadPool, batchExchangeStatusFuture, sessionId);
            clientHolder.set(client);

            // Start server thread, wait for initialization, and connect client
            Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, TEST_TIMEOUT_SECONDS, sessionId);

            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: {}", numBatches);

            // Client sends all batches upfront
            DiscoveryNode serverNode = infra.serverTransportService().getLocalNode();
            sendAllBatchesUpfront(client, batches, batchesSent, serverNode);

            // Poll for all result pages and mark batches as complete
            for (int batchId = 0; batchId < numBatches; batchId++) {
                logger.info("[TEST-CLIENT] Polling for batch {} results", batchId);
                boolean batchComplete = false;
                long pollStartTime = System.currentTimeMillis();
                long pollTimeoutMs = TEST_TIMEOUT_SECONDS * 1000;

                while (batchComplete == false) {
                    if (System.currentTimeMillis() - pollStartTime > pollTimeoutMs) {
                        throw new AssertionError("Timeout waiting for batch " + batchId + " to complete");
                    }

                    if (client.hasFailed()) {
                        throw new AssertionError("Client failed while waiting for batch " + batchId);
                    }

                    IsBlockedResult blocked = client.waitForPage();
                    if (blocked.listener().isDone() == false) {
                        PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                        blocked.listener().addListener(waitFuture);
                        try {
                            waitFuture.actionGet(1, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            continue;
                        }
                    }

                    BatchPage resultPage;
                    while ((resultPage = client.pollPage()) != null) {
                        if (resultPage.isBatchMarkerOnly() == false) {
                            allOutputPagesRef.get().add(resultPage);
                        }
                        if (resultPage.isLastPageInBatch()) {
                            batchComplete = true;
                        }
                    }
                }

                client.markBatchCompleted(batchId);
                callbackBatchIds.add((long) batchId);
                processedBatches.incrementAndGet();
                logger.info("[TEST-CLIENT] Batch {} completed", batchId);
            }

            // Signal no more batches
            client.finish();

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
            List<Long> callbackBatchIds = Collections.synchronizedList(new ArrayList<>());
            AtomicReference<List<Page>> allOutputPagesRef = new AtomicReference<>(new ArrayList<>());
            AtomicReference<Exception> serverException = new AtomicReference<>();

            // Set up batch exchange status listener
            PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

            // Generate unique session ID for this test
            String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

            // Set up client on main thread
            // Note: Using pull-based model - pages are polled from client.pollPage()
            BidirectionalBatchExchangeClient client = setupClient(infra, threadPool, batchExchangeStatusFuture, sessionId);

            // Start server thread, wait for initialization, and connect client
            Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, TEST_TIMEOUT_SECONDS, sessionId);

            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: 1 (marker batch)");

            // Client sends NO pages - send a marker batch to represent "no data"
            logger.info("[TEST-CLIENT] Sending no pages, sending marker batch");
            DiscoveryNode serverNode = infra.serverTransportService().getLocalNode();
            client.sendBatchMarker(0, serverNode);

            // Poll for the response marker from the server
            logger.info("[TEST-CLIENT] Polling for response marker");
            boolean batchComplete = false;
            long pollStartTime = System.currentTimeMillis();
            long pollTimeoutMs = TEST_TIMEOUT_SECONDS * 1000;

            while (batchComplete == false) {
                if (System.currentTimeMillis() - pollStartTime > pollTimeoutMs) {
                    throw new AssertionError("Timeout waiting for batch 0 marker response");
                }

                if (client.hasFailed()) {
                    throw new AssertionError("Client failed while waiting for batch 0 marker response");
                }

                // Wait for a page to be available
                IsBlockedResult blocked = client.waitForPage();
                if (blocked.listener().isDone() == false) {
                    PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                    blocked.listener().addListener(waitFuture);
                    try {
                        waitFuture.actionGet(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        continue;
                    }
                }

                // Poll pages
                BatchPage resultPage;
                while ((resultPage = client.pollPage()) != null) {
                    logger.info(
                        "[TEST-CLIENT] Received result page: batchId={}, pageIndex={}, isLast={}, positions={}, isMarker={}",
                        resultPage.batchId(),
                        resultPage.pageIndexInBatch(),
                        resultPage.isLastPageInBatch(),
                        resultPage.getPositionCount(),
                        resultPage.isBatchMarkerOnly()
                    );

                    // For a marker batch, we should receive a marker response
                    if (resultPage.isBatchMarkerOnly() == false) {
                        allOutputPagesRef.get().add(resultPage);
                    }

                    if (resultPage.isLastPageInBatch()) {
                        batchComplete = true;
                    }
                }
            }

            // Mark batch as completed and update tracking
            client.markBatchCompleted(0);
            callbackBatchIds.add(0L);
            processedBatches.incrementAndGet();
            logger.info("[TEST-CLIENT] Batch 0 (marker) completed");

            // Signal no more batches will be sent
            client.finish();
            logger.info("[TEST-CLIENT] Called finish()");

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
     * Test that sending a multi-page batch (page with isLastPageInBatch=false) throws an IllegalArgumentException.
     * Currently only single-page batches are supported.
     */
    public void testMultiPageBatchNotSupported() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);
        try {
            // Set up batch exchange status listener
            PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

            // Generate unique session ID for this test
            String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

            // Set up client
            BidirectionalBatchExchangeClient client = setupClient(infra, threadPool, batchExchangeStatusFuture, sessionId);

            // Create a page to send
            IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1);
            builder.appendInt(42);
            IntBlock block = builder.build();
            Page page = new Page(block);

            // Try to send a page with isLastPageInBatch=false (multi-page batch)
            // This should throw an IllegalArgumentException because only single-page batches are supported
            logger.info("[TEST] About to call sendPage with isLastPageInBatch=false");
            DiscoveryNode serverNode = infra.serverTransportService().getLocalNode();
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
                client.sendPage(new BatchPage(page, 0, 0, false), serverNode);  // isLastPageInBatch=false
            });
            logger.info("[TEST] Caught expected IllegalArgumentException: {}", error.getMessage());

            // Verify the error message
            assertThat(error.getMessage(), containsString("Multi-page batches are not yet supported"));
            assertThat(error.getMessage(), containsString("batch 0"));
            assertThat(error.getMessage(), containsString("isLastPageInBatch=false"));

            // Clean up the page we created (it wasn't sent due to the error)
            page.releaseBlocks();

            // Clean up client
            client.close();

            logger.info("[TEST] Test completed successfully - IllegalArgumentException was thrown as expected");
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
            int numBatches = between(100, 1000);
            runBidirectionalBatchTestOnDemand(
                threadPool,
                blockFactory,
                numBatches,
                batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 1, 10),
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
                batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 1, 10),
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
        String sessionId
    ) throws Exception {
        logger.info("[TEST-CLIENT] Creating BidirectionalBatchExchangeClient with sessionId={}", sessionId);
        Task mockTask = mock(Task.class);
        // Test callback that immediately succeeds - server is set up manually in tests
        BidirectionalBatchExchangeClient.ServerSetupCallback testCallback = (serverNode, clientToServerId, serverToClientId, listener) -> {
            logger.debug("[TEST] ServerSetupCallback called for node={}", serverNode.getId());
            listener.onResponse("test-plan");
        };
        BidirectionalBatchExchangeClient client = new BidirectionalBatchExchangeClient(
            sessionId,
            infra.clientExchangeService(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            10,
            infra.clientTransportService(),
            mockTask,
            batchExchangeStatusListener,
            SINGLE_CLIENT_SETTINGS,
            testCallback,
            null // lookupPlanConsumer
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

        // Initialize client on main thread
        // Note: Using pull-based model - pages are polled from client.pollPage()
        BidirectionalBatchExchangeClient client = setupClient(infra, threadPool, batchExchangeStatusFuture, sessionId);

        // Start server thread, wait for initialization, and connect client
        Thread serverThread = startServerAndConnectClient(infra, threadPool, serverException, client, timeoutSeconds, sessionId);

        try {
            // Log number of batches to send
            logger.info("[TEST] Number of batches to send: {}", numBatches);

            // Process batches using pull-based model with per-batch verification
            for (int batchId = 0; batchId < numBatches; batchId++) {
                logger.debug("[TEST-CLIENT] Processing batch {}/{}", batchId + 1, numBatches);

                // Generate batch on demand
                List<Page> batchPages = batchGenerator.generateBatch(batchId);

                // Store copied input pages for verification (will be released after verification)
                List<Page> copiedBatchPages = copyPages(batchPages);
                synchronized (currentBatchInputPages) {
                    currentBatchInputPages.set(copiedBatchPages);
                }
                // Clear output pages collection for this batch
                synchronized (currentBatchOutputPages) {
                    currentBatchOutputPages.set(new ArrayList<>());
                }

                // Send the batch pages to the server
                DiscoveryNode serverNode = infra.serverTransportService().getLocalNode();
                sendBatchFromPages(client, batchPages, batchId, batchesSent, serverNode);

                // Poll for result pages until we receive the last page of this batch
                boolean batchComplete = false;
                long pollStartTime = System.currentTimeMillis();
                long pollTimeoutMs = timeoutSeconds * 1000;
                while (batchComplete == false) {
                    // Check for timeout
                    if (System.currentTimeMillis() - pollStartTime > pollTimeoutMs) {
                        throw new AssertionError("Timeout waiting for batch " + batchId + " to complete");
                    }

                    // Check for client failures
                    if (client.hasFailed()) {
                        throw new AssertionError("Client failed while waiting for batch " + batchId + " to complete");
                    }

                    // Wait for a page to be available (with timeout)
                    IsBlockedResult blocked = client.waitForPage();
                    if (blocked.listener().isDone() == false) {
                        // Wait for the page to become available (with short timeout to allow periodic checks)
                        PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                        blocked.listener().addListener(waitFuture);
                        try {
                            waitFuture.actionGet(1, TimeUnit.SECONDS);  // Short wait, will retry if no page
                        } catch (Exception e) {
                            // Timeout waiting for page - continue loop and check conditions
                            continue;
                        }
                    }

                    // Poll pages from the cache - poll ALL available pages
                    BatchPage resultPage;
                    while ((resultPage = client.pollPage()) != null) {
                        logger.debug(
                            "[TEST-CLIENT] Received result page: batchId={}, pageIndex={}, isLast={}, positions={}",
                            resultPage.batchId(),
                            resultPage.pageIndexInBatch(),
                            resultPage.isLastPageInBatch(),
                            resultPage.getPositionCount()
                        );

                        // Verify the page belongs to the current batch
                        assertThat("Result page batchId should match current batch", resultPage.batchId(), equalTo((long) batchId));

                        // Collect output page for verification (only non-marker pages)
                        if (resultPage.isBatchMarkerOnly() == false) {
                            synchronized (currentBatchOutputPages) {
                                currentBatchOutputPages.get().add(resultPage);
                            }
                        }

                        // Check if this is the last page of the batch
                        if (resultPage.isLastPageInBatch()) {
                            batchComplete = true;
                        }
                    }

                    // If no pages polled and cache not done, wait a bit before retrying
                    if (batchComplete == false && client.isPageCacheDone() == false) {
                        try {
                            Thread.sleep(10);  // Small sleep to avoid busy spinning
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("Interrupted while waiting for pages");
                        }
                    }
                }

                // Verify batch results
                List<Page> inputPages;
                List<Page> outputPages;
                synchronized (currentBatchInputPages) {
                    inputPages = currentBatchInputPages.get();
                }
                synchronized (currentBatchOutputPages) {
                    outputPages = new ArrayList<>(currentBatchOutputPages.get());
                }

                // Log batch completion details for debugging
                int inputPositions = inputPages != null ? inputPages.stream().mapToInt(Page::getPositionCount).sum() : 0;
                int outputPositions = outputPages.stream().mapToInt(Page::getPositionCount).sum();
                logger.debug(
                    "[TEST-CLIENT] Batch {} verification: inputPages={}, inputPositions={}, outputPages={}, outputPositions={}",
                    batchId,
                    inputPages != null ? inputPages.size() : 0,
                    inputPositions,
                    outputPages.size(),
                    outputPositions
                );

                // Verify results for this batch (only if there were input pages)
                if (inputPages != null && inputPages.isEmpty() == false) {
                    verifyBatchResultsFromInput(inputPages, outputPages, batchId, blockFactory);
                }

                // Release copied input pages after verification
                if (inputPages != null) {
                    for (Page page : inputPages) {
                        page.releaseBlocks();
                    }
                }

                // Mark batch as completed
                client.markBatchCompleted(batchId);
                callbackBatchIds.add((long) batchId);
                processedBatches.incrementAndGet();

                logger.debug("[TEST-CLIENT] Batch {} completed, processedBatches={}", batchId, processedBatches.get());
            }

            // All batches sent and processed - finish the client
            logger.info("[TEST] All {} batches processed, finishing client", numBatches);
            client.finish();

            // Wait for client to fully finish (page cache done AND server response received)
            assertBusy(() -> assertTrue("Client should be fully finished", client.isFinished()), timeoutSeconds, TimeUnit.SECONDS);

            // Wait for batch exchange status response (should indicate success)
            batchExchangeStatusFuture.actionGet(timeoutSeconds, TimeUnit.SECONDS);

            // Wait for server thread and check exceptions
            waitForServerThreadAndCheckExceptions(serverThread, serverException, timeoutSeconds * 1000);

            // Verify basic batch processing completion
            logger.info("[TEST] Verifying batch processing results");
            verifyBasicBatchResults(batchesSent, processedBatches, callbackBatchIds, numBatches);
            logger.info("[TEST] Test completed successfully");
        } finally {
            // Cleanup client
            try {
                client.close();
            } catch (Exception e) {
                logger.error("[TEST] Error closing client", e);
            }

            // Release any remaining output pages
            synchronized (currentBatchOutputPages) {
                List<Page> remainingPages = currentBatchOutputPages.get();
                if (remainingPages != null) {
                    for (Page page : remainingPages) {
                        try {
                            page.releaseBlocks();
                        } catch (Exception e) {
                            logger.error("[TEST] Error releasing page", e);
                        }
                    }
                }
            }

            // Cleanup exchange services and transport services
            cleanupServices(infra, threadPool);
        }
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

        // Client will connect to server sink lazily when first page is sent
        // The connection happens automatically in getOrCreateServerConnection()

        return serverThread;
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
        ExchangeService clientExchangeService = new ExchangeService(
            SINGLE_CLIENT_SETTINGS,
            threadPool,
            ThreadPool.Names.SEARCH,
            blockFactory
        );
        clientExchangeService.registerTransportHandler(clientTransportService);
        ExchangeService serverExchangeService = new ExchangeService(
            SINGLE_CLIENT_SETTINGS,
            threadPool,
            ThreadPool.Names.SEARCH,
            blockFactory
        );
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

        // Verify pageIndexInBatch is sequential within each batch
        logger.info("[TEST] Verifying pageIndexInBatch ordering");
        verifyPageIndexInBatchOrdering(receivedOutputPages);
        logger.info("[TEST] pageIndexInBatch verification passed");

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
                String clientToServerId = BidirectionalBatchExchangeBase.buildClientToServerId(sessionId);
                String serverToClientId = BidirectionalBatchExchangeBase.buildServerToClientId(sessionId);
                BidirectionalBatchExchangeServer server = new BidirectionalBatchExchangeServer(
                    sessionId,
                    clientToServerId,
                    serverToClientId,
                    infra.serverExchangeService(),
                    threadPool.executor(ThreadPool.Names.SEARCH),
                    10,
                    infra.serverTransportService(),
                    mockTask,
                    infra.clientTransportService().getLocalNode(),
                    SINGLE_CLIENT_SETTINGS
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
    private void sendAllBatchesUpfront(
        BidirectionalBatchExchangeClient client,
        List<List<Page>> batches,
        AtomicInteger batchesSent,
        DiscoveryNode serverNode
    ) {
        for (int batchId = 0; batchId < batches.size(); batchId++) {
            List<Page> batchPages = batches.get(batchId);
            sendBatchFromPages(client, batchPages, batchId, batchesSent, serverNode);
        }
    }

    /**
     * Send a batch from a list of pages.
     */
    private void sendBatchFromPages(
        BidirectionalBatchExchangeClient client,
        List<Page> batchPages,
        int batchId,
        AtomicInteger batchesSent,
        DiscoveryNode serverNode
    ) {
        if (batchPages.isEmpty()) {
            logger.debug("[TEST-CLIENT] Sending empty batch marker for batchId={}", batchId);
            client.sendBatchMarker(batchId, serverNode);
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
                client.sendPage(new BatchPage(page, batchId, pageIdx, isLastPageInBatch), serverNode);
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
        return new DriverContext(bigArrays, blockFactory, LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS);
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
     * Verify that pageIndexInBatch is sequential within each batch.
     * Pages for the same batch should have pageIndexInBatch values 0, 1, 2, ... in order.
     */
    private void verifyPageIndexInBatchOrdering(List<Page> receivedOutputPages) {
        // Group pages by batchId and track expected next index for each batch
        Map<Long, Integer> expectedNextIndexByBatch = new HashMap<>();

        for (Page page : receivedOutputPages) {
            if (page instanceof BatchPage batchPage) {
                long batchId = batchPage.batchId();
                int pageIndex = batchPage.pageIndexInBatch();

                int expectedIndex = expectedNextIndexByBatch.getOrDefault(batchId, 0);
                assertThat(
                    "pageIndexInBatch should be sequential within batch "
                        + batchId
                        + " (expected "
                        + expectedIndex
                        + ", got "
                        + pageIndex
                        + ")",
                    pageIndex,
                    equalTo(expectedIndex)
                );
                expectedNextIndexByBatch.put(batchId, expectedIndex + 1);
            } else {
                throw new AssertionError("Expected BatchPage but got " + page.getClass().getSimpleName());
            }
        }

        assertThat("Should have verified at least one batch", expectedNextIndexByBatch.size(), greaterThan(0));
        logger.info("[TEST] Verified pageIndexInBatch ordering for {} batches", expectedNextIndexByBatch.size());
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
