/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.elasticsearch.compute.data.BatchMetadata;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@TestLogging(
    value = "org.elasticsearch.compute.operator.exchange.BatchDriver:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BatchContext:DEBUG",
    reason = "troubleshooting batch driver and bidirectional exchange"
)
public class BidirectionalBatchExchangeTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(BidirectionalBatchExchangeTests.class);

    private static final String MULTI_NODE = "multiNode";
    private static final String SINGLE_NODE = "singleNode";
    private static final int NUM_SERVER_NODES = 2;

    private final boolean useMultiNode;

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return List.of(new Object[] { SINGLE_NODE }, new Object[] { MULTI_NODE });
    }

    public BidirectionalBatchExchangeTests(String nodeMode) {
        this.useMultiNode = MULTI_NODE.equals(nodeMode);
    }

    private static final Settings CLIENT_SETTINGS = Settings.builder()
        .put(ExchangeSourceHandler.CONCURRENT_CLIENTS_SETTING.getKey(), 2)
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
     * Helper that runs a bidirectional batch test with the standard timeout,
     * managing threadPool and blockFactory lifecycle.
     */
    private void runSimpleBatchTest(int numBatches, BatchGenerator batchGenerator) throws Exception {
        runSimpleBatchTest(numBatches, batchGenerator, TEST_TIMEOUT_SECONDS);
    }

    private void runSimpleBatchTest(int numBatches, BatchGenerator batchGenerator, long timeoutSeconds) throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        try {
            runBidirectionalBatchTestOnDemand(threadPool, blockFactory, numBatches, batchGenerator, timeoutSeconds);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test with only a single batch.
     */
    public void testSingleBatch() throws Exception {
        BlockFactory blockFactory = blockFactory();
        runSimpleBatchTest(1, batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 3, 3));
    }

    /**
     * Test with an empty batch (0 pages) using a marker page.
     */
    public void testEmptyBatch() throws Exception {
        BlockFactory blockFactory = blockFactory();
        runSimpleBatchTest(1, batchId -> generateBatchPages(blockFactory, batchId, 0, 0, 1, 1));
    }

    /**
     * Test with mixed batches: empty and single-page batches with varying positions.
     */
    public void testMixedBatches() throws Exception {
        BlockFactory blockFactory = blockFactory();
        runSimpleBatchTest(4, batchId -> {
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
        });
    }

    /**
     * Test that batches sent upfront are handled gracefully by blocking until the previous batch completes.
     * This test verifies that the system can handle batches sent upfront without throwing exceptions.
     */
    public void testBatchProcessingAllBatchesUpfrontSuccess() throws Exception {
        ThreadPool threadPool = threadPool();
        BlockFactory blockFactory = blockFactory();
        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);

        int numBatches = between(2, 10);
        TestData testData = createSimpleTestBatches(blockFactory, numBatches, 1, 1, 1, 10);
        List<List<Page>> batches = testData.batches();
        List<List<Page>> expectedOutputBatches = testData.expectedOutputBatches();

        // Track results
        AtomicInteger processedBatches = new AtomicInteger(0);
        List<Long> callbackBatchIds = new ArrayList<>();
        AtomicInteger batchesSent = new AtomicInteger(0);
        AtomicReference<List<Page>> allOutputPagesRef = new AtomicReference<>(new ArrayList<>());

        // Set up batch exchange status listener
        PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

        // Generate unique session ID for this test
        String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

        // Track which server nodes workers are created on
        List<String> createdWorkerNodeIds = new ArrayList<>();

        BidirectionalBatchExchangeClient client = setupClient(
            infra,
            threadPool,
            batchExchangeStatusFuture,
            sessionId,
            createdWorkerNodeIds
        );

        try {

            logger.debug("[TEST] Number of batches to send: {}", numBatches);

            sendAllBatchesUpfront(client, batches, batchesSent);

            // Poll for all result pages and mark batches as complete.
            // Pages may arrive out of batch-ID order (e.g., batch 3 before batch 2) when multiple
            // servers process at different speeds. Track which batches have been received so we don't
            // lose pages consumed while waiting for an earlier batch.
            Set<Long> receivedBatchIds = new HashSet<>();
            for (int batchId = 0; batchId < numBatches; batchId++) {
                logger.debug("[TEST-CLIENT] Polling for batch {} results", batchId);
                final int currentBatchId = batchId;
                pollUntilBatchComplete(client, batchId, TEST_TIMEOUT_SECONDS, resultPage -> {
                    BatchMetadata metadata = resultPage.batchMetadata();
                    logger.debug(
                        "[TEST-CLIENT] Received result page: batchId={}, pageIndex={}, isLast={}, positions={}",
                        metadata.batchId(),
                        metadata.pageIndexInBatch(),
                        metadata.isLastPageInBatch(),
                        resultPage.getPositionCount()
                    );
                    if (resultPage.isBatchMarkerOnly() == false) {
                        allOutputPagesRef.get().add(resultPage);
                    }
                    if (metadata.isLastPageInBatch()) {
                        receivedBatchIds.add(metadata.batchId());
                    }
                }, () -> receivedBatchIds.contains((long) currentBatchId));

                client.markBatchCompleted(batchId);
                callbackBatchIds.add((long) batchId);
                processedBatches.incrementAndGet();
                logger.debug("[TEST-CLIENT] Batch {} completed", batchId);
            }

            // Signal no more batches
            logger.debug("[TEST] All {} batches processed, calling client.finish()", numBatches);
            client.finish();
            logger.debug("[TEST] client.finish() returned");

            // Wait for client completion (includes waiting for all server responses)
            logger.debug(
                "[TEST] Waiting for client completion: isFinished={}, isExchangeDone={}, hasFailed={}",
                client.isFinished(),
                client.isExchangeDone(),
                client.hasFailed()
            );
            waitForClientCompletion(client, batchExchangeStatusFuture, TEST_TIMEOUT_SECONDS);
            logger.debug("[TEST] waitForClientCompletion() returned");

            // Verify results and release pages
            verifyResultsAndReleasePages(
                batchesSent,
                processedBatches,
                callbackBatchIds,
                allOutputPagesRef,
                expectedOutputBatches,
                numBatches
            );
            allOutputPagesRef.set(null); // Prevent double-release in finally block

            // Verify worker node usage
            verifyWorkerNodes(infra, createdWorkerNodeIds, numBatches);

            logger.debug("[TEST] Test completed successfully");
        } finally {
            // Close client before transport services to avoid cascading disconnect errors
            logger.debug("[TEST] Entering finally block, calling client.close()");
            try {
                client.close();
                logger.debug("[TEST] client.close() completed");
            } catch (Exception e) {
                logger.error("[TEST] Error closing client", e);
            }

            // Release any collected output pages that weren't released by verification
            List<Page> remainingPages = allOutputPagesRef.get();
            if (remainingPages != null) {
                for (Page page : remainingPages) {
                    try {
                        page.releaseBlocks();
                    } catch (Exception e) {
                        logger.error("[TEST] Error releasing page", e);
                    }
                }
            }

            logger.debug("[TEST] Calling cleanupServices()");
            cleanupServices(infra, threadPool);
            logger.debug("[TEST] cleanupServices() completed");
        }
    }

    /**
     * Test that sending a multipage batch (page with isLastPageInBatch=false) throws an IllegalArgumentException.
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
            BidirectionalBatchExchangeClient client = setupClient(
                infra,
                threadPool,
                batchExchangeStatusFuture,
                sessionId,
                new ArrayList<>()
            );

            // Create a page to send
            IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1);
            builder.appendInt(42);
            IntBlock block = builder.build();
            Page page = new Page(block);

            // Try to send a page with isLastPageInBatch=false (multi-page batch)
            // This should throw an IllegalArgumentException because only single-page batches are supported
            logger.debug("[TEST] About to call sendPage with isLastPageInBatch=false");
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
                Page pageWithMetadata = page.withBatchMetadata(new BatchMetadata(0, 0, false));  // isLastPageInBatch=false
                client.sendPage(pageWithMetadata);
            });
            logger.debug("[TEST] Caught expected IllegalArgumentException: {}", error.getMessage());

            // Verify the error message
            assertThat(error.getMessage(), containsString("Multi-page batches are not yet supported"));
            assertThat(error.getMessage(), containsString("batch 0"));
            assertThat(error.getMessage(), containsString("isLastPageInBatch=false"));

            // Clean up the page we created (it wasn't sent due to the error)
            page.releaseBlocks();

            // Clean up client
            logger.debug("[TEST] Calling client.close()");
            client.close();
            logger.debug("[TEST] client.close() completed");

            logger.debug("[TEST] Test completed successfully - IllegalArgumentException was thrown as expected");
        } finally {
            logger.debug("[TEST] Entering finally block, calling cleanupServices()");
            cleanupServices(infra, threadPool);
            logger.debug("[TEST] cleanupServices() completed");
        }
    }

    /**
     * A randomized test for BidirectionalBatchExchange.
     */
    public void testBatchProcessingRandom() throws Exception {
        BlockFactory blockFactory = blockFactory();
        runSimpleBatchTest(between(100, 1000), batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 1, 10));
    }

    /**
     * A randomized test for BidirectionalBatchExchange with 20,000 batches.
     * Batches are generated on demand and verified incrementally
     * to avoid the test running out of memory.
     */
    public void testBatchProcessingRandomGiant() throws Exception {
        BlockFactory blockFactory = blockFactory();
        runSimpleBatchTest(20_000, batchId -> generateBatchPages(blockFactory, batchId, 1, 1, 1, 10), GIANT_BATCH_TEST_TIMEOUT_SECONDS);
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
     * Servers are created lazily inside the ServerSetupCallback, matching production behavior.
     *
     * @param infra test infrastructure containing exchange services and IDs
     * @param threadPool thread pool
     * @param batchExchangeStatusListener listener for batch exchange status completion
     * @param sessionId unique session ID for this test
     * @return initialized client
     */
    private BidirectionalBatchExchangeClient setupClient(
        TestInfrastructure infra,
        ThreadPool threadPool,
        ActionListener<Void> batchExchangeStatusListener,
        String sessionId,
        List<String> createdWorkerNodeIds
    ) {
        logger.debug(
            "[TEST-CLIENT] Creating BidirectionalBatchExchangeClient with sessionId={}, numServers={}",
            sessionId,
            infra.numServers()
        );
        Task mockTask = mock(Task.class);

        // Map node IDs to their index in the server lists for lookup in the callback
        Map<String, Integer> nodeIdToIndex = new HashMap<>();
        for (int i = 0; i < infra.numServers(); i++) {
            nodeIdToIndex.put(infra.serverTransportServices().get(i).getLocalNode().getId(), i);
        }

        // Lazy server setup callback - creates the BidirectionalBatchExchangeServer on demand,
        // matching production behavior where servers are created when the client needs them.
        // The server lifecycle is self-managing: driver completion triggers server.close() and
        // sends BatchExchangeStatusResponse back to the client.
        BidirectionalBatchExchangeClient.ServerSetupCallback testCallback = (node, clientToServerId, serverToClientId, listener) -> {
            try {
                int idx = nodeIdToIndex.get(node.getId());
                logger.debug("[TEST] ServerSetupCallback creating server for node={}, worker index={}", node.getId(), idx);

                DriverContext driverContext = driverContext();
                EvalOperator addOneOperator = createAddOneOperator(driverContext);

                BidirectionalBatchExchangeServer server = new BidirectionalBatchExchangeServer(
                    sessionId,
                    clientToServerId,
                    serverToClientId,
                    infra.serverExchangeServices().get(idx),
                    threadPool.executor(ThreadPool.Names.SEARCH),
                    10,
                    infra.serverTransportServices().get(idx),
                    mock(Task.class),
                    infra.clientTransportService().getLocalNode(),
                    CLIENT_SETTINGS
                );
                server.startWithOperators(
                    driverContext,
                    threadPool.getThreadContext(),
                    List.of(addOneOperator),
                    "test-cluster",
                    () -> {},
                    ActionListener.noop()
                );
                logger.debug("[TEST] Server created and started for node={}", node.getId());
                createdWorkerNodeIds.add(node.getId());
                listener.onResponse("test-plan");
            } catch (Exception e) {
                logger.error("[TEST] ServerSetupCallback failed for node={}: {}", node.getId(), e);
                listener.onFailure(e);
            }
        };

        // Round-robin server node supplier across all server nodes
        List<DiscoveryNode> serverNodes = infra.serverTransportServices().stream().map(MockTransportService::getLocalNode).toList();
        AtomicInteger serverNodeIndex = new AtomicInteger(0);
        BidirectionalBatchExchangeClient client = new BidirectionalBatchExchangeClient(
            sessionId,
            infra.clientExchangeService(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            10,
            infra.clientTransportService(),
            mockTask,
            batchExchangeStatusListener,
            CLIENT_SETTINGS,
            testCallback,
            null, // lookupPlanConsumer
            infra.numServers(), // maxWorkers
            () -> serverNodes.get(serverNodeIndex.getAndIncrement() % serverNodes.size()) // serverNodeSupplier
        );
        logger.debug("[TEST-CLIENT] Client initialized successfully");

        return client;
    }

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
        logger.debug("[TEST] Starting bidirectional batch test: numBatches={}", numBatches);

        TestInfrastructure infra = setupTestInfrastructure(threadPool, blockFactory);

        // Track results
        AtomicInteger processedBatches = new AtomicInteger(0);
        List<Long> callbackBatchIds = new ArrayList<>();
        AtomicInteger batchesSent = new AtomicInteger(0);

        // Per-batch data - stored and verified after each batch completes
        AtomicReference<List<Page>> currentBatchInputPages = new AtomicReference<>();
        AtomicReference<List<Page>> currentBatchOutputPages = new AtomicReference<>(new ArrayList<>());

        // Set up batch exchange status listener
        PlainActionFuture<Void> batchExchangeStatusFuture = new PlainActionFuture<>();

        // Generate unique session ID for this test
        String sessionId = "test-session-" + UUID.randomUUID().toString().substring(0, 8);

        // Track which server nodes workers are created on
        List<String> createdWorkerNodeIds = new ArrayList<>();

        // Initialize client on main thread - servers are created lazily inside the callback
        // Note: Using pull-based model - pages are polled from client.pollPage()
        BidirectionalBatchExchangeClient client = setupClient(
            infra,
            threadPool,
            batchExchangeStatusFuture,
            sessionId,
            createdWorkerNodeIds
        );

        try {
            // Log number of batches to send
            logger.debug("[TEST] Number of batches to send: {}", numBatches);

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

                // Send the batch pages (client routes to servers internally via serverNodeSupplier)
                sendBatchFromPages(client, batchPages, batchId, batchesSent);

                // Poll for result pages until we receive the last page of this batch
                final int currentBatchId = batchId;
                AtomicBoolean batchDone = new AtomicBoolean(false);
                pollUntilBatchComplete(client, batchId, timeoutSeconds, resultPage -> {
                    BatchMetadata metadata = resultPage.batchMetadata();
                    logger.debug(
                        "[TEST-CLIENT] Received result page: batchId={}, pageIndex={}, isLast={}, positions={}",
                        metadata.batchId(),
                        metadata.pageIndexInBatch(),
                        metadata.isLastPageInBatch(),
                        resultPage.getPositionCount()
                    );
                    assertThat("Result page batchId should match current batch", metadata.batchId(), equalTo((long) currentBatchId));
                    if (resultPage.isBatchMarkerOnly() == false) {
                        synchronized (currentBatchOutputPages) {
                            currentBatchOutputPages.get().add(resultPage);
                        }
                    }
                    if (metadata.isLastPageInBatch()) {
                        batchDone.set(true);
                    }
                }, batchDone::get);

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
            logger.debug("[TEST] All {} batches processed, calling client.finish()", numBatches);
            client.finish();
            logger.debug("[TEST] client.finish() returned");

            // Wait for client to fully finish (page cache done AND server response received)
            logger.debug(
                "[TEST] Waiting for client.isFinished(): isFinished={}, isExchangeDone={}, hasFailed={}",
                client.isFinished(),
                client.isExchangeDone(),
                client.hasFailed()
            );
            assertBusy(() -> assertTrue("Client should be fully finished", client.isFinished()), timeoutSeconds, TimeUnit.SECONDS);
            logger.debug("[TEST] client.isFinished() is now true");

            // Wait for batch exchange status response (should indicate success)
            // This completes when all servers have finished and sent their responses back
            logger.debug("[TEST] Waiting for batchExchangeStatusFuture (isDone={})", batchExchangeStatusFuture.isDone());
            batchExchangeStatusFuture.actionGet(timeoutSeconds, TimeUnit.SECONDS);
            logger.debug("[TEST] batchExchangeStatusFuture completed");

            // Verify basic batch processing completion
            logger.debug("[TEST] Verifying batch processing results");
            verifyBasicBatchResults(batchesSent, processedBatches, callbackBatchIds, numBatches);

            // Verify worker node usage
            verifyWorkerNodes(infra, createdWorkerNodeIds, numBatches);

            logger.debug("[TEST] Test completed successfully");
        } finally {
            // Cleanup client
            logger.debug("[TEST] Entering finally block, calling client.close()");
            try {
                client.close();
                logger.debug("[TEST] client.close() completed");
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
            logger.debug("[TEST] Calling cleanupServices()");
            cleanupServices(infra, threadPool);
            logger.debug("[TEST] cleanupServices() completed");
        }
    }

    /**
     * Test infrastructure record containing transport services, exchange services, and exchange IDs.
     * Server services are lists to support multi-node mode (one per server node).
     */
    private record TestInfrastructure(
        MockTransportService clientTransportService,
        List<MockTransportService> serverTransportServices,
        ExchangeService clientExchangeService,
        List<ExchangeService> serverExchangeServices
    ) {
        int numServers() {
            return serverTransportServices.size();
        }
    }

    /**
     * Set up test infrastructure: transport services, exchange services, and exchange IDs.
     * In single-node mode, creates 1 server. In multi-node mode, creates NUM_SERVER_NODES servers.
     */
    private TestInfrastructure setupTestInfrastructure(ThreadPool threadPool, BlockFactory blockFactory) {
        MockTransportService clientTransportService = newTransportService(threadPool);

        int numServers = useMultiNode ? NUM_SERVER_NODES : 1;
        List<MockTransportService> serverTransportServices = new ArrayList<>();
        List<ExchangeService> serverExchangeServices = new ArrayList<>();

        for (int i = 0; i < numServers; i++) {
            MockTransportService serverTransportService = newTransportService(threadPool);
            // Connect both ways for bidirectional communication (client <-> server, no server-to-server)
            AbstractSimpleTransportTestCase.connectToNode(clientTransportService, serverTransportService.getLocalNode());
            AbstractSimpleTransportTestCase.connectToNode(serverTransportService, clientTransportService.getLocalNode());

            ExchangeService serverExchangeService = new ExchangeService(CLIENT_SETTINGS, threadPool, ThreadPool.Names.SEARCH, blockFactory);
            serverExchangeService.registerTransportHandler(serverTransportService);

            serverTransportServices.add(serverTransportService);
            serverExchangeServices.add(serverExchangeService);
        }

        // Create client exchange service
        ExchangeService clientExchangeService = new ExchangeService(CLIENT_SETTINGS, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        clientExchangeService.registerTransportHandler(clientTransportService);

        return new TestInfrastructure(clientTransportService, serverTransportServices, clientExchangeService, serverExchangeServices);
    }

    /**
     * Cleanup exchange services and transport services.
     */
    private void cleanupServices(TestInfrastructure infra, ThreadPool threadPool) {
        logger.debug("[TEST-CLEANUP] Closing client exchange service");
        if (infra.clientExchangeService() != null) {
            infra.clientExchangeService().close();
        }
        logger.debug("[TEST-CLEANUP] Client exchange service closed");

        for (int i = 0; i < infra.serverExchangeServices().size(); i++) {
            ExchangeService serverExchangeService = infra.serverExchangeServices().get(i);
            if (serverExchangeService != null) {
                logger.debug("[TEST-CLEANUP] Closing server exchange service {}", i);
                serverExchangeService.close();
                logger.debug("[TEST-CLEANUP] Server exchange service {} closed", i);
            }
        }

        logger.debug("[TEST-CLEANUP] Closing client transport service");
        if (infra.clientTransportService() != null) {
            infra.clientTransportService().close();
        }
        logger.debug("[TEST-CLEANUP] Client transport service closed");

        for (int i = 0; i < infra.serverTransportServices().size(); i++) {
            MockTransportService serverTransportService = infra.serverTransportServices().get(i);
            if (serverTransportService != null) {
                logger.debug("[TEST-CLEANUP] Closing server transport service {}", i);
                serverTransportService.close();
                logger.debug("[TEST-CLEANUP] Server transport service {} closed", i);
            }
        }

        logger.debug("[TEST-CLEANUP] Terminating thread pool");
        terminate(threadPool);
        logger.debug("[TEST-CLEANUP] Thread pool terminated");
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
        logger.debug(
            "[TEST] waitForClientCompletion: waiting for source handler to finish (isFinished={})",
            client.getServerToClientSourceHandler().isFinished()
        );
        assertBusy(() -> assertTrue("Client source should be finished", client.getServerToClientSourceHandler().isFinished()));
        logger.debug("[TEST] waitForClientCompletion: source handler finished");

        // Wait for batch exchange status response (should indicate success)
        logger.debug(
            "[TEST] waitForClientCompletion: waiting for batchExchangeStatusFuture (isDone={})",
            batchExchangeStatusFuture.isDone()
        );
        batchExchangeStatusFuture.actionGet(timeoutSeconds, TimeUnit.SECONDS);
        logger.debug("[TEST] waitForClientCompletion: batchExchangeStatusFuture completed");
    }

    /**
     * Polls pages from the client until the target batch is complete.
     * Encapsulates the shared timeout/wait/poll loop used by multiple test methods.
     */
    private void pollUntilBatchComplete(
        BidirectionalBatchExchangeClient client,
        int batchId,
        long timeoutSeconds,
        Consumer<Page> pageConsumer,
        BooleanSupplier isComplete
    ) {
        long pollStartTime = System.currentTimeMillis();
        long pollTimeoutMs = timeoutSeconds * 1000;
        int pollIterations = 0;

        while (isComplete.getAsBoolean() == false) {
            long elapsed = System.currentTimeMillis() - pollStartTime;
            if (elapsed > pollTimeoutMs) {
                throw new AssertionError(
                    "Timeout waiting for batch "
                        + batchId
                        + " to complete after "
                        + elapsed
                        + "ms. Client state: isFinished="
                        + client.isFinished()
                        + ", isExchangeDone="
                        + client.isExchangeDone()
                        + ", hasFailed="
                        + client.hasFailed()
                        + ", sortedSource="
                        + client.getSortedSource()
                        + ", pollIterations="
                        + pollIterations
                );
            }

            if (client.hasFailed()) {
                throw new AssertionError("Client failed while waiting for batch " + batchId + " to complete");
            }

            pollIterations++;
            if (pollIterations % 10 == 0) {
                logger.debug(
                    "[TEST-CLIENT] Still waiting for batch {}: elapsed={}ms, sortedSource={}, isExchangeDone={}",
                    batchId,
                    elapsed,
                    client.getSortedSource(),
                    client.isExchangeDone()
                );
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

            Page resultPage;
            while ((resultPage = client.pollPage()) != null) {
                pageConsumer.accept(resultPage);
            }

            if (isComplete.getAsBoolean() == false && client.isExchangeDone() == false) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting for pages");
                }
            }
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
        logger.debug("[TEST] Verifying batch processing results");
        verifyBasicBatchResults(batchesSent, processedBatches, callbackBatchIds, numBatches);

        // Verify we received result pages
        List<Page> receivedOutputPages = allOutputPagesRef.get();
        int expectedTotalPages = expectedOutputBatches.stream().mapToInt(List::size).sum();
        logger.debug("[TEST] Received {} result pages, expected {} total pages", receivedOutputPages.size(), expectedTotalPages);

        // Verify data correctness
        logger.debug("[TEST] Verifying data correctness");
        verifyBidirectionalBatchDataCorrectness(expectedOutputBatches, receivedOutputPages, numBatches);
        logger.debug("[TEST] Data verification passed");

        // Verify pageIndexInBatch is sequential within each batch
        logger.debug("[TEST] Verifying pageIndexInBatch ordering");
        verifyPageIndexInBatchOrdering(receivedOutputPages);
        logger.debug("[TEST] pageIndexInBatch verification passed");

        // Release collected pages
        logger.debug("[TEST] Releasing collected pages");
        for (Page page : receivedOutputPages) {
            page.releaseBlocks();
        }
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
                client.sendPage(page.withBatchMetadata(new BatchMetadata(batchId, pageIdx, isLastPageInBatch)));
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
     * Verify that the correct number of workers were created on the correct server nodes.
     * In multiNode mode with enough batches, all server nodes should be used.
     * Workers should always be on server nodes, never on the client node.
     */
    private void verifyWorkerNodes(TestInfrastructure infra, List<String> createdWorkerNodeIds, int numBatches) {
        // Determine expected number of workers: min(numBatches, numServers)
        // because workers are created lazily, one per batch until maxWorkers is reached
        int expectedWorkers = Math.min(numBatches, infra.numServers());
        assertThat("Expected " + expectedWorkers + " workers to be created", createdWorkerNodeIds.size(), equalTo(expectedWorkers));

        // Collect the set of unique node IDs used
        Set<String> uniqueNodeIds = new HashSet<>(createdWorkerNodeIds);
        // Collect the server node IDs from infra
        Set<String> serverNodeIds = infra.serverTransportServices()
            .stream()
            .map(ts -> ts.getLocalNode().getId())
            .collect(Collectors.toSet());
        String clientNodeId = infra.clientTransportService().getLocalNode().getId();

        // All worker nodes must be server nodes, not the client node
        for (String nodeId : uniqueNodeIds) {
            assertThat("Worker node " + nodeId + " should be a server node", serverNodeIds, hasItem(nodeId));
            assertNotEquals("Worker node should not be the client node", clientNodeId, nodeId);
        }

        // In multiNode mode with enough batches, all server nodes should be used
        if (useMultiNode && numBatches >= infra.numServers()) {
            assertThat("All server nodes should be used in multiNode mode", uniqueNodeIds, equalTo(serverNodeIds));
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
            BatchMetadata metadata = page.batchMetadata();
            if (metadata != null) {
                long batchId = metadata.batchId();
                int pageIndex = metadata.pageIndexInBatch();

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
                throw new AssertionError("Expected page with BatchMetadata but got page without metadata");
            }
        }

        assertThat("Should have verified at least one batch", expectedNextIndexByBatch.size(), greaterThan(0));
        logger.debug("[TEST] Verified pageIndexInBatch ordering for {} batches", expectedNextIndexByBatch.size());
    }

    /**
     * Helper method to create an EvalOperator that adds 1 to int values.
     */
    private EvalOperator createAddOneOperator(DriverContext driverContext) {
        return new EvalOperator(driverContext, new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                if (page.getBlockCount() == 0) {
                    return driverContext.blockFactory().newConstantNullBlock(page.getPositionCount());
                }

                Block firstBlock = page.getBlock(0);
                if (firstBlock instanceof IntBlock == false) {
                    return driverContext.blockFactory().newConstantNullBlock(page.getPositionCount());
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
