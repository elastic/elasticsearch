/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.exchange.BatchPage;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Streaming version of LookupFromIndexOperator.
 * Uses BidirectionalBatchExchange to stream pages to server and receive results.
 */
public class StreamingLookupFromIndexOperator extends LookupFromIndexOperator {
    private static final Logger logger = LogManager.getLogger(StreamingLookupFromIndexOperator.class);
    private static final AtomicLong lookupJoinSessionIdGenerator = new AtomicLong(0);
    private final AtomicLong batchIdGenerator = new AtomicLong(0);

    private final AtomicReference<BidirectionalBatchExchangeClient> exchangeClientRef = new AtomicReference<>();
    private final DriverContext driverContext;
    // Synchronization lock for concurrent access to shared state
    private final Object stateLock = new Object();
    // Flag to track if operator has been closed
    private volatile boolean closed = false;

    /**
     * Per-batch state tracking. Each batch has its own state keyed by batchId.
     * This allows multiple batches to be in flight concurrently.
     */
    private static class BatchState {
        final ActionListener<OngoingJoin> listener;
        final Page inputPage;
        final List<Page> results = new ArrayList<>();

        BatchState(ActionListener<OngoingJoin> listener, Page inputPage) {
            this.listener = listener;
            this.inputPage = inputPage;
        }
    }

    private final Map<Long, BatchState> activeBatches = new ConcurrentHashMap<>();

    public StreamingLookupFromIndexOperator(
        List<MatchConfig> matchFields,
        String sessionId,
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions
    ) {
        super(
            matchFields,
            sessionId,
            driverContext,
            parentTask,
            maxOutstandingRequests,
            lookupService,
            lookupIndexPattern,
            lookupIndex,
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions
        );
        this.driverContext = driverContext;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<OngoingJoin> listener) {
        // Check if operator was closed
        synchronized (stateLock) {
            if (closed) {
                AsyncOperator.releasePageOnAnyThread(inputPage);
                listener.onFailure(new IllegalStateException("Operator was closed"));
                return;
            }
        }

        // Generate batchId early - this will be used as the key for per-batch state
        final long batchId = batchIdGenerator.incrementAndGet();

        // Create batch state and store it before starting async operations
        BatchState batchState = new BatchState(listener, inputPage);
        activeBatches.put(batchId, batchState);

        // Get or initialize exchange client, then process the page
        getOrInitializeExchangeClient(matchFieldsMapping, ActionListener.wrap(client -> {
            // Client is ready, send page via exchange and process results
            sendPageAndProcessResults(client, batchId, batchState);
        }, failure -> {
            // Remove batch state and notify listener of failure
            BatchState state = activeBatches.remove(batchId);
            if (state != null && state.listener == listener) {
                // Release input page on failure (may be called from different thread)
                AsyncOperator.releasePageOnAnyThread(inputPage);
                listener.onFailure(failure);
            }
        }));
    }

    private void getOrInitializeExchangeClient(MatchFieldsMapping mapping, ActionListener<BidirectionalBatchExchangeClient> listener) {

        // Check if client already exists
        BidirectionalBatchExchangeClient exchangeClient = exchangeClientRef.get();
        if (exchangeClient != null) {
            listener.onResponse(exchangeClient);
            return;
        }
        // Determine server node
        DiscoveryNode serverNode = determineServerNode();
        if (serverNode == null) {
            listener.onFailure(new IllegalStateException("Could not determine server node for lookup"));
            return;
        }

        // Create exchange client first (before sending setup request to server)
        // Generate streamingSessionId - it will be stored in the client and can be retrieved via getter
        String streamingSessionId = sessionId + "/lookup/" + lookupJoinSessionIdGenerator.incrementAndGet();
        ExchangeService exchangeService = lookupService.getExchangeService();
        try {
            // Create listeners that will be passed to the client during initialization
            Consumer<BatchPage> resultPageCollector = batchPage -> {
                if (batchPage.getPositionCount() > 0) {
                    long batchId = batchPage.batchId();
                    BatchState state = activeBatches.get(batchId);
                    if (state != null) {
                        // Create a new Page from the batchPage blocks (without deep copying)
                        // Increment ref count on blocks so they're not released when batchPage is released
                        Block[] blocks = new Block[batchPage.getBlockCount()];
                        for (int i = 0; i < batchPage.getBlockCount(); i++) {
                            blocks[i] = batchPage.getBlock(i);
                            blocks[i].incRef();
                        }
                        Page page = new Page(blocks);
                        state.results.add(page);
                    } else {
                        // Batch state was already removed - check if this is expected (operator closed or exchange failed)
                        // If not expected, throw to catch bugs where batches are cleaned up incorrectly
                        boolean operatorClosed;
                        boolean exchangeFailed;
                        synchronized (stateLock) {
                            operatorClosed = closed;
                            BidirectionalBatchExchangeClient client = exchangeClientRef.get();
                            exchangeFailed = client != null && client.hasFailed();
                        }
                        if (operatorClosed || exchangeFailed) {
                            // Expected: operator was closed or exchange failed, batches were cleaned up
                            // The batch's listener was already notified of failure/closure,
                            // so we can safely release the page and ignore it
                            logger.debug(
                                "StreamingLookupFromIndexOperator: Received result page for batch {} "
                                    + "but batch state was already removed (operatorClosed={}, exchangeFailed={})",
                                batchId,
                                operatorClosed,
                                exchangeFailed
                            );
                            // Release the page (may be called from different thread)
                            AsyncOperator.releasePageOnAnyThread(batchPage);
                        } else {
                            // Unexpected: batch state removed but operator not closed and exchange didn't fail
                            // This indicates a bug - throw to catch it
                            AsyncOperator.releasePageOnAnyThread(batchPage);
                            throw new IllegalStateException(
                                "Received result page for batch "
                                    + batchId
                                    + " but batch state was already cleaned up "
                                    + "and operator is not closed and exchange did not fail - this indicates a bug"
                            );
                        }
                    }
                }
            };
            BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener = completedBatchId -> handleBatchCompletion(
                completedBatchId
            );

            BidirectionalBatchExchangeClient newClient = new BidirectionalBatchExchangeClient(
                streamingSessionId,
                lookupService.getClusterService().getClusterName().value(),
                exchangeService,
                lookupService.getExecutor(),  // Use same executor as server
                1, // maxBufferSize
                lookupService.getTransportService(),
                parentTask,
                serverNode,
                ActionListener.wrap(nullValue -> handleBatchExchangeSuccess(), failure -> handleBatchExchangeFailure(failure)),
                driverContext.bigArrays(),
                lookupService.getBreaker(),  // Use global breaker for client driver (not parent's LocalCircuitBreaker)
                lookupService.getThreadContext(),
                resultPageCollector,
                batchDoneListener
            );

            // Create setup request - use streamingSessionId from client (or local variable since we just created it)
            // Using newClient.getSessionId() for consistency, though we could use the local variable
            LookupFromIndexService.Request setupRequest = new LookupFromIndexService.Request(
                sessionId,
                lookupIndex,
                lookupIndexPattern,
                mapping.reindexedMatchFields(),
                new Page(0), // Empty page for setup
                loadFields,
                source,
                rightPreJoinPlan,
                joinOnConditions,
                newClient.getSessionId()
            );

            // Send setup request to server and wait for response
            lookupService.lookupAsync(
                setupRequest,
                parentTask,
                ActionListener.wrap(
                    pages -> handleLookupSetupSuccess(pages, newClient, listener),
                    failure -> handleLookupSetupFailure(failure, listener)
                )
            );

            // at this point newClient is fully set up and ready to use, try to set it as the shared client
            BidirectionalBatchExchangeClient existing;
            synchronized (stateLock) {
                if (closed) {
                    listener.onFailure(new IllegalStateException("Operator was closed"));
                    return;
                }
                existing = exchangeClientRef.compareAndSet(null, newClient) ? null : exchangeClientRef.get();
            }
            if (existing != null) {
                // Another thread initialized it while we were creating ours, use that one and close ours
                newClient.close();
                listener.onResponse(existing);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Handle successful lookup setup. Completes client initialization by connecting to server.
     * Listeners are already set during client construction.
     */
    private void handleLookupSetupSuccess(
        List<Page> pages,
        BidirectionalBatchExchangeClient newClient,
        ActionListener<BidirectionalBatchExchangeClient> listener
    ) {
        try {
            // Server is ready, complete client setup
            // Connect to server sink
            newClient.connectToServerSink();

            // Client is fully initialized and ready (listeners were set during construction)
            listener.onResponse(newClient);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Handle lookup setup failure. Simply forwards the failure to the listener.
     */
    private void handleLookupSetupFailure(Exception failure, ActionListener<BidirectionalBatchExchangeClient> listener) {
        listener.onFailure(failure);
    }

    /**
     * Handle batch completion. Processes the batch results and notifies the listener.
     */
    private void handleBatchCompletion(long completedBatchId) {
        logger.debug("StreamingLookupFromIndexOperator: Batch {} completion marker received", completedBatchId);
        BatchState state = activeBatches.remove(completedBatchId);
        if (state != null) {
            logger.debug(
                "StreamingLookupFromIndexOperator: Batch {} completed, processing results ({} result pages)",
                completedBatchId,
                state.results.size()
            );
            // Check if operator is closed - if so, discard results (following AsyncOperator pattern)
            // This check happens after getting the batch state, similar to AsyncOperator.onSeqNoCompleted
            boolean operatorClosed;
            synchronized (stateLock) {
                operatorClosed = closed;
            }
            if (operatorClosed) {
                // Operator is closing, discard results and notify listener of failure
                // This follows the same pattern as AsyncOperator.discardResults()
                logger.debug(
                    "StreamingLookupFromIndexOperator: Batch {} completed but operator is closed, discarding results",
                    completedBatchId
                );
                // Release pages (may be called from different thread)
                for (Page page : state.results) {
                    AsyncOperator.releasePageOnAnyThread(page);
                }
                // Release inputPage (may be called from different thread)
                AsyncOperator.releasePageOnAnyThread(state.inputPage);
                // Notify listener that operator is closing
                try {
                    state.listener.onFailure(new IllegalStateException("Operator is closing"));
                } catch (Exception e) {
                    logger.debug(
                        "StreamingLookupFromIndexOperator: Exception notifying listener for batch {} (operator closing): {}",
                        completedBatchId,
                        e.getMessage()
                    );
                }
                return;
            }
            try {
                // Perform join with the collected results
                // OngoingJoin will own and release the pages when it's closed
                OngoingJoin ongoingJoin = new OngoingJoin(
                    new RightChunkedLeftJoin(state.inputPage, loadFields.size()),
                    state.results.iterator()
                );

                state.listener.onResponse(ongoingJoin);
                logger.debug(
                    "StreamingLookupFromIndexOperator: Batch {} completed successfully, result delivered to operator",
                    completedBatchId
                );
            } catch (Exception e) {
                logger.error(
                    "StreamingLookupFromIndexOperator: Batch {} failed during join creation: {}",
                    completedBatchId,
                    e.getMessage()
                );
                // Release pages if OngoingJoin creation fails - they're not owned by OngoingJoin yet
                // (may be called from different thread)
                for (Page page : state.results) {
                    AsyncOperator.releasePageOnAnyThread(page);
                }
                // Release inputPage if join creation fails (may be called from different thread)
                AsyncOperator.releasePageOnAnyThread(state.inputPage);
                try {
                    state.listener.onFailure(e);
                } catch (Exception e2) {
                    logger.debug(
                        "StreamingLookupFromIndexOperator: Exception notifying listener for batch {} (join creation failed): {}",
                        completedBatchId,
                        e2.getMessage()
                    );
                }
            }
        } else {
            // Batch state was already removed - check if this is expected (operator closed or exchange failed)
            // If not expected, throw to catch bugs where batches are cleaned up incorrectly
            boolean operatorClosed;
            boolean exchangeFailed;
            synchronized (stateLock) {
                operatorClosed = closed;
                BidirectionalBatchExchangeClient client = exchangeClientRef.get();
                exchangeFailed = client != null && client.hasFailed();
            }
            if (operatorClosed || exchangeFailed) {
                // Expected: operator was closed or exchange failed, batches were cleaned up
                // The batch's listener was already notified of failure/closure,
                // so we can safely ignore this completion marker
                logger.debug(
                    "StreamingLookupFromIndexOperator: Batch {} completed but state was already removed "
                        + "(operatorClosed={}, exchangeFailed={})",
                    completedBatchId,
                    operatorClosed,
                    exchangeFailed
                );
                // Don't throw - the batch was already handled, throwing would cause the failure
                // to be stored in failureRef but the listener wouldn't be notified, causing a hang
            } else {
                // Unexpected: batch state removed but operator not closed and exchange didn't fail
                // This indicates a bug - throw to catch it
                throw new IllegalStateException(
                    "Batch "
                        + completedBatchId
                        + " completed but batch state was already cleaned up "
                        + "and operator is not closed and exchange did not fail - this indicates a bug"
                );
            }
        }
    }

    private void sendPageAndProcessResults(BidirectionalBatchExchangeClient client, long batchId, BatchState batchState) {
        try {
            // Create batch page (each page is its own batch)
            // Use base class method to recreate inputBlockArray from inputPage using channelMapping
            // Calling applyMatchFieldsMapping also increments totalRows as a side effect
            Block[] inputBlockArray = applyMatchFieldsMapping(batchState.inputPage, matchFieldsMapping.channelMapping());
            BatchPage batchPage = new BatchPage(new Page(inputBlockArray), batchId, true);

            // Note: batchDoneListener is set once during client initialization and routes by batchId
            // We don't need to set it here - it's already set up to handle all batches

            // Send page - batch done listener will be called when batch completes
            client.sendPage(batchPage);
        } catch (Exception e) {
            // Remove batch state and notify listener of failure
            BatchState state = activeBatches.remove(batchId);
            if (state != null) {
                // Release input page on failure (may be called from different thread)
                AsyncOperator.releasePageOnAnyThread(batchState.inputPage);
                batchState.listener.onFailure(e);
            }
        }
    }

    /**
     * Handle batch exchange success. Verifies all batches have been processed.
     * If batches remain after waiting, this indicates a bug and they are cleaned up.
     */
    private void handleBatchExchangeSuccess() {
        logger.debug("Batch exchange completed successfully");
        // When batch exchange completes successfully, verify all batches have been processed.
        // Due to a potential race condition between the success response and marker page delivery,
        // we wait briefly for any in-flight batch completion markers to be processed.
        // The server sends the completion marker before the success response, so it should arrive soon.
        long waitTimeMs = 5000; // 5 seconds should be plenty
        long startTime = System.currentTimeMillis();
        long pollIntervalMs = 10;

        while (System.currentTimeMillis() - startTime < waitTimeMs) {
            synchronized (stateLock) {
                if (activeBatches.isEmpty()) {
                    logger.debug("Batch exchange completed successfully, all batches processed");
                    return;
                }
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // After waiting, if batches still remain, this indicates a bug
        synchronized (stateLock) {
            if (activeBatches.isEmpty() == false) {
                List<Long> remainingBatchIds = new ArrayList<>(activeBatches.keySet());
                logger.error(
                    "Batch exchange completed but {} active batches remain (batchIds: {}) - "
                        + "this indicates batches did not complete properly",
                    activeBatches.size(),
                    remainingBatchIds
                );
                // This should not happen in normal operation - batches should complete before exchange closes
                // But if it does, we need to handle it to prevent the operator from hanging
                List<BatchState> batchesToNotify = new ArrayList<>(activeBatches.values());
                activeBatches.clear();
                cleanupBatches(
                    batchesToNotify,
                    new IllegalStateException("Batch exchange completed but batch did not complete - this indicates a bug")
                );
            } else {
                logger.debug("Batch exchange completed successfully, all batches processed");
            }
        }
    }

    /**
     * Handle batch exchange failure. Cleans up all active batches and notifies them of the failure.
     */
    private void handleBatchExchangeFailure(Exception failure) {
        logger.error("Batch exchange failed", failure);
        // Notify all active batches of the failure
        synchronized (stateLock) {
            List<BatchState> batchesToNotify = new ArrayList<>(activeBatches.values());
            activeBatches.clear();
            cleanupBatches(batchesToNotify, failure);
        }
    }

    /**
     * Clean up batches by releasing resources and notifying listeners.
     * This method is thread-safe and can be called from any thread.
     *
     * @param batches the batches to clean up
     * @param failure the failure exception to notify listeners with
     */
    private void cleanupBatches(List<BatchState> batches, Exception failure) {
        for (BatchState state : batches) {
            try {
                // Release input page (may be called from different thread)
                AsyncOperator.releasePageOnAnyThread(state.inputPage);
                // Release any collected result pages (may be called from different thread)
                for (Page page : state.results) {
                    AsyncOperator.releasePageOnAnyThread(page);
                }
                state.listener.onFailure(failure);
            } catch (Exception e) {
                // Log but don't rethrow - listener might be in an invalid state during cleanup
                logger.debug("Failed to notify batch listener (operator may be closing)", e);
            }
        }
    }

    private DiscoveryNode determineServerNode() {
        try {
            var clusterState = lookupService.getClusterService().state();
            var projectState = lookupService.getProjectResolver().getProjectState(clusterState);
            var shardIterators = lookupService.getClusterService()
                .operationRouting()
                .searchShards(projectState, new String[] { lookupIndex }, java.util.Map.of(), "_local");
            if (shardIterators.size() != 1) {
                return null;
            }
            ShardIterator shardIt = shardIterators.get(0);
            ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                return null;
            }
            return clusterState.nodes().get(shardRouting.currentNodeId());
        } catch (Exception e) {
            logger.error("Failed to determine server node", e);
            return null;
        }
    }

    @Override
    protected void doClose() {
        // Set closed flag first to prevent new operations from starting
        closed = true;
        BidirectionalBatchExchangeClient clientToClose = null;

        // First, get the client and nullify its reference to prevent new requests
        synchronized (stateLock) {
            if (exchangeClientRef != null) {
                clientToClose = exchangeClientRef.getAndSet(null);
            }
        }

        // Close the exchange client. This call is designed to block until all
        // server responses (including the final marker page) have been received/processed
        // by the client's internal pipeline (BatchDetectionSinkOperator),
        // ensuring no more pages will be delivered to this operator's listener.
        if (clientToClose != null) {
            try {
                logger.debug("StreamingLookupFromIndexOperator: Closing exchange client");
                clientToClose.close();
                logger.debug("StreamingLookupFromIndexOperator: Exchange client closed gracefully");
            } catch (Exception e) {
                logger.error("StreamingLookupFromIndexOperator: Error closing exchange client", e);
            }
        }

        // Now that the exchange client is closed and no more pages are expected,
        // it is safe to clean up the internal batch state.
        // The client.close() waits for the driver to finish, which ensures all batchDoneListener
        // callbacks have completed. If batches remain here, it indicates a bug.
        List<BatchState> batchesToCleanup = new ArrayList<>();
        synchronized (stateLock) {
            if (activeBatches.isEmpty() == false) {
                List<Long> remainingBatchIds = new ArrayList<>(activeBatches.keySet());
                logger.error(
                    "StreamingLookupFromIndexOperator: {} batches did not complete before close (batchIds: {}) - this indicates a bug. "
                        + "All batches should have completed before client.close() returns (driver should have finished all callbacks)",
                    activeBatches.size(),
                    remainingBatchIds
                );
            }
            batchesToCleanup.addAll(activeBatches.values());
            activeBatches.clear(); // Now it's safe to clear activeBatches
            logger.debug("StreamingLookupFromIndexOperator: Cleared {} active batches for cleanup", batchesToCleanup.size());
        }

        // Proceed with cleaning up resources held by individual batch states
        cleanupBatches(batchesToCleanup, new IllegalStateException("Operator is closing"));
        logger.debug("StreamingLookupFromIndexOperator: Batch state resources cleaned up");

        // Double-check that no batches were added during cleanup (should not happen)
        // This would indicate a race condition or that performAsync was called after close started
        synchronized (stateLock) {
            if (activeBatches.isEmpty() == false) {
                List<Long> remainingBatchIds = new ArrayList<>(activeBatches.keySet());
                logger.error(
                    "StreamingLookupFromIndexOperator: {} unexpected batches found after cleanup (batchIds: {}) - "
                        + "this indicates batches were added after close started, which should not happen",
                    activeBatches.size(),
                    remainingBatchIds
                );
                // Clean up the remaining batches
                List<BatchState> remainingBatches = new ArrayList<>(activeBatches.values());
                activeBatches.clear();
                cleanupBatches(remainingBatches, new IllegalStateException("Operator is closing"));
            }
        }
        logger.debug("StreamingLookupFromIndexOperator: Finished doClose()");

        // Close ongoing join from parent class (this accesses 'ongoing' field which is only accessed from driver thread)
        super.doClose();
    }

    /**
     * OngoingJoin record for StreamingLookupFromIndexOperator.
     * This is a copy of the parent's OngoingJoin but with fixed release logic.
     */
    protected record StreamingOngoingJoin(RightChunkedLeftJoin join, Iterator<Page> itr) implements Releasable {
        @Override
        public void close() {
            // Use releasePageOnAnyThread to ensure pages can be released from any thread
            // This is important because pages may have been collected from exchange callbacks
            // that run on different threads
            Releasables.close(join, Releasables.wrap(() -> Iterators.map(itr, page -> () -> AsyncOperator.releasePageOnAnyThread(page))));
        }

        public void releaseOnAnyThread() {
            Releasables.close(
                join::releaseOnAnyThread,
                Releasables.wrap(() -> Iterators.map(itr, page -> () -> AsyncOperator.releasePageOnAnyThread(page)))
            );
        }
    }

    @Override
    protected void releaseFetchedOnAnyThread(LookupFromIndexOperator.OngoingJoin ongoingJoin) {
        // Convert parent's OngoingJoin to StreamingOngoingJoin for proper release
        // Pages came from exchange callback (different thread), so use releaseOnAnyThread
        if (ongoingJoin != null) {
            // Create a new StreamingOngoingJoin with the same join and iterator
            StreamingOngoingJoin streamingJoin = new StreamingOngoingJoin(ongoingJoin.join(), ongoingJoin.itr());
            streamingJoin.releaseOnAnyThread();
        }
    }
}
