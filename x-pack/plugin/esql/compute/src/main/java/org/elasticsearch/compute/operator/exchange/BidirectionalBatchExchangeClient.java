/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Client-side handler for bidirectional batch exchange.
 * <p>
 * The client:
 * <ul>
 *   <li>Sends batches to the server via clientToServer exchange (using ExchangeSink)</li>
 *   <li>Receives results from the server via serverToClient exchange (using ExchangeSource)</li>
 *   <li>Detects batch completion by reading BatchPage with isLastPageInBatch=true</li>
 *   <li>Triggers onBatchDone callback when batch completes</li>
 * </ul>
 * <p>
 * Only one batch can be active at a time. The client must wait for onBatchDone before sending the next batch.
 */
public final class BidirectionalBatchExchangeClient extends BidirectionalBatchExchangeBase {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(BidirectionalBatchExchangeClient.class);

    private final String clusterName;

    private ExchangeSinkHandler clientToServerSinkHandler;
    private ExchangeSink clientToServerSink;
    private ExchangeSourceHandler serverToClientSourceHandler;
    private ExchangeSourceOperator serverToClientSource;
    private BigArrays bigArrays;
    private CircuitBreaker breaker;
    private ThreadContext threadContext;
    private Driver clientDriver;
    private PlainActionFuture<Void> clientDriverFuture; // Future for client driver completion
    private LocalCircuitBreaker clientLocalBreaker; // Local breaker for client driver context
    private BatchDetectionSinkOperator batchDetectionSink;
    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final DiscoveryNode serverNode; // Server node for transport connection
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private final PlainActionFuture<Void> serverResponseFuture = new PlainActionFuture<>(); // Future for server response completion
    private volatile boolean requestSent = false; // Track if batch exchange status request was sent
    // Track batch IDs to ensure all batches complete before closing
    private volatile long startedBatchId = -1; // Highest batch ID that has been sent
    private volatile long completedBatchId = -1; // Highest batch ID that has been completed

    /**
     * Listener for batch completion events.
     */
    public interface BatchDoneListener {
        /**
         * Called when a batch completes.
         * @param batchId the ID of the completed batch
         */
        void onBatchDone(long batchId);
    }

    /**
     * Create a new BidirectionalBatchExchangeClient.
     *
     * @param sessionId session ID for the driver
     * @param clusterName cluster name
     * @param exchangeService  the exchange service
     * @param executor         executor for async operations
     * @param maxBufferSize    maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task             task for transport-based remote sink
     * @param serverNode       server node for transport connection
     * @param batchExchangeStatusListener listener that will be called when batch exchange status is received (success or failure)
     * @param bigArrays        big arrays needed for the client driver context
     * @param breaker          circuit breaker for the client driver (should be global breaker, not a LocalCircuitBreaker)
     * @param threadContext    thread context needed for the client driver
     * @throws Exception if initialization fails
     */
    public BidirectionalBatchExchangeClient(
        String sessionId,
        String clusterName,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        DiscoveryNode serverNode,
        ActionListener<Void> batchExchangeStatusListener,
        BigArrays bigArrays,
        CircuitBreaker breaker,
        ThreadContext threadContext,
        Consumer<BatchPage> resultPageCollector,
        BatchDoneListener batchDoneListener
    ) throws Exception {
        super(sessionId, exchangeService, executor, maxBufferSize, transportService, task);
        this.clusterName = clusterName;
        this.serverNode = serverNode;
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.threadContext = threadContext;
        logger.info(
            "[CLIENT] Created BidirectionalBatchExchangeClient: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
            clientToServerId,
            serverToClientId,
            maxBufferSize
        );
        initialize(resultPageCollector, batchDoneListener);
    }

    /**
     * Initialize the client exchanges.
     * Called automatically from the constructor.
     */
    private void initialize(Consumer<BatchPage> resultPageCollector, BatchDoneListener batchDoneListener) throws Exception {
        logger.info("[CLIENT] Initializing BidirectionalBatchExchangeClient");
        if (bigArrays == null || breaker == null || threadContext == null) {
            throw new IllegalStateException("BigArrays, CircuitBreaker, and ThreadContext must be provided");
        }
        if (breaker instanceof LocalCircuitBreaker) {
            throw new IllegalArgumentException(
                "BidirectionalBatchExchangeClient requires a global CircuitBreaker, not a LocalCircuitBreaker. "
                    + "The client driver runs concurrently on a different thread, and LocalCircuitBreaker "
                    + "has single-thread assertions that would fail."
            );
        }

        // Create sink handler for client-to-server direction
        clientToServerSinkHandler = exchangeService.createSinkHandler(clientToServerId, maxBufferSize);
        clientToServerSink = clientToServerSinkHandler.createExchangeSink(() -> {});
        // Register failure listener for client-to-server exchange failures
        clientToServerSinkHandler.addCompletionListener(ActionListener.wrap(nullValue -> {
            // Success - no action needed
        }, failure -> { handleFailure("client-to-server exchange", failure); }));
        logger.debug("[CLIENT] Created client-to-server sink handler: exchangeId={}", clientToServerId);

        // Create source handler for server-to-client direction
        serverToClientSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(serverToClientId, serverToClientSourceHandler);
        serverToClientSource = new ExchangeSourceOperator(serverToClientSourceHandler.createExchangeSource());
        logger.debug("[CLIENT] Created server-to-client source handler: exchangeId={}", serverToClientId);

        // Create sink operator that detects batch completion
        batchDetectionSink = new BatchDetectionSinkOperator(serverToClientSource, resultPageCollector, batchDoneListener, batchId -> {
            // Update completedBatchId atomically using Math.max
            synchronized (BidirectionalBatchExchangeClient.this) {
                completedBatchId = Math.max(completedBatchId, batchId);
            }
        }, failureRef, () -> {
            // Check if there are incomplete batches - used by needsInput() to keep driver running
            synchronized (BidirectionalBatchExchangeClient.this) {
                return startedBatchId >= 0 && completedBatchId < startedBatchId;
            }
        });

        // Get node name from transport service
        String nodeName = transportService.getLocalNode().getName();

        // Use sessionId for shortDescription and description
        String shortDescription = "batch-exchange-client";
        Supplier<String> description = () -> "bidirectional-batch-exchange-client-" + sessionId;

        // Create a separate DriverContext for the client driver
        // This ensures isolation between the main workflow driver and the exchange client driver
        // Each driver needs its own workingSet for releasables and async action tracking
        //
        // The breaker passed to this client should be the global breaker (not a LocalCircuitBreaker)
        // because the client driver runs concurrently on a different thread, and LocalCircuitBreaker
        // has single-thread assertions that would fail if we used the parent driver's LocalCircuitBreaker.
        BlockFactory parentBlockFactory = new BlockFactory(breaker, bigArrays);
        this.clientLocalBreaker = new LocalCircuitBreaker(
            breaker,
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE.getBytes(),
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE.getBytes()
        );
        BlockFactory clientBlockFactory = parentBlockFactory.newChildFactory(clientLocalBreaker);
        DriverContext clientDriverContext = new DriverContext(bigArrays, clientBlockFactory, "batch-exchange-client");

        // Create driver to drive the ExchangeSourceOperator
        clientDriver = new Driver(
            sessionId,
            shortDescription,
            clusterName,
            nodeName,
            System.currentTimeMillis(),
            System.nanoTime(),
            clientDriverContext,
            description,
            serverToClientSource,
            List.of(), // No intermediate operators
            batchDetectionSink,
            TimeValue.timeValueMinutes(5),
            clientLocalBreaker
        );
        logger.info("[CLIENT] Created client driver");

        // Start the driver immediately - it will wait for pages if exchange isn't connected yet
        logger.info("[CLIENT] Starting client driver to actively request pages from exchange");
        clientDriverFuture = new PlainActionFuture<>();
        // Create a listener that completes the future AND handles failures
        ActionListener<Void> driverListener = ActionListener.wrap(nullValue -> {
            // Driver completed successfully - complete the future
            clientDriverFuture.onResponse(nullValue);
            logger.debug("[CLIENT] Client driver completed successfully");
        }, failure -> {
            // Driver failed - complete the future with exception AND propagate to failureRef
            clientDriverFuture.onFailure(failure);
            logger.error("[CLIENT][ERROR] Client driver failed", failure);
            handleFailure("client driver", failure);
        });
        Driver.start(threadContext, executor, clientDriver, 1000, driverListener);
        // Don't wait for completion - driver runs in background
        logger.debug("[CLIENT] Client driver started successfully");
    }

    /**
     * Get the server-to-client source handler.
     * Can be used to connect the server's sink by calling addRemoteSink() directly.
     */
    public ExchangeSourceHandler getServerToClientSourceHandler() {
        return serverToClientSourceHandler;
    }

    /**
     * Get the session ID (streaming session ID) used by this client.
     * @return the session ID
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Check if the exchange client has failed.
     * @return true if a failure has occurred, false otherwise
     */
    public boolean hasFailed() {
        return failureRef.get() != null;
    }

    /**
     * Send batch exchange status request to server before page communication starts.
     * The server will reply after batch processing completes.
     * Called internally from connectToServerSink().
     */
    private void sendBatchExchangeStatusRequest() {
        try {
            Transport.Connection connection = transportService.getConnection(serverNode);
            logger.info("[CLIENT] Sending batch exchange status request for exchangeId={}", serverToClientId);
            ExchangeService.sendBatchExchangeStatusRequest(
                transportService,
                connection,
                serverToClientId,
                executor,
                ActionListener.wrap(response -> {
                    // Mark server response as received FIRST so close() can proceed immediately
                    // Then call the listener callback asynchronously to avoid blocking
                    logger.debug("[CLIENT] Marking serverResponseFuture as complete (success path)");
                    serverResponseFuture.onResponse(null);
                    logger.debug("[CLIENT] serverResponseFuture marked as complete");

                    try {
                        logger.info(
                            "[CLIENT] Received batch exchange status response for exchangeId={}, success={}",
                            serverToClientId,
                            response.isSuccess()
                        );
                        if (response.isSuccess()) {
                            logger.info("[CLIENT] Batch exchange completed successfully");
                            if (batchExchangeStatusListener != null) {
                                logger.info("[CLIENT] Calling batch exchange status listener onResponse (success)");
                                // Execute listener callback synchronously - we're already on an executor thread
                                // (the responseExecutor from sendBatchExchangeStatusRequest), and the callback
                                // is lightweight (just synchronized map access and cleanup), so no need for
                                // additional async execution that could fail during shutdown
                                try {
                                    batchExchangeStatusListener.onResponse(null);
                                    logger.debug("[CLIENT] Batch exchange status listener onResponse completed");
                                } catch (Exception e) {
                                    logger.error("[CLIENT][ERROR] Exception in batch exchange status listener callback", e);
                                }
                            }
                        } else {
                            Exception failure = response.getFailure();
                            logger.warn(
                                "[CLIENT] Batch exchange status response indicates failure: {}",
                                failure != null ? failure.getMessage() : "unknown"
                            );
                            handleFailure("batch exchange status response", failure);
                        }
                    } catch (Exception e) {
                        logger.error("[CLIENT][ERROR] Exception processing batch exchange status response", e);
                    }
                }, failure -> {
                    // Mark server response as received FIRST so close() can proceed immediately
                    logger.debug("[CLIENT] Marking serverResponseFuture as complete (failure path)");
                    serverResponseFuture.onResponse(null);
                    logger.debug("[CLIENT] serverResponseFuture marked as complete");

                    try {
                        logger.error(
                            "[CLIENT][ERROR] Failed to receive batch exchange status response for exchangeId={}: {}",
                            serverToClientId,
                            failure.getMessage()
                        );
                        handleFailure("batch exchange status response (transport error)", failure);
                    } catch (Exception e) {
                        logger.error("[CLIENT][ERROR] Exception handling batch exchange status response failure: {}", e.getMessage());
                    }
                })
            );
            requestSent = true; // Mark that request was sent
            logger.info("[CLIENT] Batch exchange status request sent for exchangeId={}", serverToClientId);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send batch exchange status request for exchange [" + serverToClientId + "]", e);
        }
    }

    /**
     * Connect to the server's sink handler for server-to-client exchange.
     * This should be called after the server has created its sink handler.
     * Uses transport for error propagation.
     * Also sends batch exchange status request before page communication starts.
     */
    public void connectToServerSink() {
        // Use transport-based remote sink for error propagation
        logger.debug("[CLIENT] Connecting to server sink handler via transport for server-to-client exchange");
        connectRemoteSink(serverNode, serverToClientId, serverToClientSourceHandler, ActionListener.wrap(nullValue -> {
            // Success - no action needed
        }, failure -> { handleFailure("server-to-client exchange", failure); }), "server sink handler");

        // Send batch exchange status request before page communication starts
        sendBatchExchangeStatusRequest();
    }

    /**
     * Send a BatchPage to the server for processing.
     * The batchId should be monotonically increasing for each call
     * and isLastPageInBatch should be true only for the last page of a batch.
     * Sending a page for a new batch before the previous batch is done
     * (i.e. before isLastPageInBatch is set for a previous batch page) will result in an error.
     */
    public void sendPage(BatchPage batchPage) {
        checkFailure();
        // Track the highest batch ID that has been sent
        startedBatchId = Math.max(startedBatchId, batchPage.batchId());
        clientToServerSink.addPage(batchPage);
    }

    /**
     * Send a marker page to signal batch completion for an empty batch.
     *
     * @param batchId the batch ID
     */
    public void sendBatchMarker(long batchId) {
        checkFailure();
        BatchPage marker = BatchPage.createMarker(batchId);
        clientToServerSink.addPage(marker);
    }

    /**
     * Finish the client-to-server exchange (no more batches will be sent).
     */
    public void finish() {
        if (clientToServerSink != null && clientToServerSink.isFinished() == false) {
            logger.info("[CLIENT] Finishing client-to-server exchange (no more batches will be sent)");
            clientToServerSink.finish();
        }
    }

    /**
     * Handle failures from any of the three failure sources:
     * 1. Server-to-client exchange failure
     * 2. Client-to-server exchange failure
     * 3. Batch exchange status response failure
     *
     * Only the first failure will trigger the batchExchangeStatusListener.onFailure() callback.
     * Subsequent failures will be logged but ignored to prevent duplicate notifications.
     *
     * @param source the source of the failure (for logging)
     * @param failure the failure exception
     */
    private void handleFailure(String source, Exception failure) {
        // Use compareAndSet to ensure only the first failure triggers the listener
        if (failureRef.compareAndSet(null, failure)) {
            logger.error("[CLIENT][ERROR] First failure received from {}: {}", source, failure.getMessage());
            if (batchExchangeStatusListener != null) {
                logger.info(
                    "[CLIENT] Calling batch exchange status listener onFailure (from {}), failure={}",
                    source,
                    failure.getMessage()
                );
                batchExchangeStatusListener.onFailure(failure);
                logger.debug("[CLIENT] Batch exchange status listener onFailure completed");
            }
        } else {
            // Failure already stored - just log, don't notify again
            Exception existingFailure = failureRef.get();
            logger.warn(
                "[CLIENT] Additional failure received from {} (ignored, first failure was: {}): {}",
                source,
                existingFailure != null ? existingFailure.getMessage() : "unknown",
                failure.getMessage()
            );
        }
    }

    private void checkFailure() {
        Exception failure = failureRef.get();
        if (failure != null) {
            throw new RuntimeException("BidirectionalBatchExchangeClient failed", failure);
        }
    }

    @Override
    public void close() {
        logger.info("[CLIENT] Closing BidirectionalBatchExchangeClient");

        // Finish client-to-server exchange FIRST to signal the server that no more batches will be sent
        // This allows the server driver to finish and send the response
        finish();
        if (clientToServerSinkHandler != null) {
            logger.debug("[CLIENT] Finishing client-to-server sink handler");
            // If there's a failure, we need to abort the sink handler with the failure
            // to avoid assertion errors when the sink handler isn't fully finished
            Exception failure = failureRef.get();
            if (failure != null || clientToServerSinkHandler.isFinished() == false) {
                // Use a failure to abort the sink handler - this ensures isFinished() returns true
                Exception abortReason = failure != null
                    ? failure
                    : new IllegalStateException("Sink handler not finished during close - aborting");
                logger.debug("[CLIENT] Aborting sink handler due to: {}", abortReason.getMessage());
                exchangeService.finishSinkHandler(clientToServerId, abortReason);
            } else {
                exchangeService.finishSinkHandler(clientToServerId, null);
            }
        }

        // Wait for server response - this ensures the server has finished processing
        // and sent all pages (including marker pages) before we close the source
        // Only wait if we actually sent the request
        if (requestSent) {
            try {
                logger.debug("[CLIENT] Waiting for server response before closing: future.isDone={}", serverResponseFuture.isDone());
                if (serverResponseFuture.isDone() == false) {
                    // Wait with same timeout as client driver - server should complete before driver times out
                    serverResponseFuture.actionGet(TimeValue.timeValueSeconds(30));
                }
                logger.debug("[CLIENT] Server response received, server has finished processing");
            } catch (Exception e) {
                logger.error(
                    "[CLIENT][ERROR] Timeout or exception waiting for server response - server may not have finished processing",
                    e
                );
                // If waiting failed, this is an error - the server should have responded
                // But proceed with close to avoid hanging - this indicates a bug
            }
        } else {
            logger.debug("[CLIENT] Batch exchange status request was never sent, skipping wait for server response");
        }

        // Wait for all started batches to complete, but only if there are no errors.
        // If there are errors, batches may never complete, so don't wait (fail fast).
        // IMPORTANT: We wait for batch completion BEFORE closing the source, so the client driver
        // can continue processing marker pages from the source while we wait.
        // Note: Driver exceptions are automatically propagated to failureRef via the listener
        // added in initializeClientDriver(), so we only need to check failureRef here.
        if (startedBatchId >= 0 && failureRef.get() == null) {
            logger.info("[CLIENT] Waiting for all batches to complete: started={}, completed={}", startedBatchId, completedBatchId);
            long timeoutMs = 30_000; // 30 seconds
            long startTime = System.currentTimeMillis();
            long pollIntervalMs = 10; // Poll every 10ms
            long lastLogTime = startTime;
            long logIntervalMs = 1000; // Log every second during wait
            while (completedBatchId < startedBatchId && (System.currentTimeMillis() - startTime) < timeoutMs) {
                // Check for errors during wait - if error occurs, stop waiting immediately
                // Driver exceptions are automatically propagated to failureRef via listener
                if (failureRef.get() != null) {
                    logger.debug("[CLIENT] Error detected during batch completion wait, stopping wait");
                    break;
                }

                // Check if the source is finished with no more data - if so, no more batch completions will arrive
                // This can happen if the server completed before processing all batches (e.g., due to circuit breaker)
                boolean sourceFinished = serverToClientSource != null && serverToClientSource.isFinished();
                int bufferSize = serverToClientSource != null ? serverToClientSource.bufferSize() : -1;
                if (sourceFinished && bufferSize == 0) {
                    // Source is finished and buffer is empty - no more batch completion markers will arrive
                    logger.warn(
                        "[CLIENT] Source finished with empty buffer but batches incomplete: started={}, completed={}. "
                            + "Server may have completed before processing all batches.",
                        startedBatchId,
                        completedBatchId
                    );
                    IllegalStateException serverCompletedEarly = new IllegalStateException(
                        String.format(
                            Locale.ROOT,
                            "Server completed before processing all batches: started=%d, completed=%d",
                            startedBatchId,
                            completedBatchId
                        )
                    );
                    failureRef.compareAndSet(null, serverCompletedEarly);
                    break;
                }

                // Log periodically during wait to diagnose issues
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime >= logIntervalMs) {
                    boolean driverFinished = clientDriver != null && clientDriverFuture.isDone();
                    boolean hasIncomplete = startedBatchId >= 0 && completedBatchId < startedBatchId;
                    boolean sourceBlocked = serverToClientSource != null && serverToClientSource.isBlocked().listener().isDone() == false;
                    boolean canProduceMore = serverToClientSource != null && serverToClientSource.canProduceMoreDataWithoutExtraInput();
                    logger.info(
                        "[CLIENT] Still waiting for batches: started={}, completed={}, elapsedMs={}, driverFinished={},"
                            + " sourceFinished={}, hasIncomplete={}, bufferSize={}, sourceBlocked={}, canProduceMore={}",
                        startedBatchId,
                        completedBatchId,
                        currentTime - startTime,
                        driverFinished,
                        sourceFinished,
                        hasIncomplete,
                        bufferSize,
                        sourceBlocked,
                        canProduceMore
                    );
                    lastLogTime = currentTime;
                }

                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("[CLIENT] Interrupted while waiting for batch completion");
                    break;
                }
            }
            if (completedBatchId < startedBatchId) {
                if (failureRef.get() != null) {
                    logger.debug(
                        "[CLIENT] Not all batches completed due to error: started={}, completed={}",
                        startedBatchId,
                        completedBatchId
                    );
                } else {
                    IllegalStateException timeoutError = new IllegalStateException(
                        String.format(
                            Locale.ROOT,
                            "Not all batches completed before timeout: started=%d, completed=%d",
                            startedBatchId,
                            completedBatchId
                        )
                    );
                    logger.error(
                        "[CLIENT][ERROR] Not all batches completed before timeout: started={}, completed={}: {}",
                        startedBatchId,
                        completedBatchId,
                        timeoutError.getMessage()
                    );
                    // Set the failure so subsequent operations know about the error
                    failureRef.compareAndSet(null, timeoutError);
                }
            } else {
                logger.debug("[CLIENT] All batches completed: started={}, completed={}", startedBatchId, completedBatchId);
            }
        } else if (startedBatchId >= 0 && failureRef.get() != null) {
            logger.debug(
                "[CLIENT] Skipping batch completion wait due to error: started={}, completed={}",
                startedBatchId,
                completedBatchId
            );
        }

        // Wait for driver to finish completely before closing the source.
        // The driver will finish the operator when it's done, so we should wait for that
        // to avoid race conditions where we try to finish/close the source while the driver is still using it.
        // The driver will close clientLocalBreaker (passed as releasable) when it finishes
        if (clientDriver != null) {
            boolean driverFinished = false;
            try {
                logger.debug("[CLIENT] Waiting for client driver to finish before closing source");
                clientDriverFuture.actionGet(TimeValue.timeValueSeconds(30));
                logger.debug("[CLIENT] Client driver completed successfully");
                driverFinished = true;
            } catch (Exception e) {
                logger.debug("[CLIENT] Exception waiting for driver completion, will abort driver", e);
            }

            if (driverFinished == false) {
                // Driver didn't finish - use abort() to properly shut down.
                // abort() handles both cases:
                // - If driver hasn't started: calls drainAndCloseOperators() immediately (safe, no CME)
                // - If driver has started: cancels it and lets it finish naturally
                // This avoids calling close() on a potentially running driver which causes ConcurrentModificationException
                Exception abortReason = new RuntimeException("BidirectionalBatchExchangeClient closing");
                PlainActionFuture<Void> abortFuture = new PlainActionFuture<>();
                clientDriver.abort(abortReason, abortFuture);
                // Wait for abort to complete - the driver will finish after checking for cancellation
                try {
                    abortFuture.actionGet(TimeValue.timeValueSeconds(10));
                    logger.debug("[CLIENT] Driver aborted successfully");
                } catch (Exception e) {
                    logger.debug("[CLIENT] Driver abort did not complete within timeout, driver will finish eventually", e);
                }
            }
            // If driver finished, no need to abort - it's already done and cleaned up
        }

        // Now close the source - the driver has been aborted
        // The driver will have already finished the operator, so we just need to close it
        if (serverToClientSource != null) {
            logger.debug("[CLIENT] Closing server-to-client source (driver has been aborted)");
            serverToClientSource.close();
        }
        if (serverToClientSourceHandler != null) {
            logger.debug("[CLIENT] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }

        logger.info("[CLIENT] BidirectionalBatchExchangeClient closed");
    }
}
