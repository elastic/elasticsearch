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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * Client-side handler for bidirectional batch exchange.
 * <p>
 * The client:
 * <ul>
 *   <li>Sends batches to the server via clientToServer exchange (using ExchangeSink)</li>
 *   <li>Receives results from the server via serverToClient exchange (using ExchangeSource)</li>
 *   <li>Tracks batch completion via {@link #markBatchCompleted(long)}</li>
 * </ul>
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
    private PageBufferOperator pageCacheSink;
    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final Object sendFinishLock = new Object(); // Synchronizes sendPage() and finish() to prevent race
    private final DiscoveryNode serverNode; // Server node for transport connection
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private final SubscribableListener<Void> serverResponseListener = new SubscribableListener<>(); // Listener for server response
    private volatile boolean requestSent = false; // Track if batch exchange status request was sent
    // Track batch IDs to ensure all batches complete before closing
    private volatile long startedBatchId = -1; // Highest batch ID that has been sent
    private volatile long completedBatchId = -1; // Highest batch ID that has been completed

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
     * @param settings         settings for exchange configuration
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
        Settings settings
    ) throws Exception {
        super(sessionId, exchangeService, executor, maxBufferSize, transportService, task, settings);
        this.clusterName = clusterName;
        this.serverNode = serverNode;
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.bigArrays = bigArrays;
        this.breaker = breaker;
        this.threadContext = threadContext;
        logger.debug(
            "[LookupJoinClient] Created BidirectionalBatchExchangeClient: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
            clientToServerId,
            serverToClientId,
            maxBufferSize
        );
        initialize();
    }

    /**
     * Initialize the client exchanges.
     * Called automatically from the constructor.
     */
    private void initialize() throws Exception {
        logger.debug("[LookupJoinClient] Initializing BidirectionalBatchExchangeClient");
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
        // When handler completes (buffer finished), clean up the sink handler
        // Handler completion fires when buffer.finish() completes (all pages consumed or drained)
        clientToServerSinkHandler.addCompletionListener(
            ActionListener.wrap(v -> exchangeService.finishSinkHandler(clientToServerId, null), e -> {
                handleFailure("client-to-server exchange", e);
                exchangeService.finishSinkHandler(clientToServerId, e);
            })
        );
        logger.debug("[LookupJoinClient] Created client-to-server sink handler: exchangeId={}", clientToServerId);

        // Create source handler for server-to-client direction
        serverToClientSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(serverToClientId, serverToClientSourceHandler);
        serverToClientSource = new ExchangeSourceOperator(serverToClientSourceHandler.createExchangeSource());
        logger.debug("[LookupJoinClient] Created server-to-client source handler: exchangeId={}", serverToClientId);

        // Create page cache sink - caches pages for the consumer to poll
        // The consumer is responsible for detecting batch completion by checking isLastPageInBatch()
        pageCacheSink = new PageBufferOperator();

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
        DriverContext clientDriverContext = new DriverContext(
            bigArrays,
            clientBlockFactory,
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS,
            "batch-exchange-client"
        );

        // Create BatchPageSorterOperator to ensure pages are delivered in order within each batch
        BatchPageSorterOperator batchPageSorter = new BatchPageSorterOperator();

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
            List.of(batchPageSorter), // Sort pages by pageIndexInBatch before passing to sink
            pageCacheSink,
            TimeValue.timeValueMinutes(5),
            clientLocalBreaker
        );
        logger.debug("[LookupJoinClient] Created client driver");

        // Start the driver immediately - it will wait for pages if exchange isn't connected yet
        logger.debug("[LookupJoinClient] Starting client driver to actively request pages from exchange");
        clientDriverFuture = new PlainActionFuture<>();
        // Create a listener that completes the future AND handles failures
        ActionListener<Void> driverListener = ActionListener.wrap(nullValue -> {
            // Driver completed successfully - complete the future
            clientDriverFuture.onResponse(nullValue);
            logger.debug("[LookupJoinClient] Client driver completed successfully");
        }, failure -> {
            // Driver failed - complete the future with exception AND propagate to failureRef
            clientDriverFuture.onFailure(failure);
            logger.error("[LookupJoinClient] Client driver failed", failure);
            handleFailure("client driver", failure);
        });
        Driver.start(threadContext, executor, clientDriver, 1000, driverListener);
        // Don't wait for completion - driver runs in background
        logger.debug("[LookupJoinClient] Client driver started successfully");
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
     * Polls a page from the cache.
     * The consumer should call this to retrieve pages and check {@link BatchPage#isLastPageInBatch()}
     * to detect batch completion.
     *
     * @return the next page, or null if the cache is empty
     */
    public BatchPage pollPage() {
        return pageCacheSink.poll();
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        return pageCacheSink.waitForPage();
    }

    /**
     * Returns true if the page cache is done (upstream finished and cache empty).
     */
    public boolean isPageCacheDone() {
        return pageCacheSink.isDone();
    }

    /**
     * Returns true if the client has fully finished - page cache is done AND server response was received.
     * The server response confirms whether the server succeeded or failed.
     * If the server failed, hasFailed() will return true and the failure can be retrieved.
     */
    public boolean isFinished() {
        return isPageCacheDone() && serverResponseListener.isDone();
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when the server response is received.
     * Use this to block while waiting for the server's success/failure confirmation.
     */
    public IsBlockedResult waitForServerResponse() {
        if (serverResponseListener.isDone()) {
            return NOT_BLOCKED;
        }
        return new IsBlockedResult(serverResponseListener, "waiting for server response");
    }

    /**
     * Returns the current number of pages in the cache.
     */
    public int pageCacheSize() {
        return pageCacheSink.size();
    }

    /**
     * Send batch exchange status request to server before page communication starts.
     * The server will reply after batch processing completes.
     * Called internally from connectToServerSink().
     */
    private void sendBatchExchangeStatusRequest() {
        try {
            Transport.Connection connection = transportService.getConnection(serverNode);
            logger.debug("[LookupJoinClient] Sending batch exchange status request for exchangeId={}", serverToClientId);
            ExchangeService.sendBatchExchangeStatusRequest(
                transportService,
                connection,
                serverToClientId,
                executor,
                ActionListener.wrap(response -> {
                    logger.debug(
                        "[LookupJoinClient] Received batch exchange status response for exchangeId={}, success={}",
                        serverToClientId,
                        response.isSuccess()
                    );
                    if (response.isSuccess()) {
                        // Success path: complete listener first, then call callback
                        logger.debug("[LookupJoinClient] Completing serverResponseListener (success path)");
                        serverResponseListener.onResponse(null);
                        logger.debug("[LookupJoinClient] Batch exchange completed successfully");
                        if (batchExchangeStatusListener != null) {
                            logger.debug("[LookupJoinClient] Calling batch exchange status listener onResponse (success)");
                            try {
                                batchExchangeStatusListener.onResponse(null);
                                logger.debug("[LookupJoinClient] Batch exchange status listener onResponse completed");
                            } catch (Exception e) {
                                logger.error("[LookupJoinClient] Exception in batch exchange status listener callback", e);
                            }
                        }
                    } else {
                        // Failure path: call handleFailure FIRST to propagate failure to operator,
                        // THEN complete listener. This prevents race where isFinished() returns true
                        // before the failure is set in the operator.
                        Exception failure = response.getFailure();
                        logger.warn(
                            "[LookupJoinClient] Batch exchange status response indicates failure: {}",
                            failure != null ? failure.getMessage() : "unknown"
                        );
                        handleFailure("batch exchange status response", failure);
                        logger.debug("[LookupJoinClient] Completing serverResponseListener (failure path, after handleFailure)");
                        serverResponseListener.onResponse(null);
                    }
                }, failure -> {
                    // Transport failure path: call handleFailure FIRST, then complete listener
                    logger.error(
                        "[LookupJoinClient] Failed to receive batch exchange status response for exchangeId={}: {}",
                        serverToClientId,
                        failure.getMessage()
                    );
                    handleFailure("batch exchange status response (transport error)", failure);
                    logger.debug("[LookupJoinClient] Completing serverResponseListener (transport failure path, after handleFailure)");
                    serverResponseListener.onResponse(null);
                })
            );
            requestSent = true; // Mark that request was sent
            logger.debug("[LookupJoinClient] Batch exchange status request sent for exchangeId={}", serverToClientId);
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
        logger.debug("[LookupJoinClient] Connecting to server sink handler via transport for server-to-client exchange");
        connectRemoteSink(serverNode, serverToClientId, serverToClientSourceHandler, ActionListener.wrap(nullValue -> {
            // Success - no action needed
        }, failure -> { handleFailure("server-to-client exchange", failure); }), "server sink handler");

        // Send batch exchange status request before page communication starts
        sendBatchExchangeStatusRequest();
    }

    /**
     * Send a BatchPage to the server for processing.
     * The batchId should be monotonically increasing for each call.
     * Currently, only single-page batches are supported, so isLastPageInBatch must always be true.
     */
    public void sendPage(BatchPage batchPage) {
        synchronized (sendFinishLock) {
            checkFailure();
            // Currently only single-page batches are supported
            //
            if (batchPage.isLastPageInBatch() == false) {
                throw new IllegalArgumentException(
                    "Multi-page batches are not yet supported. Received page for batch "
                        + batchPage.batchId()
                        + " with isLastPageInBatch=false (pageIndex="
                        + batchPage.pageIndexInBatch()
                        + ")"
                );
            }
            // Track the highest batch ID that has been sent
            startedBatchId = Math.max(startedBatchId, batchPage.batchId());
            clientToServerSink.addPage(batchPage);
        }
    }

    /**
     * Send a marker page to signal batch completion for an empty batch.
     *
     * @param batchId the batch ID
     */
    public void sendBatchMarker(long batchId) {
        synchronized (sendFinishLock) {
            checkFailure();
            BatchPage marker = BatchPage.createMarker(batchId, 0);
            clientToServerSink.addPage(marker);
        }
    }

    /**
     * Mark a batch as completed. Called by the consumer when it finishes processing a batch.
     * @param batchId the completed batch ID
     */
    public void markBatchCompleted(long batchId) {
        completedBatchId = batchId;
    }

    /**
     * Finish the client-to-server exchange (no more batches will be sent).
     */
    public void finish() {
        synchronized (sendFinishLock) {
            if (clientToServerSink != null && clientToServerSink.isFinished() == false) {
                logger.debug("[LookupJoinClient] Finishing client-to-server exchange (no more batches will be sent)");
                clientToServerSink.finish();
            }
        }
    }

    /**
     * Handle failures from any failure source (e.g., server setup, exchange, transport).
     *
     * Only the first failure will trigger the batchExchangeStatusListener.onFailure() callback.
     * Subsequent failures will be logged but ignored to prevent duplicate notifications.
     *
     * @param context the context of the failure (for logging), e.g., "server setup", "client-to-server exchange"
     * @param failure the failure exception
     */
    public void handleFailure(String context, Exception failure) {
        // Use compareAndSet to ensure only the first failure triggers the listener
        if (failureRef.compareAndSet(null, failure)) {
            logger.error("[LookupJoinClient] Failure from {} will be reported: {}", context, failure.getMessage());
            // Finish the server-to-client source handler to unblock the client driver
            // The driver is waiting for pages from this exchange, so we need to signal completion
            if (serverToClientSourceHandler != null) {
                logger.debug("[LookupJoinClient] Finishing server-to-client source handler due to failure");
                serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            }
            // Close the page cache sink to unblock any consumers waiting for pages
            // This ensures waitForPage() returns NOT_BLOCKED so the operator can check for failure
            if (pageCacheSink != null) {
                logger.debug("[LookupJoinClient] Closing page cache sink due to failure");
                pageCacheSink.close();
            }
            if (batchExchangeStatusListener != null) {
                logger.debug(
                    "[LookupJoinClient] Calling batch exchange status listener onFailure (from {}), failure={}",
                    context,
                    failure.getMessage()
                );
                batchExchangeStatusListener.onFailure(failure);
                logger.debug("[LookupJoinClient] Batch exchange status listener onFailure completed");
            }
        } else {
            // Failure already stored - just log, don't notify again
            Exception existingFailure = failureRef.get();
            logger.warn(
                "[LookupJoinClient] Additional failure received from {} is ignored: {}. Already reporting failure: {}",
                context,
                failure.getMessage(),
                existingFailure != null ? existingFailure.getMessage() : "unknown"
            );
        }
    }

    private void checkFailure() {
        Exception failure = failureRef.get();
        if (failure != null) {
            // Rethrow RuntimeExceptions directly (e.g., CircuitBreakingException) for proper test handling
            if (failure instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException("BidirectionalBatchExchangeClient failed", failure);
        }
    }

    @Override
    public void close() {
        logger.debug("[LookupJoinClient] Closing BidirectionalBatchExchangeClient");

        // Finish client-to-server exchange FIRST to signal the server that no more batches will be sent
        // This allows the server driver to finish and send the response
        try {
            finish();
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error calling finish()", e);
        }

        // Always drain the sink handler's buffer to release any pages, including late pages
        // that were added after the handler "completed" (race condition).
        // We call onFailure() directly on the handler because finishSinkHandler() might have
        // already removed the handler from ExchangeService via the completion listener.
        try {
            if (clientToServerSinkHandler != null) {
                Exception failure = failureRef.get();
                if (failure != null) {
                    logger.debug("[LookupJoinClient] Draining sink handler buffer due to failure: {}", failure.getMessage());
                } else {
                    logger.debug("[LookupJoinClient] Draining sink handler buffer during close");
                }
                // onFailure() calls buffer.finish(true) which drains all pages
                clientToServerSinkHandler.onFailure(new TaskCancelledException("client closed"));
            }
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error draining sink handler", e);
        }

        // Wait for server response - this ensures the server has finished processing
        // and sent all pages (including marker pages) before we close the source.
        if (requestSent) {
            try {
                logger.debug("[LookupJoinClient] Waiting for server response before closing: isDone={}", serverResponseListener.isDone());
                if (serverResponseListener.isDone() == false) {
                    // Wait with same timeout as client driver - server should complete before driver times out
                    // Create a temporary future for blocking wait
                    PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                    serverResponseListener.addListener(waitFuture);
                    waitFuture.actionGet(TimeValue.timeValueSeconds(30));
                }
                logger.debug("[LookupJoinClient] Server response received, server has finished processing");
            } catch (Exception e) {
                logger.warn(
                    "[LookupJoinClient] Timeout or exception waiting for server response - server may not have finished processing",
                    e
                );
                // Proceed with close to avoid hanging - timeout could be due to network issues or server failure
            }
        }

        // Wait for all started batches to complete, but only if there are no errors.
        // If there are errors, batches may never complete, so don't wait (fail fast).
        // IMPORTANT: We wait for batch completion BEFORE closing the source, so the client driver
        // can continue processing marker pages from the source while we wait.
        // Note: Driver exceptions are automatically propagated to failureRef via the listener
        // added in initializeClientDriver(), so we only need to check failureRef here.
        if (startedBatchId >= 0 && failureRef.get() == null) {
            logger.debug(
                "[LookupJoinClient] Waiting for all batches to complete: started={}, completed={}",
                startedBatchId,
                completedBatchId
            );
            long timeoutMs = 30_000; // 30 seconds
            long startTime = System.currentTimeMillis();
            long pollIntervalMs = 10; // Poll every 10ms
            while (completedBatchId < startedBatchId && (System.currentTimeMillis() - startTime) < timeoutMs) {
                // Check for errors during wait - if error occurs, stop waiting immediately
                if (failureRef.get() != null) {
                    logger.debug("[LookupJoinClient] Error detected during batch completion wait, stopping wait");
                    break;
                }

                // Check if the source is finished with no more data - if so, no more batch completions will arrive
                boolean sourceFinished = serverToClientSource != null && serverToClientSource.isFinished();
                int bufferSize = serverToClientSource != null ? serverToClientSource.bufferSize() : -1;
                if (sourceFinished && bufferSize == 0) {
                    logger.warn(
                        "[LookupJoinClient] Source finished with empty buffer but batches incomplete: started={}, completed={}",
                        startedBatchId,
                        completedBatchId
                    );
                    handleFailure(
                        "source finished early",
                        new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "Server completed before processing all batches: started=%d, completed=%d",
                                startedBatchId,
                                completedBatchId
                            )
                        )
                    );
                    break;
                }

                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (completedBatchId < startedBatchId) {
                if (failureRef.get() != null) {
                    logger.debug(
                        "[LookupJoinClient] Not all batches completed due to error: started={}, completed={}",
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
                        "[LookupJoinClient] Not all batches completed before timeout: started={}, completed={}: {}",
                        startedBatchId,
                        completedBatchId,
                        timeoutError.getMessage()
                    );
                    // Set the failure so subsequent operations know about the error
                    failureRef.compareAndSet(null, timeoutError);
                }
            } else {
                logger.debug("[LookupJoinClient] All batches completed: started={}, completed={}", startedBatchId, completedBatchId);
            }
        } else if (startedBatchId >= 0 && failureRef.get() != null) {
            logger.debug(
                "[LookupJoinClient] Skipping batch completion wait due to error: started={}, completed={}",
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
            // Log state before waiting
            boolean sourceFinished = serverToClientSource != null && serverToClientSource.isFinished();
            int sourceBufferSize = serverToClientSource != null ? serverToClientSource.bufferSize() : -1;
            boolean pageCacheDone = pageCacheSink != null && pageCacheSink.isDone();
            logger.debug(
                "[LookupJoinClient] Before waiting for driver: sourceFinished={}, sourceBufferSize={}, "
                    + "pageCacheDone={}, clientDriverFuture.isDone={}",
                sourceFinished,
                sourceBufferSize,
                pageCacheDone,
                clientDriverFuture.isDone()
            );
            try {
                logger.debug("[LookupJoinClient] Waiting for client driver to finish (30s timeout)...");
                clientDriverFuture.actionGet(TimeValue.timeValueSeconds(30));
                logger.debug("[LookupJoinClient] Client driver completed successfully");
                driverFinished = true;
            } catch (Exception e) {
                logger.debug("[LookupJoinClient] Exception waiting for driver completion, will abort driver: {}", e.getMessage());
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
                    logger.debug("[LookupJoinClient] Driver aborted successfully");
                } catch (Exception e) {
                    logger.debug("[LookupJoinClient] Driver abort did not complete within timeout, driver will finish eventually", e);
                }
            }
            // If driver finished, no need to abort - it's already done and cleaned up
        }

        // Now close the source - the driver has been aborted
        // The driver will have already finished the operator, so we just need to close it
        if (serverToClientSource != null) {
            logger.debug("[LookupJoinClient] Closing server-to-client source (driver has been aborted)");
            serverToClientSource.close();
        }
        if (serverToClientSourceHandler != null) {
            logger.debug("[LookupJoinClient] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }

        logger.debug("[LookupJoinClient] BidirectionalBatchExchangeClient closed");
    }
}
