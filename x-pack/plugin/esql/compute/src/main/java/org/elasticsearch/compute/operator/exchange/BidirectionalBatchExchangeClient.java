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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    private ExchangeSinkHandler clientToServerSinkHandler;
    private ExchangeSink clientToServerSink;
    private ExchangeSourceHandler serverToClientSourceHandler;
    private BatchSortedExchangeSource sortedSource; // Wraps ExchangeSource with sorting
    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final Object sendFinishLock = new Object(); // Synchronizes sendPage() and finish() to prevent race
    private final DiscoveryNode serverNode; // Server node for transport connection
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private final SubscribableListener<Void> serverResponseListener = new SubscribableListener<>(); // Listener for server response
    private volatile boolean requestSent = false; // Track if batch exchange status request was sent
    private volatile boolean closed = false; // Track if close() has been called (for idempotency)
    // Track batch counts to ensure all batches complete before closing
    private final AtomicInteger startedBatchCount = new AtomicInteger(0);
    private final AtomicInteger completedBatchCount = new AtomicInteger(0);

    /**
     * Create a new BidirectionalBatchExchangeClient.
     *
     * @param sessionId session ID for the client
     * @param clusterName cluster name
     * @param exchangeService  the exchange service
     * @param executor         executor for async operations
     * @param maxBufferSize    maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task             task for transport-based remote sink
     * @param serverNode       server node for transport connection
     * @param batchExchangeStatusListener listener that will be called when batch exchange status is received (success or failure)
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
        Settings settings
    ) throws Exception {
        super(sessionId, exchangeService, executor, maxBufferSize, transportService, task, settings);
        this.serverNode = serverNode;
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        // logger.debug(
        // "[LookupJoinClient] Created BidirectionalBatchExchangeClient: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
        // clientToServerId,
        // serverToClientId,
        // maxBufferSize
        // );
        initialize();
    }

    /**
     * Initialize the client exchanges.
     * Called automatically from the constructor.
     */
    private void initialize() {
        // logger.debug("[LookupJoinClient] Initializing BidirectionalBatchExchangeClient");

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
        // logger.debug("[LookupJoinClient] Created client-to-server sink handler: exchangeId={}", clientToServerId);

        // Create source handler for server-to-client direction
        serverToClientSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(serverToClientId, serverToClientSourceHandler);

        // Create BatchSortedExchangeSource that wraps ExchangeSource with sorting
        ExchangeSource exchangeSource = serverToClientSourceHandler.createExchangeSource();
        sortedSource = new BatchSortedExchangeSource(exchangeSource);
        // logger.debug("[LookupJoinClient] Created server-to-client sorted source: exchangeId={}", serverToClientId);
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
     * @return the next page, or null if no pages are available
     */
    public BatchPage pollPage() {
        return sortedSource.pollPage();
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        return sortedSource.waitForReading();
    }

    /**
     * Blocks until a page is ready to be polled OR the exchange is finished.
     * This guarantees that when NOT_BLOCKED is returned, either:
     * <ul>
     *   <li>{@link #hasReadyPages()} == true (pollPage will return a page)</li>
     *   <li>{@link #isPageCacheDone()} == true (no more pages expected)</li>
     * </ul>
     * This prevents busy-spinning when pages arrive out of order in multi-node scenarios.
     *
     * @return NOT_BLOCKED if a page is ready or finished, otherwise a blocked result
     */
    public IsBlockedResult waitUntilPageReady() {
        return sortedSource.waitUntilReady();
    }

    /**
     * Returns true if the sorted source is done (upstream finished and no buffered pages).
     */
    public boolean isPageCacheDone() {
        return sortedSource.isFinished();
    }

    /**
     * Returns true if the client has fully finished - all batches complete AND server response was received.
     * The server response confirms whether the server succeeded or failed.
     * If the server failed, hasFailed() will return true and the failure can be retrieved.
     */
    public boolean isFinished() {
        // If all sent batches have been completed and server responded, we're done.
        int started = startedBatchCount.get();
        int completed = completedBatchCount.get();
        if (started > 0 && completed >= started && serverResponseListener.isDone()) {
            return true;
        }
        // Also check page cache for edge cases (e.g., no batches sent)
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
        return sortedSource.bufferSize();
    }

    /**
     * Returns true if there are pages ready to be output (in correct order).
     * Unlike {@link #pageCacheSize()}, this only returns true when pages are
     * actually ready for consumption, not when they're buffered waiting for
     * out-of-order pages to arrive.
     */
    public boolean hasReadyPages() {
        return sortedSource.hasReadyPages();
    }

    /**
     * Send batch exchange status request to server before page communication starts.
     * The server will reply after batch processing completes.
     * Called internally from connectToServerSink().
     */
    private void sendBatchExchangeStatusRequest() {
        try {
            Transport.Connection connection = transportService.getConnection(serverNode);
            // logger.debug("[LookupJoinClient] Sending batch exchange status request for exchangeId={}", serverToClientId);
            ExchangeService.sendBatchExchangeStatusRequest(
                transportService,
                connection,
                serverToClientId,
                executor,
                ActionListener.<BatchExchangeStatusResponse>wrap(response -> {
                    // logger.debug(
                    // "[LookupJoinClient] Received batch exchange status response for exchangeId={}, success={}",
                    // serverToClientId,
                    // response.isSuccess()
                    // );
                    if (response.isSuccess()) {
                        // Success path: complete listener first, then call callback
                        // logger.debug("[LookupJoinClient] Completing serverResponseListener (success path)");
                        serverResponseListener.onResponse(null);
                        // logger.debug("[LookupJoinClient] Batch exchange completed successfully");
                        if (batchExchangeStatusListener != null) {
                            // logger.debug("[LookupJoinClient] Calling batch exchange status listener onResponse (success)");
                            try {
                                batchExchangeStatusListener.onResponse(null);
                                // logger.debug("[LookupJoinClient] Batch exchange status listener onResponse completed");
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
                        // logger.debug("[LookupJoinClient] Completing serverResponseListener (failure path, after handleFailure)");
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
                    // logger.debug("[LookupJoinClient] Completing serverResponseListener (transport failure path, after handleFailure)");
                    serverResponseListener.onResponse(null);
                })
            );
            requestSent = true; // Mark that request was sent
            // logger.debug("[LookupJoinClient] Batch exchange status request sent for exchangeId={}", serverToClientId);
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
        // logger.debug("[LookupJoinClient] Connecting to server sink handler via transport for server-to-client exchange");
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
            // Track the number of batches that have been sent
            startedBatchCount.incrementAndGet();
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
     * @param batchId the completed batch ID (used for logging, not tracked)
     */
    public void markBatchCompleted(long batchId) {
        completedBatchCount.incrementAndGet();
        // logger.trace("[LookupJoinClient] Batch {} completed, total completed={}", batchId, completedBatchCount.get());
    }

    /**
     * Finish the client-to-server exchange (no more batches will be sent).
     */
    public void finish() {
        synchronized (sendFinishLock) {
            if (clientToServerSink != null && clientToServerSink.isFinished() == false) {
                // logger.debug("[LookupJoinClient] Finishing client-to-server exchange (no more batches will be sent)");
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
            // Notify the operator's failure listener FIRST, before unblocking the driver.
            // This ensures that when finishEarly() wakes up the driver thread, the operator's
            // failure field is already set, so getOutput() will throw immediately.
            if (batchExchangeStatusListener != null) {
                // logger.debug(
                // "[LookupJoinClient] Calling batch exchange status listener onFailure (from {}), failure={}",
                // context,
                // failure.getMessage()
                // );
                batchExchangeStatusListener.onFailure(failure);
                // logger.debug("[LookupJoinClient] Batch exchange status listener onFailure completed");
            }
            // NOW unblock the driver - the failure is already set in the operator
            // Finish the server-to-client source handler to unblock the client driver
            // The driver is waiting for pages from this exchange, so we need to signal completion
            if (serverToClientSourceHandler != null) {
                // logger.debug("[LookupJoinClient] Finishing server-to-client source handler due to failure");
                serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            }
            // Close the sorted source to unblock any consumers waiting for pages
            // This ensures waitForPage() returns NOT_BLOCKED so the operator can check for failure
            if (sortedSource != null) {
                // logger.debug("[LookupJoinClient] Closing sorted source due to failure");
                sortedSource.close();
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
        if (closed) {
            return;
        }
        closed = true;
        // logger.debug("[LookupJoinClient] Closing BidirectionalBatchExchangeClient");

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
                // Exception failure = failureRef.get();
                // if (failure != null) {
                // logger.debug("[LookupJoinClient] Draining sink handler buffer due to failure: {}", failure.getMessage());
                // } else {
                // logger.debug("[LookupJoinClient] Draining sink handler buffer during close");
                // }
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
                // logger.debug("[LookupJoinClient] Waiting for server response before closing: isDone={}",
                // serverResponseListener.isDone());
                if (serverResponseListener.isDone() == false) {
                    PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                    serverResponseListener.addListener(waitFuture);
                    waitFuture.actionGet(TimeValue.timeValueSeconds(30));
                }
                // logger.debug("[LookupJoinClient] Server response received, server has finished processing");
            } catch (Exception e) {
                logger.warn(
                    "[LookupJoinClient] Timeout or exception waiting for server response - server may not have finished processing",
                    e
                );
                // Proceed with close to avoid hanging - timeout could be due to network issues or server failure
            }
        }

        // Log incomplete batches for debugging (but don't wait - we're shutting down)
        int started = startedBatchCount.get();
        int completed = completedBatchCount.get();
        if (started > 0 && completed < started) {
            logger.warn("[LookupJoinClient] Closing with incomplete batches: started={}, completed={}", started, completed);
        }

        // Finish the server-to-client source handler to signal completion
        if (serverToClientSourceHandler != null) {
            // logger.debug("[LookupJoinClient] Finishing server-to-client source handler");
            serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
        }

        // Close the sorted source to release any buffered pages
        if (sortedSource != null) {
            // logger.debug("[LookupJoinClient] Closing sorted source");
            sortedSource.close();
        }

        // Remove the source handler from the exchange service
        if (serverToClientSourceHandler != null) {
            // logger.debug("[LookupJoinClient] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }

        // logger.debug("[LookupJoinClient] BidirectionalBatchExchangeClient closed");
    }
}
