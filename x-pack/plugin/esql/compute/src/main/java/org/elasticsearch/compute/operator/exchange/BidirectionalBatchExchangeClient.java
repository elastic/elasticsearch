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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
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

    /**
     * Functional interface for server setup callback.
     * Called when a new server connection needs to be established.
     */
    @FunctionalInterface
    public interface ServerSetupCallback {
        /**
         * Send setup request to the server.
         *
         * @param serverNode the server node to connect to
         * @param clientToServerId the per-server unique client-to-server exchange ID
         * @param serverToClientId the shared server-to-client exchange ID
         * @param listener called with the plan string (nullable) on success, or failure
         */
        void sendSetupRequest(DiscoveryNode serverNode, String clientToServerId, String serverToClientId, ActionListener<String> listener);
    }

    // Per-server state (one per server)
    private final Map<String, ServerConnection> serverConnections = new HashMap<>();

    // Shared state (single instance for all servers)
    private ExchangeSourceHandler serverToClientSourceHandler;
    private BatchSortedExchangeSource sortedSource; // Wraps ExchangeSource with sorting

    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final Object sendFinishLock = new Object(); // Synchronizes sendPage() and finish() to prevent race
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private volatile boolean closed = false; // Track if close() has been called (for idempotency)
    // Track batch counts to ensure all batches complete before closing
    private final AtomicInteger startedBatchCount = new AtomicInteger(0);
    private final AtomicInteger completedBatchCount = new AtomicInteger(0);

    // Server setup callback - called lazily when first page is sent to a server
    private final ServerSetupCallback serverSetupCallback;

    // Callback for when lookup plan is received from server (for profiling)
    @Nullable
    private final java.util.function.Consumer<String> lookupPlanConsumer;

    /**
     * Create a new BidirectionalBatchExchangeClient.
     *
     * @param sessionId session ID for the client
     * @param exchangeService  the exchange service
     * @param executor         executor for async operations
     * @param maxBufferSize    maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task             task for transport-based remote sink
     * @param batchExchangeStatusListener listener that will be called when batch exchange status is received (success or failure)
     * @param settings         settings for exchange configuration
     * @param serverSetupCallback callback to send setup request when a new server is connected
     * @param lookupPlanConsumer optional callback to receive lookup plan string from server setup response
     */
    public BidirectionalBatchExchangeClient(
        String sessionId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        ActionListener<Void> batchExchangeStatusListener,
        Settings settings,
        ServerSetupCallback serverSetupCallback,
        @Nullable java.util.function.Consumer<String> lookupPlanConsumer
    ) {
        // Client uses per-server clientToServerIds (in ServerConnection), but base class needs a value.
        // Pass the base clientToServerId which is used as a prefix for per-server IDs.
        super(
            sessionId,
            buildClientToServerId(sessionId),
            buildServerToClientId(sessionId),
            exchangeService,
            executor,
            maxBufferSize,
            transportService,
            task,
            settings
        );
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.serverSetupCallback = serverSetupCallback;
        this.lookupPlanConsumer = lookupPlanConsumer;
        logger.debug(
            "[LookupJoinClient] Created BidirectionalBatchExchangeClient: serverToClientId={}, maxBufferSize={}",
            serverToClientId,
            maxBufferSize
        );
        initialize();
    }

    /**
     * Initialize the shared client exchanges.
     * Called automatically from the constructor.
     * Per-server client-to-server exchanges are created lazily in getOrCreateServerConnection().
     */
    private void initialize() {
        logger.debug("[LookupJoinClient] Initializing BidirectionalBatchExchangeClient (shared state only)");

        // Create source handler for server-to-client direction (shared for all servers)
        serverToClientSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(serverToClientId, serverToClientSourceHandler);

        // Create BatchSortedExchangeSource that wraps ExchangeSource with sorting
        // All servers send results to the same source handler, sorted by batchId
        ExchangeSource exchangeSource = serverToClientSourceHandler.createExchangeSource();
        sortedSource = new BatchSortedExchangeSource(exchangeSource);
        logger.debug("[LookupJoinClient] Created shared server-to-client sorted source: exchangeId={}", serverToClientId);
    }

    /**
     * Get or create a server connection for the given server node.
     * If this is the first time connecting to this server, it will:
     * 1. Create client-to-server sink handler and sink
     * 2. Send setup request via serverSetupCallback
     * 3. On setup response, connect to server's sink and send batch exchange status request
     *
     * @param serverNode the server node to connect to
     * @return the server connection (may still be initializing)
     */
    private ServerConnection getOrCreateServerConnection(DiscoveryNode serverNode) {
        return serverConnections.computeIfAbsent(serverNode.getId(), nodeId -> {
            ServerConnection conn = new ServerConnection(serverNode, sessionId);
            initializeServerConnection(conn);
            return conn;
        });
    }

    /**
     * Initialize a server connection: create sink handler and send setup request.
     */
    private void initializeServerConnection(ServerConnection conn) {
        logger.debug(
            "[LookupJoinClient] Initializing server connection for node={}, clientToServerId={}",
            conn.serverNode.getId(),
            conn.clientToServerId
        );

        // Create sink handler for client-to-server direction (per-server)
        conn.clientToServerSinkHandler = exchangeService.createSinkHandler(conn.clientToServerId, maxBufferSize);
        conn.clientToServerSink = conn.clientToServerSinkHandler.createExchangeSink(() -> {});

        // When handler completes (buffer finished), clean up the sink handler
        conn.clientToServerSinkHandler.addCompletionListener(
            ActionListener.wrap(v -> exchangeService.finishSinkHandler(conn.clientToServerId, null), e -> {
                handleFailure("client-to-server exchange for " + conn.serverNode.getId(), e);
                exchangeService.finishSinkHandler(conn.clientToServerId, e);
            })
        );
        logger.debug("[LookupJoinClient] Created client-to-server sink handler: exchangeId={}", conn.clientToServerId);

        // Send setup request to server via callback
        serverSetupCallback.sendSetupRequest(conn.serverNode, conn.clientToServerId, serverToClientId, ActionListener.wrap(planString -> {
            try {
                logger.debug("[LookupJoinClient] Server setup complete for node={}", conn.serverNode.getId());
                // Pass lookup plan to consumer if provided
                if (lookupPlanConsumer != null && planString != null) {
                    lookupPlanConsumer.accept(planString);
                }
                // Connect to server's sink and send batch exchange status request
                connectToServerSink(conn);
                conn.setupReadyListener.onResponse(null);
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Server setup callback failed for node={}: {}", conn.serverNode.getId(), e.getMessage());
                handleFailure("server setup callback for " + conn.serverNode.getId(), e);
                conn.setupReadyListener.onFailure(e);
            }
        }, e -> {
            logger.error("[LookupJoinClient] Server setup failed for node={}: {}", conn.serverNode.getId(), e.getMessage());
            handleFailure("server setup for " + conn.serverNode.getId(), e);
            conn.setupReadyListener.onFailure(e);
        }));
    }

    /**
     * Connect to a server's sink handler for server-to-client exchange.
     * This should be called after the server has created its sink handler.
     */
    private void connectToServerSink(ServerConnection conn) {
        logger.debug(
            "[LookupJoinClient] Connecting to server sink for node={}, serverToClientId={}",
            conn.serverNode.getId(),
            serverToClientId
        );
        // All servers connect to the shared serverToClientSourceHandler
        connectRemoteSink(conn.serverNode, serverToClientId, serverToClientSourceHandler, ActionListener.wrap(nullValue -> {
            // Success - no action needed
        }, failure -> { handleFailure("server-to-client exchange for " + conn.serverNode.getId(), failure); }), "server sink handler");

        // Send batch exchange status request
        sendBatchExchangeStatusRequest(conn);
    }

    /**
     * Send batch exchange status request to a specific server.
     */
    private void sendBatchExchangeStatusRequest(ServerConnection conn) {
        try {
            Transport.Connection connection = transportService.getConnection(conn.serverNode);
            logger.debug(
                "[LookupJoinClient] Sending batch exchange status request for node={}, exchangeId={}",
                conn.serverNode.getId(),
                serverToClientId
            );
            ExchangeService.sendBatchExchangeStatusRequest(
                transportService,
                connection,
                serverToClientId,
                executor,
                ActionListener.<BatchExchangeStatusResponse>wrap(response -> {
                    logger.debug(
                        "[LookupJoinClient] Received batch exchange status response for node={}, success={}",
                        conn.serverNode.getId(),
                        response.isSuccess()
                    );
                    if (response.isSuccess()) {
                        logger.debug("[LookupJoinClient] Completing serverResponseListener for node={} (success)", conn.serverNode.getId());
                        conn.serverResponseListener.onResponse(null);
                        // Only call global listener if all servers have responded
                        checkAllServersResponded();
                    } else {
                        Exception failure = response.getFailure();
                        logger.warn(
                            "[LookupJoinClient] Batch exchange status response indicates failure for node={}: {}",
                            conn.serverNode.getId(),
                            failure != null ? failure.getMessage() : "unknown"
                        );
                        handleFailure("batch exchange status response for " + conn.serverNode.getId(), failure);
                        conn.serverResponseListener.onResponse(null);
                    }
                }, failure -> {
                    logger.error(
                        "[LookupJoinClient] Failed to receive batch exchange status response for node={}: {}",
                        conn.serverNode.getId(),
                        failure.getMessage()
                    );
                    handleFailure("batch exchange status response (transport error) for " + conn.serverNode.getId(), failure);
                    conn.serverResponseListener.onResponse(null);
                })
            );
            conn.requestSent = true;
            logger.debug("[LookupJoinClient] Batch exchange status request sent for node={}", conn.serverNode.getId());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send batch exchange status request for node [" + conn.serverNode.getId() + "]", e);
        }
    }

    /**
     * Check if all servers have responded successfully, and if so, call the global success listener.
     */
    private void checkAllServersResponded() {
        for (ServerConnection conn : serverConnections.values()) {
            if (conn.serverResponseListener.isDone() == false) {
                return; // Not all servers have responded yet
            }
        }
        // All servers responded - call global success listener if no failure
        if (failureRef.get() == null && batchExchangeStatusListener != null) {
            logger.debug("[LookupJoinClient] All servers responded successfully, calling batch exchange status listener");
            try {
                batchExchangeStatusListener.onResponse(null);
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Exception in batch exchange status listener callback", e);
            }
        }
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
     * Returns true if the client has fully finished - all batches complete AND all server responses were received.
     * The server responses confirm whether each server succeeded or failed.
     * If any server failed, hasFailed() will return true and the failure can be retrieved.
     */
    public boolean isFinished() {
        // Check if all servers have responded
        if (allServersResponded() == false) {
            return false;
        }
        // If all sent batches have been completed and all servers responded, we're done.
        int started = startedBatchCount.get();
        int completed = completedBatchCount.get();
        if (started > 0 && completed >= started) {
            return true;
        }
        // Also check page cache for edge cases (e.g., no batches sent)
        return isPageCacheDone();
    }

    /**
     * Check if all servers have responded.
     */
    private boolean allServersResponded() {
        if (serverConnections.isEmpty()) {
            return true; // No servers connected yet - considered done
        }
        for (ServerConnection conn : serverConnections.values()) {
            if (conn.serverResponseListener.isDone() == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when all server responses are received.
     * Use this to block while waiting for all servers' success/failure confirmation.
     */
    public IsBlockedResult waitForServerResponse() {
        // Find a server that hasn't responded yet
        for (ServerConnection conn : serverConnections.values()) {
            if (conn.serverResponseListener.isDone() == false) {
                return new IsBlockedResult(conn.serverResponseListener, "waiting for server response from " + conn.serverNode.getId());
            }
        }
        return NOT_BLOCKED;
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
     * Send a BatchPage to a specific server for processing.
     * The server connection will be lazily initialized if this is the first page sent to this server.
     * The batchId should be monotonically increasing for each call.
     * Currently, only single-page batches are supported, so isLastPageInBatch must always be true.
     *
     * @param batchPage the batch page to send
     * @param serverNode the server node to send the page to
     */
    public void sendPage(BatchPage batchPage, DiscoveryNode serverNode) {
        synchronized (sendFinishLock) {
            checkFailure();
            // Currently only single-page batches are supported
            if (batchPage.isLastPageInBatch() == false) {
                throw new IllegalArgumentException(
                    "Multi-page batches are not yet supported. Received page for batch "
                        + batchPage.batchId()
                        + " with isLastPageInBatch=false (pageIndex="
                        + batchPage.pageIndexInBatch()
                        + ")"
                );
            }
            // Get or create the server connection (lazily initializes if needed)
            ServerConnection conn = getOrCreateServerConnection(serverNode);
            // Track the number of batches that have been sent
            startedBatchCount.incrementAndGet();
            conn.clientToServerSink.addPage(batchPage);
        }
    }

    /**
     * Send a marker page to signal batch completion for an empty batch.
     *
     * @param batchId the batch ID
     * @param serverNode the server node to send the marker to
     */
    public void sendBatchMarker(long batchId, DiscoveryNode serverNode) {
        synchronized (sendFinishLock) {
            checkFailure();
            ServerConnection conn = getOrCreateServerConnection(serverNode);
            BatchPage marker = BatchPage.createMarker(batchId, 0);
            conn.clientToServerSink.addPage(marker);
        }
    }

    /**
     * Mark a batch as completed. Called by the consumer when it finishes processing a batch.
     * @param batchId the completed batch ID (used for logging, not tracked)
     */
    public void markBatchCompleted(long batchId) {
        completedBatchCount.incrementAndGet();
        logger.trace("[LookupJoinClient] Batch {} completed, total completed={}", batchId, completedBatchCount.get());
    }

    /**
     * Finish all client-to-server exchanges (no more batches will be sent to any server).
     */
    public void finish() {
        synchronized (sendFinishLock) {
            for (ServerConnection conn : serverConnections.values()) {
                if (conn.clientToServerSink != null && conn.clientToServerSink.isFinished() == false && conn.finished == false) {
                    logger.debug(
                        "[LookupJoinClient] Finishing client-to-server exchange for node={} (no more batches will be sent)",
                        conn.serverNode.getId()
                    );
                    conn.clientToServerSink.finish();
                    conn.finished = true;
                }
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
                logger.debug(
                    "[LookupJoinClient] Calling batch exchange status listener onFailure (from {}), failure={}",
                    context,
                    failure.getMessage()
                );
                batchExchangeStatusListener.onFailure(failure);
                logger.debug("[LookupJoinClient] Batch exchange status listener onFailure completed");
            }
            // NOW unblock the driver - the failure is already set in the operator
            // Finish the server-to-client source handler to unblock the client driver
            // The driver is waiting for pages from this exchange, so we need to signal completion
            if (serverToClientSourceHandler != null) {
                logger.debug("[LookupJoinClient] Finishing server-to-client source handler due to failure");
                serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            }
            // Close the sorted source to unblock any consumers waiting for pages
            // This ensures waitForPage() returns NOT_BLOCKED so the operator can check for failure
            if (sortedSource != null) {
                logger.debug("[LookupJoinClient] Closing sorted source due to failure");
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
        logger.debug("[LookupJoinClient] Closing BidirectionalBatchExchangeClient");

        // Finish all client-to-server exchanges FIRST to signal servers that no more batches will be sent
        // This allows the server drivers to finish and send responses
        try {
            finish();
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error calling finish()", e);
        }

        // Drain all sink handler buffers to release any pages
        for (ServerConnection conn : serverConnections.values()) {
            try {
                if (conn.clientToServerSinkHandler != null) {
                    logger.debug("[LookupJoinClient] Draining sink handler buffer for node={}", conn.serverNode.getId());
                    conn.clientToServerSinkHandler.onFailure(new TaskCancelledException("client closed"));
                }
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Error draining sink handler for node={}", conn.serverNode.getId(), e);
            }
        }

        // Wait for all server responses - this ensures all servers have finished processing
        for (ServerConnection conn : serverConnections.values()) {
            // First: wait for setup to complete (success or failure) so requestSent has its final value
            try {
                if (conn.setupReadyListener.isDone() == false) {
                    logger.debug("[LookupJoinClient] Waiting for setup completion for node={}", conn.serverNode.getId());
                    PlainActionFuture<Void> setupFuture = new PlainActionFuture<>();
                    conn.setupReadyListener.addListener(setupFuture);
                    setupFuture.actionGet(TimeValue.timeValueSeconds(30));
                }
            } catch (Exception e) {
                logger.warn("[LookupJoinClient] Timeout or exception waiting for setup completion for node={}", conn.serverNode.getId(), e);
            }

            // Then: if request was sent, wait for server response
            if (conn.requestSent) {
                try {
                    logger.debug(
                        "[LookupJoinClient] Waiting for server response from node={}, isDone={}",
                        conn.serverNode.getId(),
                        conn.serverResponseListener.isDone()
                    );
                    if (conn.serverResponseListener.isDone() == false) {
                        PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                        conn.serverResponseListener.addListener(waitFuture);
                        waitFuture.actionGet(TimeValue.timeValueSeconds(30));
                    }
                    logger.debug("[LookupJoinClient] Server response received from node={}", conn.serverNode.getId());
                } catch (Exception e) {
                    logger.warn(
                        "[LookupJoinClient] Timeout or exception waiting for server response from node={} - server may not have finished",
                        conn.serverNode.getId(),
                        e
                    );
                }
            }
        }

        // Log incomplete batches for debugging (but don't wait - we're shutting down)
        int started = startedBatchCount.get();
        int completed = completedBatchCount.get();
        if (started > 0 && completed < started) {
            logger.warn("[LookupJoinClient] Closing with incomplete batches: started={}, completed={}", started, completed);
        }

        // Finish the shared server-to-client source handler to signal completion
        if (serverToClientSourceHandler != null) {
            logger.debug("[LookupJoinClient] Finishing server-to-client source handler");
            serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
        }

        // Close the sorted source to release any buffered pages
        if (sortedSource != null) {
            logger.debug("[LookupJoinClient] Closing sorted source");
            sortedSource.close();
        }

        // Remove the source handler from the exchange service
        if (serverToClientSourceHandler != null) {
            logger.debug("[LookupJoinClient] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }

        logger.debug("[LookupJoinClient] BidirectionalBatchExchangeClient closed");
    }

    /**
     * Holds per-server state for client-to-server exchange.
     * Each server has its own sink handler and sink for sending pages,
     * but all servers share the same server-to-client source handler for receiving results.
     */
    private static class ServerConnection {
        final DiscoveryNode serverNode;
        final String clientToServerId;
        ExchangeSinkHandler clientToServerSinkHandler;
        ExchangeSink clientToServerSink;
        final SubscribableListener<Void> serverResponseListener = new SubscribableListener<>();
        final SubscribableListener<Void> setupReadyListener = new SubscribableListener<>();
        volatile boolean requestSent = false;
        volatile boolean finished = false;

        ServerConnection(DiscoveryNode serverNode, String sessionId) {
            this.serverNode = serverNode;
            // Each server gets a unique clientToServerId based on session and node
            this.clientToServerId = BidirectionalBatchExchangeBase.buildClientToServerId(sessionId) + "/" + serverNode.getId();
        }
    }
}
