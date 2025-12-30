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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
public final class BidirectionalBatchExchangeClient implements Releasable {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(BidirectionalBatchExchangeClient.class);

    private final String clientToServerId;
    private final String serverToClientId;
    private final String sessionId;
    private final String clusterName;
    private final ExchangeService exchangeService;
    private final Executor executor;
    private final int maxBufferSize;

    private ExchangeSinkHandler clientToServerSinkHandler;
    private ExchangeSink clientToServerSink;
    private ExchangeSourceHandler serverToClientSourceHandler;
    private ExchangeSourceOperator serverToClientSource;
    private BatchDoneListener batchDoneListener;
    private Consumer<BatchPage> resultPageCollector; // For collecting result pages (for testing)
    private DriverContext driverContext;
    private ThreadContext threadContext;
    private Driver clientDriver;
    private PlainActionFuture<Void> clientDriverFuture; // Future for client driver completion
    private BatchDetectionSinkOperator batchDetectionSink;
    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final TransportService transportService; // For transport-based remote sink
    private final Task task; // For transport-based remote sink
    private final DiscoveryNode serverNode; // Server node for transport connection
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion

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
     * @param clientToServerId exchange ID for client-to-server direction
     * @param serverToClientId exchange ID for server-to-client direction
     * @param sessionId session ID for the driver
     * @param clusterName cluster name
     * @param exchangeService  the exchange service
     * @param executor         executor for async operations
     * @param maxBufferSize    maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task             task for transport-based remote sink
     * @param serverNode       server node for transport connection
     * @param batchExchangeStatusListener listener that will be called when batch exchange status is received (success or failure)
     * @param driverContext    driver context needed for the client driver
     * @param threadContext    thread context needed for the client driver
     * @throws Exception if initialization fails
     */
    public BidirectionalBatchExchangeClient(
        String clientToServerId,
        String serverToClientId,
        String sessionId,
        String clusterName,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        DiscoveryNode serverNode,
        ActionListener<Void> batchExchangeStatusListener,
        DriverContext driverContext,
        ThreadContext threadContext
    ) throws Exception {
        this.clientToServerId = clientToServerId;
        this.serverToClientId = serverToClientId;
        this.sessionId = sessionId;
        this.clusterName = clusterName;
        this.exchangeService = exchangeService;
        this.executor = executor;
        this.maxBufferSize = maxBufferSize;
        this.transportService = transportService;
        this.task = task;
        this.serverNode = serverNode;
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.driverContext = driverContext;
        this.threadContext = threadContext;
        logger.info(
            "[CLIENT] Created BidirectionalBatchExchangeClient: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
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
        logger.info("[CLIENT] Initializing BidirectionalBatchExchangeClient");
        if (driverContext == null || threadContext == null) {
            throw new IllegalStateException("DriverContext and ThreadContext must be set via setDriverContext() before initialize()");
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
        batchDetectionSink = new BatchDetectionSinkOperator();

        // Get node name from transport service
        String nodeName = transportService.getLocalNode().getName();

        // Use sessionId for shortDescription and description
        String shortDescription = "batch-exchange-client";
        Supplier<String> description = () -> "bidirectional-batch-exchange-client-" + sessionId;

        // Create driver to drive the ExchangeSourceOperator
        clientDriver = new Driver(
            sessionId,
            shortDescription,
            clusterName,
            nodeName,
            System.currentTimeMillis(),
            System.nanoTime(),
            driverContext,
            description,
            serverToClientSource,
            List.of(), // No intermediate operators
            batchDetectionSink,
            TimeValue.timeValueMinutes(5),
            () -> {}
        );
        logger.info("[CLIENT] Created client driver");

        // Start the driver immediately - it will wait for pages if exchange isn't connected yet
        logger.info("[CLIENT] Starting client driver to actively request pages from exchange");
        clientDriverFuture = new PlainActionFuture<>();
        Driver.start(threadContext, executor, clientDriver, 1000, clientDriverFuture);
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
                    logger.info(
                        "[CLIENT] Received batch exchange status response for exchangeId={}, success={}",
                        serverToClientId,
                        response.isSuccess()
                    );
                    if (response.isSuccess()) {
                        logger.info("[CLIENT] Batch exchange completed successfully");
                        if (batchExchangeStatusListener != null) {
                            logger.info("[CLIENT] Calling batch exchange status listener onResponse (success)");
                            batchExchangeStatusListener.onResponse(null);
                            logger.debug("[CLIENT] Batch exchange status listener onResponse completed");
                        }
                    } else {
                        Exception failure = response.getFailure();
                        logger.warn(
                            "[CLIENT] Batch exchange status response indicates failure: {}",
                            failure != null ? failure.getMessage() : "unknown"
                        );
                        handleFailure("batch exchange status response", failure);
                    }
                }, failure -> {
                    logger.error("[CLIENT] Failed to receive batch exchange status response for exchangeId={}", serverToClientId, failure);
                    handleFailure("batch exchange status response (transport error)", failure);
                })
            );
            logger.info("[CLIENT] Batch exchange status request sent for exchangeId={}", serverToClientId);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send batch exchange status request for exchange " + serverToClientId, e);
        }
    }

    /**
     * Connect to the server's sink handler for server-to-client exchange.
     * This should be called after the server has created its sink handler.
     * Uses transport for error propagation.
     * Also sends batch exchange status request before page communication starts.
     */
    public void connectToServerSink() {
        try {
            // Use transport-based remote sink for error propagation
            Transport.Connection connection = transportService.getConnection(serverNode);
            RemoteSink serverRemoteSink = exchangeService.newRemoteSink(task, serverToClientId, transportService, connection);
            logger.debug("[CLIENT] Connected to server sink handler via transport for server-to-client exchange");
            // Register failure listener for server-to-client exchange failures
            serverToClientSourceHandler.addRemoteSink(serverRemoteSink, true, () -> {}, 1, ActionListener.wrap(nullValue -> {
                // Success - no action needed
            }, failure -> { handleFailure("server-to-client exchange", failure); }));

            // Send batch exchange status request before page communication starts
            sendBatchExchangeStatusRequest();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to server sink handler for exchange " + serverToClientId, e);
        }
    }

    /**
     * Wait for the client driver to finish.
     * This should be called after the source is finished to ensure the driver has completed processing.
     */
    public void waitForClientDriverToFinish(long timeout, TimeUnit unit) throws Exception {
        if (clientDriverFuture != null) {
            clientDriverFuture.actionGet(timeout, unit);
        }
    }

    /**
     * Set the batch done listener.
     * This listener will be called when a batch completes.
     */
    public void setBatchDoneListener(BatchDoneListener listener) {
        this.batchDoneListener = listener;
    }

    /**
     * This collector will be called for each result BatchPage received from the server.
     */
    public void setResultPageCollector(Consumer<BatchPage> collector) {
        this.resultPageCollector = collector;
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
            logger.error("[CLIENT] First failure received from {}: {}", source, failure.getMessage(), failure);
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

    /**
     * Sink operator that detects batch completion and collects result pages.
     */
    private class BatchDetectionSinkOperator extends SinkOperator {
        @Override
        public boolean needsInput() {
            boolean needs = serverToClientSource == null || serverToClientSource.isFinished() == false;
            logger.debug(
                "[CLIENT] BatchDetectionSinkOperator.needsInput() called: returning={}, sourceFinished={}",
                needs,
                serverToClientSource != null ? serverToClientSource.isFinished() : "null"
            );
            return needs;
        }

        @Override
        protected void doAddInput(Page page) {
            // SinkOperator.doAddInput requires Page parameter, but we know it's always BatchPage
            BatchPage batchPage = (BatchPage) page;
            try {
                // Only collect BatchPage if it has data (positionCount > 0)
                // Empty BatchPages (markers) are still used for batch completion but not passed to collector
                if (batchPage.getPositionCount() > 0) {
                    if (resultPageCollector != null) {
                        try {
                            // Collector receives the BatchPage - it should copy data if needed
                            resultPageCollector.accept(batchPage);
                        } catch (Exception e) {
                            logger.error("[CLIENT] Error in result page collector for BatchPage", e);
                        }
                    }
                }

                // Handle batch completion (even if BatchPage has no data)
                if (batchPage.isLastPageInBatch()) {
                    long batchId = batchPage.batchId();
                    if (batchDoneListener != null) {
                        try {
                            batchDoneListener.onBatchDone(batchId);
                        } catch (Exception e) {
                            logger.error("[CLIENT] Error in batch done listener for batchId=" + batchId, e);
                            failureRef.compareAndSet(null, e);
                        }
                    } else {
                        logger.warn("[CLIENT] Batch done listener is null for batchId={}", batchId);
                    }
                }
            } finally {
                // Release page after processing (collector should have copied data if needed)
                batchPage.releaseBlocks();
            }
        }

        @Override
        public void finish() {
            // No-op
        }

        @Override
        public boolean isFinished() {
            boolean finished = serverToClientSource != null && serverToClientSource.isFinished();
            logger.debug(
                "[CLIENT] BatchDetectionSinkOperator.isFinished() called: returning={}, sourceFinished={}",
                finished,
                serverToClientSource != null ? serverToClientSource.isFinished() : "null"
            );
            return finished;
        }

        @Override
        public IsBlockedResult isBlocked() {
            return Operator.NOT_BLOCKED;
        }

        @Override
        public void close() {
            // No-op - resources are managed by BidirectionalBatchExchangeClient
        }
    }

    @Override
    public void close() {
        logger.info("[CLIENT] Closing BidirectionalBatchExchangeClient");
        finish();
        if (clientDriver != null) {
            logger.debug("[CLIENT] Closing client driver");
            clientDriver.close();
        }
        if (serverToClientSource != null) {
            logger.debug("[CLIENT] Closing server-to-client source");
            serverToClientSource.close();
        }
        if (serverToClientSourceHandler != null) {
            logger.debug("[CLIENT] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }
        logger.info("[CLIENT] BidirectionalBatchExchangeClient closed");
    }
}
