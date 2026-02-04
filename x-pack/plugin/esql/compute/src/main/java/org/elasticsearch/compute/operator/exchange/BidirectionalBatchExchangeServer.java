/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Server-side handler for bidirectional batch exchange.
 * <p>
 * The server:
 * <ul>
 *   <li>Receives batches from the client via clientToServer exchange (using ExchangeSource)</li>
 *   <li>Sends results to the client via serverToClient exchange (using ExchangeSink)</li>
 *   <li>Uses BatchDriver to process batches</li>
 *   <li>Sends empty marker page when batch completes (via onBatchDone callback)</li>
 * </ul>
 * <p>
 * Only one batch can be active at a time. BatchDriver will throw an exception if multiple batches are sent concurrently.
 */
public final class BidirectionalBatchExchangeServer extends BidirectionalBatchExchangeBase {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(BidirectionalBatchExchangeServer.class);

    /**
     * Timeout for waiting for BatchExchangeStatusRequest from the client.
     * If the client doesn't send the request within this time, the server will close and cleanup.
     */
    private static final long CLIENT_READY_TIMEOUT_SECONDS = 30;

    private ExchangeSourceHandler clientToServerSourceHandler;
    private ExchangeSourceOperator clientToServerSource;
    private ExchangeSinkHandler serverToClientSinkHandler;
    private ExchangeSink serverToClientSink;
    private ExchangeSinkOperator serverToClientSinkOperator;
    private BatchDriver batchDriver;
    private final DiscoveryNode clientNode; // Client node for transport connection
    private PlainActionFuture<Void> driverFuture; // Future for driver completion
    private ThreadContext threadContext; // Thread context for starting driver
    private boolean driverPrepared = false; // Whether driver has been prepared but not started
    private boolean driverStarted = false; // Whether driver has been started (client sent BatchExchangeStatusRequest)
    private ScheduledFuture<?> clientReadyTimeoutFuture; // Timeout for client to send BatchExchangeStatusRequest
    private ActionListener<BatchExchangeStatusResponse> batchExchangeStatusListener; // Listener to call when batch processing completes
    private final AtomicReference<Releasable> releasableRef = new AtomicReference<>(); // Releasable resources (shardContext, etc.) that
                                                                                       // should be closed when driver finishes or server
                                                                                       // closes
    private volatile boolean closing = false; // Flag to prevent recursive close if server is part of the releasable
    private final SubscribableListener<Void> remoteSinkReady = new SubscribableListener<>(); // Signals when remote sink connection is ready

    /**
     * Create a new BidirectionalBatchExchangeServer with explicit exchange IDs.
     * This is stage 1: creates the server and source handler.
     * Call {@link #startWithOperators(DriverContext, ThreadContext, List, String, Releasable, ActionListener)} to complete setup.
     *
     * @param sessionId session ID for the driver (used for logging)
     * @param clientToServerId explicit client-to-server exchange ID (per-server unique)
     * @param serverToClientId explicit server-to-client exchange ID (shared across servers)
     * @param exchangeService the exchange service
     * @param executor executor for async operations
     * @param maxBufferSize maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task task for transport-based remote sink
     * @param clientNode client node for transport connection
     * @throws Exception if initialization fails
     */
    public BidirectionalBatchExchangeServer(
        String sessionId,
        String clientToServerId,
        String serverToClientId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        DiscoveryNode clientNode,
        Settings settings
    ) throws Exception {
        super(sessionId, clientToServerId, serverToClientId, exchangeService, executor, maxBufferSize, transportService, task, settings);
        this.clientNode = clientNode;
        logger.debug(
            "[LookupJoinServer] Created BidirectionalBatchExchangeServer: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
            clientToServerId,
            serverToClientId,
            maxBufferSize
        );
        initialize();
    }

    /**
     * Get the source operator factory for use in planning.
     * This can be called after construction to get the factory before calling startWithOperators.
     *
     * @return the source operator factory
     */
    public ExchangeSourceOperator.ExchangeSourceOperatorFactory getSourceOperatorFactory() {
        if (clientToServerSourceHandler == null) {
            throw new IllegalStateException("Server not initialized");
        }
        return new ExchangeSourceOperator.ExchangeSourceOperatorFactory(clientToServerSourceHandler::createExchangeSource);
    }

    /**
     * Stage 2: Start batch processing with the intermediate operators.
     * This must be called after planning is complete.
     *
     * @param driverContext driver context
     * @param threadContext thread context for starting the driver
     * @param intermediateOperators intermediate operators to execute
     * @param clusterName cluster name
     * @param releasable releasable resources
     * @param readyListener listener called when the server is ready to receive pages (remote sink connected)
     * @throws Exception if starting fails
     */
    public void startWithOperators(
        DriverContext driverContext,
        ThreadContext threadContext,
        List<Operator> intermediateOperators,
        String clusterName,
        Releasable releasable,
        ActionListener<Void> readyListener
    ) throws Exception {
        startBatchProcessing(driverContext, threadContext, intermediateOperators, clusterName, TimeValue.timeValueSeconds(1), releasable);
        remoteSinkReady.addListener(readyListener);
    }

    /**
     * Initialize the server exchanges.
     * Called automatically from the constructor.
     */
    private void initialize() {
        logger.debug("[LookupJoinServer] Initializing BidirectionalBatchExchangeServer");
        // Create source handler for client-to-server direction
        clientToServerSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(clientToServerId, clientToServerSourceHandler);
        clientToServerSource = new ExchangeSourceOperator(clientToServerSourceHandler.createExchangeSource());
        logger.debug("[LookupJoinServer] Created client-to-server source handler: exchangeId={}", clientToServerId);

        // Create sink handler for server-to-client direction
        serverToClientSinkHandler = exchangeService.createSinkHandler(serverToClientId, maxBufferSize);
        serverToClientSink = serverToClientSinkHandler.createExchangeSink(() -> {});
        logger.debug("[LookupJoinServer] Created server-to-client sink handler: exchangeId={}", serverToClientId);

        // Register this server with ExchangeService so it can receive BatchExchangeStatusRequest messages
        // The handler is registered once in ExchangeService.registerTransportHandler() and routes to servers
        exchangeService.registerBatchExchangeServer(serverToClientId, this);
        logger.debug("[LookupJoinServer] Registered with ExchangeService for exchangeId={}", serverToClientId);
        logger.debug("[LookupJoinServer] BidirectionalBatchExchangeServer initialized successfully");
    }

    /**
     * Handle BatchExchangeStatusRequest from the client.
     * Called by ExchangeService's singleton handler which routes requests to the appropriate server.
     * <p>
     * The server stores the response channel BEFORE starting processing, ensuring it can always reply
     * if an error occurs. Processing only starts after this request is received.
     */
    public void handleBatchExchangeStatusRequest(BatchExchangeStatusRequest request, TransportChannel channel, Task task) {
        final String exchangeId = request.exchangeId();

        // Verify the exchange ID matches (should always be true since ExchangeService routes correctly)
        if (exchangeId.equals(serverToClientId) == false) {
            logger.error(
                "[LookupJoinServer] Received BatchExchangeStatusRequest for wrong exchangeId={}, expected {}",
                exchangeId,
                serverToClientId
            );
            return;
        }

        // Check if server is already closing - if so, reply with failure immediately
        if (closing) {
            logger.error(
                "[LookupJoinServer] Received BatchExchangeStatusRequest but server is already closing for exchangeId={}",
                exchangeId
            );
            try {
                channel.sendResponse(new BatchExchangeStatusResponse(false, new IllegalStateException("Server is closing")));
            } catch (Exception e) {
                logger.debug("[LookupJoinServer] Failed to send failure response (server closing)", e);
            }
            return;
        }

        // Store the listener to send response when batch processing completes
        // This MUST be done before starting processing to ensure we can always reply on error
        batchExchangeStatusListener = new ChannelActionListener<>(channel);
        logger.debug(
            "[LookupJoinServer] BatchExchangeStatusRequest received for exchangeId={}, stored listener (processing will start now)",
            exchangeId
        );

        // Start the driver now that client is ready and we have the response channel
        // If an error occurs during startup, ensure we reply
        try {
            onClientReady();
        } catch (Exception e) {
            // If starting the driver fails, reply immediately with failure
            logger.error(
                "[LookupJoinServer] Failed to start driver after BatchExchangeStatusRequest for exchangeId={}: {}",
                exchangeId,
                e.getMessage()
            );
            sendBatchExchangeStatusResponse(false, e);
        }
    }

    /**
     * Called when BatchExchangeStatusRequest is received from the client.
     * This indicates the client is ready, so we can start the driver.
     * <p>
     * This method ensures that if an error occurs during driver startup, the error is properly
     * handled and a response is sent to the client via the stored batchExchangeStatusListener.
     */
    private void onClientReady() {
        if (driverPrepared == false) {
            String errorMsg = "Driver not prepared when BatchExchangeStatusRequest received";
            logger.error("[LookupJoinServer] onClientReady called but driver not prepared yet for exchangeId={}", serverToClientId);
            // Reply with failure since we can't start processing
            sendBatchExchangeStatusResponse(false, new IllegalStateException(errorMsg));
            return;
        }
        if (closing) {
            String errorMsg = "Server is closing when BatchExchangeStatusRequest received";
            logger.error("[LookupJoinServer] Server is closing, cannot start driver for exchangeId={}", serverToClientId);
            // Reply with failure since we can't start processing
            sendBatchExchangeStatusResponse(false, new IllegalStateException(errorMsg));
            return;
        }

        // Cancel the timeout since client is ready
        if (clientReadyTimeoutFuture != null) {
            FutureUtils.cancel(clientReadyTimeoutFuture);
            clientReadyTimeoutFuture = null;
        }

        // Mark driver as started before actually starting it
        driverStarted = true;

        logger.debug("[LookupJoinServer] Client is ready, starting driver for exchangeId={}", serverToClientId);
        // driverFuture was already created in startBatchProcessing(), reuse it
        // The driver completion listener will handle both success and failure cases and reply
        Driver.start(threadContext, executor, batchDriver, 1000, createDriverCompletionListener());
        logger.debug("[LookupJoinServer] Server driver started");
    }

    /**
     * Create an ActionListener for driver completion that handles both success and failure cases.
     * Important: We close server resources BEFORE sending the response to the client.
     * This ensures all resources (like DirectoryReader) are released before the client
     * considers the operation complete and proceeds with its own cleanup.
     */
    private ActionListener<Void> createDriverCompletionListener() {
        return ActionListener.wrap(ignored -> {
            logger.debug("[LookupJoinServer] Driver completion listener onResponse called (success) for exchangeId={}", serverToClientId);
            driverFuture.onResponse(null);
            logger.debug("[LookupJoinServer] Batch processing completed successfully for exchangeId={}", serverToClientId);
            // Close server resources BEFORE sending response to client
            // This ensures DirectoryReader etc. are closed before client proceeds with cleanup
            Exception closeException = null;
            try {
                close();
            } catch (Exception e) {
                logger.error("[LookupJoinServer] Exception during close after successful driver completion", e);
                closeException = e;
            }
            // Send response - success if close() succeeded, failure if close() threw
            if (closeException != null) {
                handleDriverFailureAfterClose(closeException);
            } else {
                sendBatchExchangeStatusResponse(true, null);
            }
        }, failure -> {
            logger.debug(
                "[LookupJoinServer] Driver completion listener onFailure called for exchangeId={}, failure={}",
                serverToClientId,
                failure != null ? failure.getMessage() : "unknown"
            );
            logger.debug(
                "[LookupJoinServer] Batch processing completed with failure for exchangeId={}, failure={}",
                serverToClientId,
                failure != null ? failure.getMessage() : "unknown"
            );
            // Complete the future first so close() won't throw
            driverFuture.onFailure(failure);
            // Close server resources BEFORE sending response
            try {
                close();
            } catch (Exception e) {
                logger.error("[LookupJoinServer] Exception during close after driver failure", e);
                // Continue to send the original failure, not the close exception
            }
            // Always handle failure and send response
            handleDriverFailureAfterClose(failure);
        });
    }

    /**
     * Handle driver failure after close() has already been called.
     * This propagates the failure to the exchange sink handler and sends the failure response.
     * Note: driverFuture.onFailure() and close() have already been called before this method.
     */
    private void handleDriverFailureAfterClose(Exception failure) {
        logger.error("[LookupJoinServer] Server driver failed, propagating failure to exchange sink handler", failure);
        try {
            serverToClientSinkHandler.onFailure(failure);
        } catch (Exception e) {
            logger.error("[LookupJoinServer] Exception propagating failure to sink handler", e);
        }
        // Always send response, even if onFailure() threw
        sendBatchExchangeStatusResponse(false, failure);
    }

    /**
     * Send batch exchange status response to the client.
     * <p>
     * This method ensures we always reply to the client, even if an error occurred.
     * The listener is stored when BatchExchangeStatusRequest is received, before processing starts.
     */
    private void sendBatchExchangeStatusResponse(boolean success, Exception failure) {
        ActionListener<BatchExchangeStatusResponse> listener = batchExchangeStatusListener;
        if (listener != null) {
            logger.debug(
                "[LookupJoinServer] Sending batch exchange status {} response for exchangeId={}",
                success ? "success" : "failure",
                serverToClientId
            );
            try {
                listener.onResponse(new BatchExchangeStatusResponse(success, failure));
                // Clear the listener after sending response to prevent duplicate replies
                batchExchangeStatusListener = null;
            } catch (Exception e) {
                // If sending response fails (e.g., channel closed, node closed), log as error but don't propagate
                // The client waits for the response, so this indicates an unexpected failure
                logger.error(
                    "[LookupJoinServer] Failed to send batch exchange status response for exchangeId={}: {}",
                    serverToClientId,
                    e.getMessage()
                );
            }
        } else {
            logger.error(
                "[LookupJoinServer] Cannot send batch exchange status response: listener is null for exchangeId={}",
                serverToClientId
            );
        }
    }

    /**
     * Get the driver future that completes when batch processing finishes.
     * @return PlainActionFuture that completes when the driver finishes
     */
    public PlainActionFuture<Void> getDriverFuture() {
        return driverFuture;
    }

    /**
     * Start batch processing with the given operators.
     * Creates a BatchDriver that processes batches and sends results back to the client.
     * Also connects to the client's sink handler for client-to-server exchange.
     * The driver will be started when the client is ready (BatchExchangeStatusRequest received).
     * Called automatically from the constructor.
     *
     * @param driverContext driver context
     * @param threadContext thread context for starting the driver
     * @param intermediateOperators intermediate operators to execute
     * @param clusterName cluster name
     */
    private void startBatchProcessing(
        DriverContext driverContext,
        ThreadContext threadContext,
        List<Operator> intermediateOperators,
        String clusterName
    ) {
        startBatchProcessing(driverContext, threadContext, intermediateOperators, clusterName, TimeValue.timeValueSeconds(1), () -> {});
    }

    /**
     * Start batch processing with the given operators and full configuration.
     * Creates a BatchDriver that processes batches and sends results back to the client.
     * Also connects to the client's sink handler for client-to-server exchange.
     * The driver will be started when the client is ready (BatchExchangeStatusRequest received).
     * Called automatically from the constructor.
     *
     * @param driverContext driver context
     * @param threadContext thread context for starting the driver
     * @param intermediateOperators intermediate operators to execute
     * @param clusterName cluster name
     * @param statusInterval status reporting interval
     * @param releasable releasable resource
     */
    private void startBatchProcessing(
        DriverContext driverContext,
        ThreadContext threadContext,
        List<Operator> intermediateOperators,
        String clusterName,
        TimeValue statusInterval,
        Releasable releasable
    ) {
        logger.debug("[LookupJoinServer] Starting batch processing: sessionId={}, operators={}", sessionId, intermediateOperators.size());

        long startTime = System.currentTimeMillis();
        long startNanos = System.nanoTime();

        // Get node name from transport service
        String nodeName = transportService.getLocalNode().getName();

        // Use sessionId for shortDescription and description
        String shortDescription = "batch-exchange";
        Supplier<String> description = () -> "bidirectional-batch-exchange-server-" + sessionId;

        // Connect to the client's sink handler for client-to-server exchange
        // This should be called after the client has created its sink handler
        logger.debug(
            "[LookupJoinServer] Connecting to client sink handler via transport for client-to-server exchange, exchangeId={}",
            clientToServerId
        );
        connectRemoteSink(clientNode, clientToServerId, clientToServerSourceHandler, ActionListener.wrap(nullValue -> {
            logger.debug("[LookupJoinServer] Client-to-server exchange sink connection completed successfully");
        }, failure -> { logger.error("[LookupJoinServer] Client-to-server exchange sink connection failed", failure); }),
            "client sink handler"
        );
        // Signal that the remote sink has been added to the source handler.
        // At this point, outstandingSinks >= 1, so the buffer won't report isFinished() = true
        // until the actual fetch completes. This prevents the race where the driver starts
        // before the fetch is registered.
        remoteSinkReady.onResponse(null);
        logger.debug("[LookupJoinServer] Remote sink added, signaling ready");
        // Create sink operator that writes to server-to-client exchange
        serverToClientSinkOperator = new ExchangeSinkOperator(serverToClientSink);
        ExchangeSinkOperator baseSinkOperator = serverToClientSinkOperator;

        // Wrap sink to convert Pages to BatchPages before sending to client
        // The driver will be set on the wrapper after BatchDriver construction
        SinkOperator wrappedSink = BatchDriver.wrapSink(baseSinkOperator);

        // Store the releasable - server.close() will close it after driver finishes
        // Driver does NOT close the releasable - everything is handled in server.close()
        this.releasableRef.set(releasable);
        logger.debug(
            "[LookupJoinServer] Stored releasable in releasableRef for cleanup: releasable={}",
            releasable != null ? releasable.getClass().getSimpleName() : "null"
        );

        // Create BatchDriver with wrapped sink that converts Pages to BatchPages
        // BatchDriver will set itself on the PageToBatchPageOperator wrapper
        // Pass a no-op releasable to the driver - server.close() will handle all cleanup
        batchDriver = new BatchDriver(
            this.sessionId,
            shortDescription,
            clusterName,
            nodeName,
            startTime,
            startNanos,
            driverContext,
            description,
            clientToServerSource,
            intermediateOperators,
            wrappedSink,
            statusInterval,
            () -> {
                // No-op - server.close() will handle all cleanup
                logger.debug("[LookupJoinServer] Driver finished, releasable will be closed by server.close()");
            }
        );
        logger.debug("[LookupJoinServer] BatchDriver created");

        // Set up batch done callback listener
        // Note: The batch marker (last page with isLastPageInBatch=true) is now sent by
        // PageToBatchPageOperator.flushBatch() which is called by BatchDriver.completeBatch()
        // before this callback is invoked. This callback is just for logging/monitoring.
        logger.debug("[LookupJoinServer] Registering batch done callback listener");
        ActionListener<Long> batchDoneListener = new ActionListener<Long>() {
            @Override
            public void onResponse(Long batchId) {}

            @Override
            public void onFailure(Exception e) {
                logger.error("[LookupJoinServer] Batch done callback onFailure() invoked", e);
                // Propagate failure to exchange
                if (serverToClientSinkHandler != null) {
                    serverToClientSinkHandler.onFailure(e);
                }
            }
        };
        batchDriver.onBatchDone().addListener(batchDoneListener);
        logger.debug("[LookupJoinServer] Batch done callback listener registered successfully");

        // Store thread context for later driver startup
        this.threadContext = threadContext;

        // Handler was already registered in initialize(), no need to register again
        logger.debug(
            "[LookupJoinServer] Driver prepared, will start when BatchExchangeStatusRequest is received for exchangeId={}",
            serverToClientId
        );

        // Mark driver as prepared (but not started yet)
        driverPrepared = true;

        // Create future that will be completed when driver finishes
        // This will be set when startDriver() is called
        driverFuture = new PlainActionFuture<>();

        // Schedule a timeout - if client doesn't send BatchExchangeStatusRequest within the timeout,
        // we need to close the server and cleanup resources (like DirectoryReader)
        clientReadyTimeoutFuture = transportService.getThreadPool().scheduler().schedule(() -> {
            if (driverStarted == false && closing == false) {
                logger.warn(
                    "[LookupJoinServer] Timeout waiting for BatchExchangeStatusRequest from client after {}s, "
                        + "closing server for exchangeId={}",
                    CLIENT_READY_TIMEOUT_SECONDS,
                    serverToClientId
                );
                // Complete the driverFuture with failure so close() won't throw
                if (driverFuture != null && driverFuture.isDone() == false) {
                    driverFuture.onFailure(
                        new IllegalStateException(
                            "Timeout waiting for client BatchExchangeStatusRequest after " + CLIENT_READY_TIMEOUT_SECONDS + "s"
                        )
                    );
                }
                // Close server to cleanup resources
                try {
                    close();
                } catch (Exception e) {
                    logger.error("[LookupJoinServer] Exception during timeout cleanup for exchangeId={}: {}", serverToClientId, e);
                }
            }
        }, CLIENT_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        logger.debug(
            "[LookupJoinServer] Scheduled client ready timeout: {}s for exchangeId={}",
            CLIENT_READY_TIMEOUT_SECONDS,
            serverToClientId
        );
    }

    /**
     * Get the BatchDriver instance.
     * Can be used to start the driver or check its status.
     */
    public BatchDriver getBatchDriver() {
        return batchDriver;
    }

    /**
     * Force close the server, completing the driver future if needed.
     * This is used during ExchangeService shutdown to cleanup servers that haven't started.
     */
    public void forceClose() {
        // Complete the driverFuture if it exists and hasn't completed yet
        // This allows close() to proceed without throwing IllegalStateException
        if (driverFuture != null && driverFuture.isDone() == false) {
            logger.debug("[LookupJoinServer] Force closing - completing driver future");
            driverFuture.onFailure(new IllegalStateException("Server force closed during ExchangeService shutdown"));
        }
        close();
    }

    @Override
    public void close() {
        // Prevent recursive close if server is part of a releasable that includes itself
        if (closing) {
            logger.debug("[LookupJoinServer] Already closing, skipping recursive close");
            return;
        }
        closing = true;

        logger.debug("[LookupJoinServer] Closing BidirectionalBatchExchangeServer");

        // Cancel the client ready timeout if it's still pending
        if (clientReadyTimeoutFuture != null) {
            FutureUtils.cancel(clientReadyTimeoutFuture);
            clientReadyTimeoutFuture = null;
        }

        // When close() is called from driver completion listener, driver is already finished
        // If driver was never prepared (setup failed before startBatchProcessing), driverFuture is null - that's ok
        // But if driver is still running, that's unexpected - throw exception
        if (driverFuture != null && driverFuture.isDone() == false) {
            throw new IllegalStateException(
                "Cannot close BidirectionalBatchExchangeServer: driver is still running. "
                    + "close() should only be called after driver finishes (from driver completion listener)."
            );
        }

        // Close all releasable resources (shardContext with DirectoryReader, localBreaker, etc.)
        // This is the single point of cleanup - driver does not close anything
        // When close() is called from driver completion listener, driver has already finished
        // and closed its operators and the releasable passed to it, but we still need to close
        // the releasable we stored (shardContext and localBreaker)
        Releasable releasable = releasableRef.getAndSet(null);
        if (releasable != null) {
            try {
                logger.debug("[LookupJoinServer] Closing releasable resources (shardContext, localBreaker, etc.)");
                releasable.close();
                logger.debug("[LookupJoinServer] Releasable resources closed successfully");
            } catch (Exception e) {
                logger.warn("[LookupJoinServer] Exception closing releasable", e);
            }
        } else {
            logger.warn("[LookupJoinServer] No releasable to close (releasableRef was null)");
        }

        // Don't need to close batchDriver - when driver finishes, it already closes its operators
        // and the releasable passed to it. The driver itself doesn't need explicit closing.
        if (serverToClientSink != null && serverToClientSink.isFinished() == false) {
            logger.debug("[LookupJoinServer] Finishing server-to-client sink");
            serverToClientSink.finish();
        }
        if (clientToServerSource != null) {
            logger.debug("[LookupJoinServer] Closing client-to-server source");
            clientToServerSource.close();
        }
        if (clientToServerSourceHandler != null) {
            // Drain any pages remaining in the source handler's buffer before removing.
            // When server fails, pages that were transferred from client but not yet consumed
            // would leak if we don't drain them here.
            logger.debug("[LookupJoinServer] Draining client-to-server source handler buffer and removing handler");
            clientToServerSourceHandler.finishEarly(true, ActionListener.noop());
            exchangeService.removeExchangeSourceHandler(clientToServerId);
        }
        if (serverToClientSinkHandler != null) {
            // Don't call finishSinkHandler() immediately - the client may still be reading pages.
            // Wait for the sink handler to be actually finished (all pages consumed) before cleaning up.
            serverToClientSinkHandler.addCompletionListener(ActionListener.wrap(v -> {
                logger.debug("[LookupJoinServer] Sink handler completed, finishing it");
                exchangeService.finishSinkHandler(serverToClientId, null);
            }, e -> {
                logger.debug("[LookupJoinServer] Sink handler completed with error, finishing it: {}", e.getMessage());
                exchangeService.finishSinkHandler(serverToClientId, e);
            }));
        }
        // Unregister this server from ExchangeService
        exchangeService.unregisterBatchExchangeServer(serverToClientId);
        logger.debug("[LookupJoinServer] BidirectionalBatchExchangeServer closed");
    }
}
