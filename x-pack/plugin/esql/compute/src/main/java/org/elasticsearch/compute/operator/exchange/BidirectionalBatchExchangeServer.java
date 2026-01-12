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
import org.elasticsearch.cluster.node.DiscoveryNode;
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
    private ActionListener<BatchExchangeStatusResponse> batchExchangeStatusListener; // Listener to call when batch processing completes
    private final AtomicReference<Releasable> releasableRef = new AtomicReference<>(); // Releasable resources (shardContext, etc.) that
                                                                                       // should be closed when driver finishes or server
                                                                                       // closes
    private volatile boolean closing = false; // Flag to prevent recursive close if server is part of the releasable

    /**
     * Create a new BidirectionalBatchExchangeServer.
     * This is stage 1: creates the server and source handler.
     * Call {@link #startWithOperators(DriverContext, ThreadContext, List, String, Releasable)} to complete setup.
     *
     * @param sessionId session ID for the driver
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
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        DiscoveryNode clientNode
    ) throws Exception {
        super(sessionId, exchangeService, executor, maxBufferSize, transportService, task);
        this.clientNode = clientNode;
        logger.info(
            "[SERVER] Created BidirectionalBatchExchangeServer: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
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
     * @throws Exception if starting fails
     */
    public void startWithOperators(
        DriverContext driverContext,
        ThreadContext threadContext,
        List<Operator> intermediateOperators,
        String clusterName,
        Releasable releasable
    ) throws Exception {
        startBatchProcessing(driverContext, threadContext, intermediateOperators, clusterName, TimeValue.timeValueSeconds(1), releasable);
    }

    /**
     * Initialize the server exchanges.
     * Called automatically from the constructor.
     */
    private void initialize() {
        logger.info("[SERVER] Initializing BidirectionalBatchExchangeServer");
        // Create source handler for client-to-server direction
        clientToServerSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(clientToServerId, clientToServerSourceHandler);
        clientToServerSource = new ExchangeSourceOperator(clientToServerSourceHandler.createExchangeSource());
        logger.debug("[SERVER] Created client-to-server source handler: exchangeId={}", clientToServerId);

        // Create sink handler for server-to-client direction
        serverToClientSinkHandler = exchangeService.createSinkHandler(serverToClientId, maxBufferSize);
        serverToClientSink = serverToClientSinkHandler.createExchangeSink(() -> {});
        logger.debug("[SERVER] Created server-to-client sink handler: exchangeId={}", serverToClientId);

        // Register this server with ExchangeService so it can receive BatchExchangeStatusRequest messages
        // The handler is registered once in ExchangeService.registerTransportHandler() and routes to servers
        exchangeService.registerBatchExchangeServer(serverToClientId, this);
        logger.info("[SERVER] Registered with ExchangeService for exchangeId={}", serverToClientId);
        logger.info("[SERVER] BidirectionalBatchExchangeServer initialized successfully");
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
                "[SERVER][ERROR] Received BatchExchangeStatusRequest for wrong exchangeId={}, expected {}",
                exchangeId,
                serverToClientId
            );
            return;
        }

        // Check if server is already closing - if so, reply with failure immediately
        if (closing) {
            logger.error("[SERVER][ERROR] Received BatchExchangeStatusRequest but server is already closing for exchangeId={}", exchangeId);
            try {
                channel.sendResponse(new BatchExchangeStatusResponse(false, new IllegalStateException("Server is closing")));
            } catch (Exception e) {
                logger.debug("[SERVER] Failed to send failure response (server closing)", e);
            }
            return;
        }

        // Store the listener to send response when batch processing completes
        // This MUST be done before starting processing to ensure we can always reply on error
        batchExchangeStatusListener = new ChannelActionListener<>(channel);
        logger.info(
            "[SERVER] BatchExchangeStatusRequest received for exchangeId={}, stored listener (processing will start now)",
            exchangeId
        );

        // Start the driver now that client is ready and we have the response channel
        // If an error occurs during startup, ensure we reply
        try {
            onClientReady();
        } catch (Exception e) {
            // If starting the driver fails, reply immediately with failure
            logger.error(
                "[SERVER][ERROR] Failed to start driver after BatchExchangeStatusRequest for exchangeId={}: {}",
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
            logger.error("[SERVER][ERROR] onClientReady called but driver not prepared yet for exchangeId={}", serverToClientId);
            // Reply with failure since we can't start processing
            sendBatchExchangeStatusResponse(false, new IllegalStateException(errorMsg));
            return;
        }
        if (closing) {
            String errorMsg = "Server is closing when BatchExchangeStatusRequest received";
            logger.error("[SERVER][ERROR] Server is closing, cannot start driver for exchangeId={}", serverToClientId);
            // Reply with failure since we can't start processing
            sendBatchExchangeStatusResponse(false, new IllegalStateException(errorMsg));
            return;
        }
        logger.info("[SERVER] Client is ready, starting driver for exchangeId={}", serverToClientId);
        // driverFuture was already created in startBatchProcessing(), reuse it
        // The driver completion listener will handle both success and failure cases and reply
        Driver.start(threadContext, executor, batchDriver, 1000, createDriverCompletionListener());
        logger.info("[SERVER] Server driver started");
    }

    /**
     * Create an ActionListener for driver completion that handles both success and failure cases.
     */
    private ActionListener<Void> createDriverCompletionListener() {
        return ActionListener.wrap(ignored -> {
            logger.info("[SERVER] Driver completion listener onResponse called (success) for exchangeId={}", serverToClientId);
            driverFuture.onResponse(null);
            logger.info("[SERVER] Batch processing completed successfully for exchangeId={}", serverToClientId);
            sendBatchExchangeStatusResponse(true, null);
            // Close the server now that the driver has finished successfully
            close();
        }, failure -> {
            logger.info(
                "[SERVER] Driver completion listener onFailure called for exchangeId={}, failure={}",
                serverToClientId,
                failure != null ? failure.getMessage() : "unknown"
            );
            logger.info(
                "[SERVER] Batch processing completed with failure for exchangeId={}, failure={}",
                serverToClientId,
                failure != null ? failure.getMessage() : "unknown"
            );
            handleDriverFailure(failure);
            // Close the server now that the driver has finished with failure
            close();
        });
    }

    /**
     * Handle driver failure by propagating it to the exchange sink handler and sending failure response.
     */
    private void handleDriverFailure(Exception failure) {
        logger.error("[SERVER][ERROR] Server driver failed, propagating failure to exchange sink handler", failure);
        serverToClientSinkHandler.onFailure(failure);
        driverFuture.onFailure(failure);
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
            logger.info(
                "[SERVER] Sending batch exchange status {} response for exchangeId={}",
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
                    "[SERVER][ERROR] Failed to send batch exchange status response for exchangeId={}: {}",
                    serverToClientId,
                    e.getMessage()
                );
            }
        } else {
            logger.error(
                "[SERVER][ERROR] Cannot send batch exchange status response: listener is null for exchangeId={}",
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
        logger.info("[SERVER] Starting batch processing: sessionId={}, operators={}", sessionId, intermediateOperators.size());

        long startTime = System.currentTimeMillis();
        long startNanos = System.nanoTime();

        // Get node name from transport service
        String nodeName = transportService.getLocalNode().getName();

        // Use sessionId for shortDescription and description
        String shortDescription = "batch-exchange";
        Supplier<String> description = () -> "bidirectional-batch-exchange-server-" + sessionId;

        // Connect to the client's sink handler for client-to-server exchange
        // This should be called after the client has created its sink handler
        logger.info(
            "[SERVER] Connecting to client sink handler via transport for client-to-server exchange, exchangeId={}",
            clientToServerId
        );
        connectRemoteSink(
            clientNode,
            clientToServerId,
            clientToServerSourceHandler,
            ActionListener.wrap(
                nullValue -> logger.debug("[SERVER] Client-to-server exchange sink connection completed successfully"),
                failure -> logger.error("[SERVER][ERROR] Client-to-server exchange sink connection failed", failure)
            ),
            "client sink handler"
        );
        // Create sink operator that writes to server-to-client exchange
        serverToClientSinkOperator = new ExchangeSinkOperator(serverToClientSink);
        ExchangeSinkOperator baseSinkOperator = serverToClientSinkOperator;

        // Wrap sink to convert Pages to BatchPages before sending to client
        // The driver will be set on the wrapper after BatchDriver construction
        SinkOperator wrappedSink = BatchDriver.wrapSink(baseSinkOperator);

        // Store the releasable - server.close() will close it after driver finishes
        // Driver does NOT close the releasable - everything is handled in server.close()
        this.releasableRef.set(releasable);
        logger.info(
            "[SERVER] Stored releasable in releasableRef for cleanup: releasable={}",
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
                logger.debug("[SERVER] Driver finished, releasable will be closed by server.close()");
            }
        );
        logger.info("[SERVER] BatchDriver created");

        // Set up batch done callback listener
        // Note: The batch marker (last page with isLastPageInBatch=true) is now sent by
        // PageToBatchPageOperator.flushBatch() which is called by BatchDriver.completeBatch()
        // before this callback is invoked. This callback is just for logging/monitoring.
        logger.info("[SERVER] Registering batch done callback listener");
        ActionListener<Long> batchDoneListener = new ActionListener<Long>() {
            @Override
            public void onResponse(Long batchId) {}

            @Override
            public void onFailure(Exception e) {
                logger.error("[SERVER][ERROR] Batch done callback onFailure() invoked", e);
                // Propagate failure to exchange
                if (serverToClientSinkHandler != null) {
                    serverToClientSinkHandler.onFailure(e);
                }
            }
        };
        batchDriver.onBatchDone().addListener(batchDoneListener);
        logger.info("[SERVER] Batch done callback listener registered successfully");

        // Store thread context for later driver startup
        this.threadContext = threadContext;

        // Handler was already registered in initialize(), no need to register again
        logger.info("[SERVER] Driver prepared, will start when BatchExchangeStatusRequest is received for exchangeId={}", serverToClientId);

        // Mark driver as prepared (but not started yet)
        driverPrepared = true;

        // Create future that will be completed when driver finishes
        // This will be set when startDriver() is called
        driverFuture = new PlainActionFuture<>();
    }

    /**
     * Get the BatchDriver instance.
     * Can be used to start the driver or check its status.
     */
    public BatchDriver getBatchDriver() {
        return batchDriver;
    }

    @Override
    public void close() {
        // Prevent recursive close if server is part of a releasable that includes itself
        if (closing) {
            logger.debug("[SERVER] Already closing, skipping recursive close");
            return;
        }
        closing = true;

        logger.info("[SERVER] Closing BidirectionalBatchExchangeServer");

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
                logger.info("[SERVER] Closing releasable resources (shardContext, localBreaker, etc.)");
                releasable.close();
                logger.info("[SERVER] Releasable resources closed successfully");
            } catch (Exception e) {
                logger.warn("[SERVER] Exception closing releasable", e);
            }
        } else {
            logger.warn("[SERVER] No releasable to close (releasableRef was null)");
        }

        // Don't need to close batchDriver - when driver finishes, it already closes its operators
        // and the releasable passed to it. The driver itself doesn't need explicit closing.
        if (serverToClientSink != null && serverToClientSink.isFinished() == false) {
            logger.debug("[SERVER] Finishing server-to-client sink");
            serverToClientSink.finish();
        }
        if (clientToServerSource != null) {
            logger.debug("[SERVER] Closing client-to-server source");
            clientToServerSource.close();
        }
        if (clientToServerSourceHandler != null) {
            logger.debug("[SERVER] Removing client-to-server source handler");
            exchangeService.removeExchangeSourceHandler(clientToServerId);
        }
        if (serverToClientSinkHandler != null) {
            logger.debug("[SERVER] Finishing server-to-client sink handler");
            exchangeService.finishSinkHandler(serverToClientId, null);
        }
        // Unregister this server from ExchangeService
        exchangeService.unregisterBatchExchangeServer(serverToClientId);
        logger.info("[SERVER] BidirectionalBatchExchangeServer closed");
    }
}
