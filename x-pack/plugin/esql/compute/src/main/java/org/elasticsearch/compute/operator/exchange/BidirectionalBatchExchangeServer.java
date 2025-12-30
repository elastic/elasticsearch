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
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.Executor;
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
public final class BidirectionalBatchExchangeServer implements Releasable {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(BidirectionalBatchExchangeServer.class);

    private final String clientToServerId;
    private final String serverToClientId;
    private final String sessionId;
    private final ExchangeService exchangeService;
    private final Executor executor;
    private final int maxBufferSize;

    private ExchangeSourceHandler clientToServerSourceHandler;
    private ExchangeSourceOperator clientToServerSource;
    private ExchangeSinkHandler serverToClientSinkHandler;
    private ExchangeSink serverToClientSink;
    private ExchangeSinkOperator serverToClientSinkOperator;
    private BatchDriver batchDriver;
    private final TransportService transportService; // For transport-based remote sink
    private final Task task; // For transport-based remote sink
    private final DiscoveryNode clientNode; // Client node for transport connection
    private PlainActionFuture<Void> driverFuture; // Future for driver completion
    private ThreadContext threadContext; // Thread context for starting driver
    private boolean driverPrepared = false; // Whether driver has been prepared but not started
    private ActionListener<BatchExchangeStatusResponse> batchExchangeStatusListener; // Listener to call when batch processing completes

    /**
     * Create a new BidirectionalBatchExchangeServer.
     *
     * @param clientToServerId exchange ID for client-to-server direction
     * @param serverToClientId exchange ID for server-to-client direction
     * @param sessionId session ID for the driver
     * @param exchangeService the exchange service
     * @param executor executor for async operations
     * @param maxBufferSize maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task task for transport-based remote sink
     * @param clientNode client node for transport connection
     * @param driverContext driver context
     * @param threadContext thread context for starting the driver
     * @param intermediateOperators intermediate operators to execute
     * @param clusterName cluster name
     * @throws Exception if initialization fails
     */
    public BidirectionalBatchExchangeServer(
        String clientToServerId,
        String serverToClientId,
        String sessionId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        DiscoveryNode clientNode,
        DriverContext driverContext,
        ThreadContext threadContext,
        List<Operator> intermediateOperators,
        String clusterName
    ) throws Exception {
        this.clientToServerId = clientToServerId;
        this.serverToClientId = serverToClientId;
        this.sessionId = sessionId;
        this.exchangeService = exchangeService;
        this.executor = executor;
        this.maxBufferSize = maxBufferSize;
        this.transportService = transportService;
        this.task = task;
        this.clientNode = clientNode;
        logger.info(
            "[SERVER] Created BidirectionalBatchExchangeServer: clientToServerId={}, serverToClientId={}, maxBufferSize={}",
            clientToServerId,
            serverToClientId,
            maxBufferSize
        );
        initialize();
        startBatchProcessing(driverContext, threadContext, intermediateOperators, clusterName);
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

        // Register transport handler for BatchExchangeStatusRequest early, before signaling initialization complete
        // This ensures the handler is ready when the client sends the request
        registerClientReadyHandler(transportService);
        logger.info("[SERVER] Registered client ready handler for exchangeId={}", serverToClientId);
        logger.info("[SERVER] BidirectionalBatchExchangeServer initialized successfully");
    }

    /**
     * Register a transport handler for BatchExchangeStatusRequest.
     * When the request is received, it stores the listener and starts the driver.
     */
    public void registerClientReadyHandler(TransportService transportService) {
        // Register handler for BatchExchangeStatusRequest
        transportService.registerRequestHandler(
            ExchangeService.BATCH_EXCHANGE_STATUS_ACTION_NAME,
            executor,
            BatchExchangeStatusRequest::new,
            new TransportRequestHandler<BatchExchangeStatusRequest>() {
                @Override
                public void messageReceived(BatchExchangeStatusRequest request, TransportChannel channel, Task task) throws Exception {
                    final String exchangeId = request.exchangeId();

                    // Only handle requests for our exchange ID
                    if (exchangeId.equals(serverToClientId) == false) {
                        logger.warn(
                            "[SERVER] Received BatchExchangeStatusRequest for wrong exchangeId={}, expected {}",
                            exchangeId,
                            serverToClientId
                        );
                        return;
                    }

                    // Store the listener to send response when batch processing completes
                    batchExchangeStatusListener = new ChannelActionListener<>(channel);
                    logger.info("[SERVER] BatchExchangeStatusRequest received for exchangeId={}, stored listener", exchangeId);

                    // Start the driver now that client is ready
                    onClientReady();
                }
            }
        );
        logger.info("[SERVER] Registered client ready handler for exchangeId={}", serverToClientId);
    }

    /**
     * Called when BatchExchangeStatusRequest is received from the client.
     * This indicates the client is ready, so we can start the driver.
     */
    private void onClientReady() {
        if (driverPrepared == false) {
            logger.warn("[SERVER] onClientReady called but driver not prepared yet");
            return;
        }
        logger.info("[SERVER] Client is ready, starting driver for exchangeId={}", serverToClientId);
        // driverFuture was already created in startBatchProcessing(), reuse it
        Driver.start(threadContext, executor, batchDriver, 1000, createDriverCompletionListener());
        logger.info("[SERVER] Server driver started");
    }

    /**
     * Create an ActionListener for driver completion that handles both success and failure cases.
     */
    private ActionListener<Void> createDriverCompletionListener() {
        return ActionListener.wrap(ignored -> {
            driverFuture.onResponse(null);
            logger.info("[SERVER] Batch processing completed successfully for exchangeId={}", serverToClientId);
            sendBatchExchangeStatusResponse(true, null);
        }, failure -> {
            logger.info(
                "[SERVER] Batch processing completed with failure for exchangeId={}, failure={}",
                serverToClientId,
                failure != null ? failure.getMessage() : "unknown"
            );
            handleDriverFailure(failure);
        });
    }

    /**
     * Handle driver failure by propagating it to the exchange sink handler and sending failure response.
     */
    private void handleDriverFailure(Exception failure) {
        logger.error("[SERVER] Server driver failed, propagating failure to exchange sink handler", failure);
        serverToClientSinkHandler.onFailure(failure);
        driverFuture.onFailure(failure);
        sendBatchExchangeStatusResponse(false, failure);
    }

    /**
     * Send batch exchange status response to the client.
     */
    private void sendBatchExchangeStatusResponse(boolean success, Exception failure) {
        if (batchExchangeStatusListener != null) {
            logger.info(
                "[SERVER] Sending batch exchange status {} response for exchangeId={}",
                success ? "success" : "failure",
                serverToClientId
            );
            batchExchangeStatusListener.onResponse(new BatchExchangeStatusResponse(success, failure));
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
        try {
            Transport.Connection connection = transportService.getConnection(clientNode);
            RemoteSink clientRemoteSink = exchangeService.newRemoteSink(task, clientToServerId, transportService, connection);
            logger.info(
                "[SERVER] Connected to client sink handler via transport for client-to-server exchange, exchangeId={}",
                clientToServerId
            );
            clientToServerSourceHandler.addRemoteSink(
                clientRemoteSink,
                true,
                () -> {},
                1,
                ActionListener.wrap(
                    nullValue -> logger.debug("[SERVER] Client-to-server exchange sink connection completed successfully"),
                    failure -> logger.error("[SERVER] Client-to-server exchange sink connection failed", failure)
                )
            );
        } catch (Exception e) {
            logger.error("[SERVER] Failed to connect to client sink handler for exchange " + clientToServerId, e);
            throw new IllegalStateException("Failed to connect to client sink handler for exchange " + clientToServerId, e);
        }
        // Create sink operator that writes to server-to-client exchange
        serverToClientSinkOperator = new ExchangeSinkOperator(serverToClientSink);
        ExchangeSinkOperator baseSinkOperator = serverToClientSinkOperator;

        // Wrap sink to convert Pages to BatchPages before sending to client
        // The driver will be set on the wrapper after BatchDriver construction
        SinkOperator wrappedSink = BatchDriver.wrapSink(baseSinkOperator);

        // Create BatchDriver with wrapped sink that converts Pages to BatchPages
        // BatchDriver will set itself on the PageToBatchPageOperator wrapper
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
            releasable
        );
        logger.info("[SERVER] BatchDriver created");

        // Set up batch done callback to send marker page
        batchDriver.onBatchDone().addListener(new ActionListener<Long>() {
            @Override
            public void onResponse(Long batchId) {
                try {
                    // Send empty marker page to signal batch completion
                    // Send directly to sink (same sink used by ExchangeSinkOperator)
                    BatchPage marker = BatchPage.createMarker(batchId);
                    serverToClientSink.addPage(marker);
                } catch (Exception e) {
                    logger.error("[SERVER] Failed to send marker page for batchId=" + batchId, e);
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("[SERVER] Batch done callback failed", e);
                // Propagate failure to exchange
                serverToClientSinkHandler.onFailure(e);
            }
        });
        logger.debug("[SERVER] Batch done callback registered");

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
        logger.info("[SERVER] Closing BidirectionalBatchExchangeServer");
        if (batchDriver != null) {
            logger.debug("[SERVER] BatchDriver will be closed by its releasable");
            // BatchDriver will be closed by its releasable
        }
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
        logger.info("[SERVER] BidirectionalBatchExchangeServer closed");
    }
}
