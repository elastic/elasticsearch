/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * Base class for bidirectional batch exchange handlers.
 * Contains common fields and exchange ID construction logic shared by
 * {@link BidirectionalBatchExchangeServer} and {@link BidirectionalBatchExchangeClient}.
 */
abstract class BidirectionalBatchExchangeBase implements Releasable {

    protected final String clientToServerId;
    protected final String serverToClientId;
    protected final String sessionId;
    protected final ExchangeService exchangeService;
    protected final Executor executor;
    protected final int maxBufferSize;
    protected final TransportService transportService;
    protected final Task task;
    protected final Settings settings;

    /**
     * Constructs the client-to-server exchange ID from the session ID.
     */
    protected static String buildClientToServerId(String sessionId) {
        return sessionId + "/clientToServer";
    }

    /**
     * Constructs the server-to-client exchange ID from the session ID.
     */
    protected static String buildServerToClientId(String sessionId) {
        return sessionId + "/serverToClient";
    }

    /**
     * Base constructor for bidirectional batch exchange handlers with explicit exchange IDs.
     *
     * @param sessionId the session ID (used for logging and identification)
     * @param clientToServerId the exchange ID for client-to-server communication
     * @param serverToClientId the exchange ID for server-to-client communication
     */
    protected BidirectionalBatchExchangeBase(
        String sessionId,
        String clientToServerId,
        String serverToClientId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        Settings settings
    ) {
        this.sessionId = sessionId;
        this.clientToServerId = clientToServerId;
        this.serverToClientId = serverToClientId;
        this.exchangeService = exchangeService;
        this.executor = executor;
        this.maxBufferSize = maxBufferSize;
        this.transportService = transportService;
        this.task = task;
        this.settings = settings;
    }

    /**
     * Connects a remote sink to a source handler via transport.
     * This is a common pattern used by both server and client to establish
     * transport-based connections for bidirectional exchange.
     * <p>
     * The {@link BatchSortedExchangeSource} ensures pages are delivered in order within each batch.
     */
    protected void connectRemoteSink(
        DiscoveryNode node,
        String exchangeId,
        ExchangeSourceHandler sourceHandler,
        ActionListener<Void> listener,
        String errorMessagePrefix
    ) {
        try {
            Transport.Connection connection = transportService.getConnection(node);
            RemoteSink remoteSink = exchangeService.newRemoteSink(task, exchangeId, transportService, connection);
            int concurrentClients = ExchangeSourceHandler.getConcurrentClients(settings);
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, concurrentClients, listener);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to " + errorMessagePrefix + " for exchange [" + exchangeId + "]", e);
        }
    }
}
