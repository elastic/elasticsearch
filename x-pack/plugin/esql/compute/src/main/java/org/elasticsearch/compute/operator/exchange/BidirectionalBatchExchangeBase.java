/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
     * Base constructor for bidirectional batch exchange handlers.
     */
    protected BidirectionalBatchExchangeBase(
        String sessionId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task
    ) {
        this.sessionId = sessionId;
        this.clientToServerId = buildClientToServerId(sessionId);
        this.serverToClientId = buildServerToClientId(sessionId);
        this.exchangeService = exchangeService;
        this.executor = executor;
        this.maxBufferSize = maxBufferSize;
        this.transportService = transportService;
        this.task = task;
    }

    /**
     * Connects a remote sink to a source handler via transport.
     * This is a common pattern used by both server and client to establish
     * transport-based connections for bidirectional exchange.
     * <p>
     * NOTE: It is extremely important to use exactly one instance when calling addRemoteSink.
     * Otherwise pages might arrive out of order.
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
            // NOTE: It is extremely important to use exactly one instance when calling addRemoteSink
            // Otherwise we might get pages out of order
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, 1, listener);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to " + errorMessagePrefix + " for exchange [" + exchangeId + "]", e);
        }
    }
}
