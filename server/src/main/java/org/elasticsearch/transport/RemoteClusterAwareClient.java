/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.Executor;

final class RemoteClusterAwareClient implements RemoteClusterClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;
    private final Executor responseExecutor;
    private final boolean ensureConnected;

    RemoteClusterAwareClient(TransportService service, String clusterAlias, Executor responseExecutor, boolean ensureConnected) {
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
        this.responseExecutor = responseExecutor;
        this.ensureConnected = ensureConnected;
    }

    @Override
    public <Request extends ActionRequest, Response extends TransportResponse> void execute(
        Transport.Connection connection,
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        service.sendRequest(
            connection,
            action.name(),
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, action.getResponseReader(), responseExecutor)
        );
    }

    @Override
    public <Request extends ActionRequest> void getConnection(@Nullable Request request, ActionListener<Transport.Connection> listener) {
        SubscribableListener

            .<Void>newForked(ensureConnectedListener -> {
                if (ensureConnected) {
                    remoteClusterService.ensureConnected(clusterAlias, ensureConnectedListener);
                } else {
                    ensureConnectedListener.onResponse(null);
                }
            })

            .andThenApply(ignored -> {
                try {
                    if (request instanceof RemoteClusterAwareRequest remoteClusterAwareRequest) {
                        return remoteClusterService.getConnection(remoteClusterAwareRequest.getPreferredTargetNode(), clusterAlias);
                    } else {
                        return remoteClusterService.getConnection(clusterAlias);
                    }
                } catch (ConnectTransportException e) {
                    if (ensureConnected == false) {
                        // trigger another connection attempt, but don't wait for it to complete
                        remoteClusterService.ensureConnected(clusterAlias, ActionListener.noop());
                    }
                    throw e;
                }
            })

            .addListener(listener);
    }
}
