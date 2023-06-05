/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

final class RemoteClusterAwareClient extends AbstractClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;
    private final boolean ensureConnected;

    RemoteClusterAwareClient(
        Settings settings,
        ThreadPool threadPool,
        TransportService service,
        String clusterAlias,
        boolean ensureConnected
    ) {
        super(settings, threadPool);
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
        this.ensureConnected = ensureConnected;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        maybeEnsureConnected(listener.wrapFailure((delegateListener, v) -> {
            final Transport.Connection connection;
            try {
                if (request instanceof RemoteClusterAwareRequest) {
                    DiscoveryNode preferredTargetNode = ((RemoteClusterAwareRequest) request).getPreferredTargetNode();
                    connection = remoteClusterService.getConnection(preferredTargetNode, clusterAlias);
                } else {
                    connection = remoteClusterService.getConnection(clusterAlias);
                }
            } catch (NoSuchRemoteClusterException e) {
                if (ensureConnected == false) {
                    // trigger another connection attempt, but don't wait for it to complete
                    remoteClusterService.ensureConnected(clusterAlias, ActionListener.noop());
                }
                throw e;
            }
            service.sendRequest(
                connection,
                action.name(),
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(delegateListener, action.getResponseReader())
            );
        }));
    }

    private void maybeEnsureConnected(ActionListener<Void> ensureConnectedListener) {
        if (ensureConnected) {
            remoteClusterService.ensureConnected(clusterAlias, ensureConnectedListener);
        } else {
            ensureConnectedListener.onResponse(null);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Client getRemoteClusterClient(String remoteClusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), remoteClusterAlias);
    }
}
