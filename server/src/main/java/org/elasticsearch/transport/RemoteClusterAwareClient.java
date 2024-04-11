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
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

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
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        maybeEnsureConnected(ActionListener.wrap(unused -> {
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
                new ActionListenerResponseHandler<>(listener, action.getResponseReader(), responseExecutor)
            );
        }, connectionError -> {
            responseExecutor.execute(new AbstractRunnable() {
                @Override
                public boolean isForceExecution() {
                    return true; // we must complete every pending listener
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : new AssertionError(e);
                }

                @Override
                public void onRejection(Exception e) {
                    if (connectionError != e) {
                        connectionError.addSuppressed(e);
                    }
                    // force-execution means we won't be rejected unless we're shutting down
                    assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
                    // in this case it's better to complete the handler on the calling thread rather than leaking it
                    doRun();
                }

                @Override
                protected void doRun() {
                    listener.onFailure(connectionError);
                }
            });
        }));
    }

    private void maybeEnsureConnected(ActionListener<Void> ensureConnectedListener) {
        if (ensureConnected) {
            ActionListener.run(ensureConnectedListener, l -> remoteClusterService.ensureConnected(clusterAlias, l));
        } else {
            ensureConnectedListener.onResponse(null);
        }
    }
}
