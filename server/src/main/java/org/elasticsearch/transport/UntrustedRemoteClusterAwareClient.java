/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class UntrustedRemoteClusterAwareClient extends AbstractClient {

    private final RemoteClusterAwareClient delegate;
    private final String clusterAlias;

    public UntrustedRemoteClusterAwareClient(
        Settings settings,
        ThreadPool threadPool,
        TransportService service,
        String clusterAlias,
        boolean ensureConnected
    ) {
        super(settings, threadPool);
        this.delegate = new RemoteClusterAwareClient(settings, threadPool, service, clusterAlias, ensureConnected);
        this.clusterAlias = clusterAlias;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        try (var ignored = threadPool().getThreadContext().newStoredContext()) {
            threadPool().getThreadContext().putTransient(RemoteClusterService.REMOTE_CLUSTER_ALIAS_TRANSIENT_NAME, clusterAlias);
            delegate.doExecute(action, request, listener);
        }
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return delegate.getRemoteClusterClient(clusterAlias);
    }
}
