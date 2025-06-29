/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A remote-only version of {@link TransportClusterStateAction} that should be used for cross-cluster requests.
 * It simply exists to handle incoming remote requests and forward them to the local transport action.
 */
public class TransportRemoteClusterStateAction extends HandledTransportAction<RemoteClusterStateRequest, ClusterStateResponse> {

    private final Client client;

    @Inject
    public TransportRemoteClusterStateAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            ClusterStateAction.NAME,
            transportService,
            actionFilters,
            RemoteClusterStateRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RemoteClusterStateRequest request, ActionListener<ClusterStateResponse> listener) {
        client.execute(ClusterStateAction.INSTANCE, request.clusterStateRequest(), listener);
    }
}
