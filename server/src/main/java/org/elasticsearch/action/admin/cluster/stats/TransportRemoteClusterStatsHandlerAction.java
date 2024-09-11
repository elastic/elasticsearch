/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * Handler action for incoming {@link RemoteClusterStatsRequest}.
 * Will pass the work to {@link TransportRemoteClusterStatsAction} and return the response.
 */
public class TransportRemoteClusterStatsHandlerAction extends HandledTransportAction<
    RemoteClusterStatsRequest,
    RemoteClusterStatsResponse> {

    public static final ActionType<RemoteClusterStatsResponse> TYPE = new ActionType<>("cluster:monitor/stats/remote/handler");
    public static final RemoteClusterActionType<RemoteClusterStatsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        TYPE.name(),
        RemoteClusterStatsResponse::new
    );
    private final NodeClient client;

    @Inject
    public TransportRemoteClusterStatsHandlerAction(NodeClient client, TransportService transportService, ActionFilters actionFilters) {
        super(TYPE.name(), transportService, actionFilters, RemoteClusterStatsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RemoteClusterStatsRequest request, ActionListener<RemoteClusterStatsResponse> listener) {
        ClusterStatsRequest subRequest = new ClusterStatsRequest(request.nodesIds());
        client.execute(TransportRemoteClusterStatsAction.TYPE, subRequest, listener);
    }
}
