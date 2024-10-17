/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
 * Will pass the work to {@link TransportClusterStatsAction} and return the response.
 */
public class TransportRemoteClusterStatsAction extends HandledTransportAction<RemoteClusterStatsRequest, RemoteClusterStatsResponse> {

    public static final String NAME = "cluster:monitor/stats/remote";
    public static final ActionType<RemoteClusterStatsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<RemoteClusterStatsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        RemoteClusterStatsResponse::new
    );
    private final NodeClient client;

    @Inject
    public TransportRemoteClusterStatsAction(NodeClient client, TransportService transportService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters, RemoteClusterStatsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RemoteClusterStatsRequest request, ActionListener<RemoteClusterStatsResponse> listener) {
        ClusterStatsRequest subRequest = new ClusterStatsRequest().asRemoteStats();
        subRequest.setParentTask(request.getParentTask());
        client.execute(
            TransportClusterStatsAction.TYPE,
            subRequest,
            listener.map(
                clusterStatsResponse -> new RemoteClusterStatsResponse(
                    clusterStatsResponse.getClusterUUID(),
                    clusterStatsResponse.getStatus(),
                    clusterStatsResponse.getNodesStats().getVersions(),
                    clusterStatsResponse.getNodesStats().getCounts().getTotal(),
                    clusterStatsResponse.getIndicesStats().getShards().getTotal(),
                    clusterStatsResponse.getIndicesStats().getIndexCount(),
                    clusterStatsResponse.getIndicesStats().getStore().sizeInBytes(),
                    clusterStatsResponse.getNodesStats().getJvm().getHeapMax().getBytes(),
                    clusterStatsResponse.getNodesStats().getOs().getMem().getTotal().getBytes()
                )
            )
        );
    }
}
