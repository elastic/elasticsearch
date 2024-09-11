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
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;

import java.util.HashSet;
import java.util.List;

/**
 * Transport action for remote cluster stats. It returs a reduced answer since most of the stats from the remote
 * cluster are not needed.
 */
public class TransportRemoteClusterStatsAction extends TransportClusterStatsBaseAction<RemoteClusterStatsResponse> {

    public static final ActionType<RemoteClusterStatsResponse> TYPE = new ActionType<>("cluster:monitor/stats/remote");
    public static final RemoteClusterActionType<RemoteClusterStatsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        TYPE.name(),
        RemoteClusterStatsResponse::new
    );

    @Inject
    public TransportRemoteClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        UsageService usageService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            threadPool,
            clusterService,
            transportService,
            nodeService,
            indicesService,
            repositoriesService,
            usageService,
            actionFilters
        );
        transportService.registerRequestHandler(
            TYPE.name(),
            // TODO: which executor here?
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            RemoteClusterStatsRequest::new,
            (request, channel, task) -> execute(task, request, new ActionListener<>() {
                @Override
                public void onResponse(RemoteClusterStatsResponse response) {
                    channel.sendResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    channel.sendResponse(e);
                }
            })
        );
    }

    @Override
    protected void newResponseAsync(
        final Task task,
        final ClusterStatsRequest request,
        final List<ClusterStatsNodeResponse> responses,
        final List<FailedNodeException> failures,
        final ActionListener<RemoteClusterStatsResponse> listener
    ) {
        final ClusterState state = clusterService.state();
        final Metadata metadata = state.metadata();
        ClusterHealthStatus status = null;
        long totalShards = 0;
        long indicesBytes = 0;
        var indexSet = new HashSet<String>();

        for (ClusterStatsNodeResponse r : responses) {
            totalShards += r.shardsStats().length;
            for (var shard : r.shardsStats()) {
                indexSet.add(shard.getShardRouting().getIndexName());
                if (shard.getStats().getStore() != null) {
                    indicesBytes += shard.getStats().getStore().totalDataSetSizeInBytes();
                }
            }
            if (status == null && r.clusterStatus() != null) {
                status = r.clusterStatus();
            }
        }

        ClusterStatsNodes nodesStats = new ClusterStatsNodes(responses);
        RemoteClusterStatsResponse response = new RemoteClusterStatsResponse(
            clusterService.getClusterName(),
            metadata.clusterUUID(),
            status,
            nodesStats.getVersions(),
            nodesStats.getCounts().getTotal(),
            totalShards,
            indexSet.size(),
            indicesBytes,
            nodesStats.getJvm().getHeapMax().getBytes(),
            nodesStats.getOs().getMem().getTotal().getBytes()
        );
        listener.onResponse(response);
    }
}
