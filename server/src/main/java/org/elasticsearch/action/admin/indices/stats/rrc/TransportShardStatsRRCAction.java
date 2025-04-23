/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats.rrc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardStatsRRCAction extends
    TransportBroadcastByNodeAction<ShardStatsRRCRequest, ShardStatsRRCResponse, ShardStatsRRC> {

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportShardStatsRRCAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ShardStatsRRCAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ShardStatsRRCRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, ShardStatsRRCRequest request, String[] concreteIndices) {
        return clusterState.routingTable(projectResolver.getProjectId()).allReplicaShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ShardStatsRRCRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ShardStatsRRCRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStatsRRC readShardResult(StreamInput in) throws IOException {
        return new ShardStatsRRC(in);
    }

    @Override
    protected ResponseFactory<ShardStatsRRCResponse, ShardStatsRRC> getResponseFactory(ShardStatsRRCRequest request,
                                                                                       ClusterState clusterState) {
        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> new ShardStatsRRCResponse(
            responses.toArray(new ShardStatsRRC[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected ShardStatsRRCRequest readRequestFrom(StreamInput in) throws IOException {
        return new ShardStatsRRCRequest(in);
    }

    @Override
    protected void shardOperation(ShardStatsRRCRequest request,
                                  ShardRouting shardRouting,
                                  Task task,
                                  ActionListener<ShardStatsRRC> listener) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;

            ShardId shardId = shardRouting.shardId();
            IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            SearchStats.Stats stats = indexShard.searchStats().getTotal();

            long trackedTime = stats.getQueryTimeInMillis() +
                stats.getFetchTimeInMillis() +
                stats.getScrollTimeInMillis() +
                stats.getScrollTimeInMillis();

            return new ShardStatsRRC(shardId.getIndex().getName(), shardId.getId(), shardRouting.allocationId().getId(), trackedTime);
        });
    }
}
