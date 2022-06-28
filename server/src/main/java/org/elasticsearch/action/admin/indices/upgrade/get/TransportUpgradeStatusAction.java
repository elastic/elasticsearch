/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportUpgradeStatusAction extends TransportBroadcastByNodeAction<
    UpgradeStatusRequest,
    UpgradeStatusResponse,
    ShardUpgradeStatus> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpgradeStatusAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpgradeStatusAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpgradeStatusRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Getting upgrade stats from *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpgradeStatusRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpgradeStatusRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpgradeStatusRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardUpgradeStatus readShardResult(StreamInput in) throws IOException {
        return new ShardUpgradeStatus(in);
    }

    @Override
    protected UpgradeStatusResponse newResponse(
        UpgradeStatusRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardUpgradeStatus> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new UpgradeStatusResponse(
            responses.toArray(new ShardUpgradeStatus[responses.size()]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected UpgradeStatusRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpgradeStatusRequest(in);
    }

    @Override
    protected void shardOperation(
        UpgradeStatusRequest request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<ShardUpgradeStatus> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
            List<Segment> segments = indexShard.segments(false);
            long total_bytes = 0;
            long to_upgrade_bytes = 0;
            long to_upgrade_bytes_ancient = 0;
            for (Segment seg : segments) {
                total_bytes += seg.sizeInBytes;
                if (seg.version.major != Version.CURRENT.luceneVersion.major) {
                    to_upgrade_bytes_ancient += seg.sizeInBytes;
                    to_upgrade_bytes += seg.sizeInBytes;
                } else if (seg.version.minor != Version.CURRENT.luceneVersion.minor) {
                    // TODO: this comparison is bogus! it would cause us to upgrade even with the same format
                    // instead, we should check if the codec has changed
                    to_upgrade_bytes += seg.sizeInBytes;
                }
            }

            return new ShardUpgradeStatus(indexShard.routingEntry(), total_bytes, to_upgrade_bytes, to_upgrade_bytes_ancient);
        });
    }
}
