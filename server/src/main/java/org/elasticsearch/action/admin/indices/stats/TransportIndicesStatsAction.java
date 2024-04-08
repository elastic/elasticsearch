/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportIndicesStatsAction extends TransportBroadcastByNodeAction<IndicesStatsRequest, IndicesStatsResponse, ShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            IndicesStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            IndicesStatsRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStats readShardResult(StreamInput in) throws IOException {
        return new ShardStats(in);
    }

    @Override
    protected ResponseFactory<IndicesStatsResponse, ShardStats> getResponseFactory(IndicesStatsRequest request, ClusterState clusterState) {
        // NB avoid capture of full cluster state
        final var metadata = clusterState.getMetadata();
        final var routingTable = clusterState.routingTable();

        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> new IndicesStatsResponse(
            responses.toArray(new ShardStats[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            metadata,
            routingTable
        );
    }

    @Override
    protected IndicesStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new IndicesStatsRequest(in);
    }

    @Override
    protected void shardOperation(IndicesStatsRequest request, ShardRouting shardRouting, Task task, ActionListener<ShardStats> listener) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
            CommonStats commonStats = CommonStats.getShardLevelStats(indicesService.getIndicesQueryCache(), indexShard, request.flags());
            CommitStats commitStats;
            SeqNoStats seqNoStats;
            RetentionLeaseStats retentionLeaseStats;
            try {
                commitStats = indexShard.commitStats();
                seqNoStats = indexShard.seqNoStats();
                retentionLeaseStats = indexShard.getRetentionLeaseStats();
            } catch (final AlreadyClosedException e) {
                // shard is closed - no stats is fine
                commitStats = null;
                seqNoStats = null;
                retentionLeaseStats = null;
            }
            return new ShardStats(
                indexShard.routingEntry(),
                indexShard.shardPath(),
                commonStats,
                commitStats,
                seqNoStats,
                retentionLeaseStats,
                indexShard.isSearchIdle(),
                indexShard.searchIdleTime()
            );
        });
    }
}
