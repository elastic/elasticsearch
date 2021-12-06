/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class TransportForgetFollowerAction extends TransportBroadcastByNodeAction<
    ForgetFollowerAction.Request,
    BroadcastResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportForgetFollowerAction(
        final ClusterService clusterService,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final IndicesService indicesService
    ) {
        super(
            ForgetFollowerAction.NAME,
            Objects.requireNonNull(clusterService),
            Objects.requireNonNull(transportService),
            Objects.requireNonNull(actionFilters),
            Objects.requireNonNull(indexNameExpressionResolver),
            ForgetFollowerAction.Request::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.indicesService = Objects.requireNonNull(indicesService);
    }

    @Override
    protected EmptyResult readShardResult(final StreamInput in) {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected BroadcastResponse newResponse(
        final ForgetFollowerAction.Request request,
        final int totalShards,
        final int successfulShards,
        final int failedShards,
        List<EmptyResult> emptyResults,
        final List<DefaultShardOperationFailedException> shardFailures,
        final ClusterState clusterState
    ) {
        return new BroadcastResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ForgetFollowerAction.Request readRequestFrom(final StreamInput in) throws IOException {
        return new ForgetFollowerAction.Request(in);
    }

    @Override
    protected void shardOperation(
        final ForgetFollowerAction.Request request,
        final ShardRouting shardRouting,
        Task task,
        ActionListener<EmptyResult> listener
    ) {
        final Index followerIndex = new Index(request.followerIndex(), request.followerIndexUUID());
        final Index leaderIndex = clusterService.state().metadata().index(request.leaderIndex()).getIndex();
        final String id = CcrRetentionLeases.retentionLeaseId(
            request.followerCluster(),
            followerIndex,
            request.leaderRemoteCluster(),
            leaderIndex
        );

        final IndexShard indexShard = indicesService.indexServiceSafe(leaderIndex).getShard(shardRouting.shardId().id());

        indexShard.acquirePrimaryOperationPermit(new ActionListener.Delegating<>(listener) {
            @Override
            public void onResponse(Releasable releasable) {
                try {
                    indexShard.removeRetentionLease(id, new ActionListener<ReplicationResponse>() {
                        @Override
                        public void onResponse(ReplicationResponse replicationResponse) {
                            releasable.close();
                            delegate.onResponse(EmptyResult.INSTANCE);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            releasable.close();
                            delegate.onFailure(e);
                        }
                    });
                } catch (Exception e) {
                    releasable.close();
                    onFailure(e);
                }
            }
        }, ThreadPool.Names.SAME, request);
    }

    @Override
    protected ShardsIterator shards(
        final ClusterState clusterState,
        final ForgetFollowerAction.Request request,
        final String[] concreteIndices
    ) {
        final GroupShardsIterator<ShardIterator> activePrimaryShards = clusterState.routingTable()
            .activePrimaryShardsGrouped(concreteIndices, false);
        final List<ShardRouting> shardRoutings = new ArrayList<>();
        final Iterator<ShardIterator> it = activePrimaryShards.iterator();
        while (it.hasNext()) {
            final ShardIterator shardIterator = it.next();
            final ShardRouting primaryShard = shardIterator.nextOrNull();
            assert primaryShard != null;
            shardRoutings.add(primaryShard);
            if (Assertions.ENABLED) {
                final ShardRouting maybeNextPrimaryShard = shardIterator.nextOrNull();
                assert maybeNextPrimaryShard == null : maybeNextPrimaryShard;
            }
        }
        return new PlainShardsIterator(shardRoutings);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(final ClusterState state, final ForgetFollowerAction.Request request) {
        return null;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        final ClusterState state,
        final ForgetFollowerAction.Request request,
        final String[] concreteIndices
    ) {
        return null;
    }
}
