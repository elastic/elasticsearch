/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService;
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService.PendingRecoverySnapshot;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action for shard recovery operation. This transport action does not actually
 * perform shard recovery, it only reports on recoveries (both active and complete).
 */
public class TransportRecoveryAction extends TransportBroadcastByNodeAction<
    RecoveryRequest,
    RecoveryResponse,
    ShardRecoveryInfo,
    PendingRecoverySnapshot> {

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;
    private final ThrottlingRecoveryService throttlingRecoveryService;

    @Inject
    public TransportRecoveryAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver,
        ThrottlingRecoveryService throttlingRecoveryService
    ) {
        super(
            RecoveryAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            RecoveryRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
        this.throttlingRecoveryService = throttlingRecoveryService;
    }

    @Override
    protected ShardRecoveryInfo readShardResult(StreamInput in) throws IOException {
        return new ShardRecoveryInfo(in);
    }

    @Override
    protected ResponseFactory<RecoveryResponse, ShardRecoveryInfo> getResponseFactory(RecoveryRequest request, ClusterState clusterState) {
        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> {
            Map<String, List<ShardRecoveryInfo>> shardResponses = new HashMap<>();
            for (ShardRecoveryInfo recoveryInfo : responses) {
                if (recoveryInfo == null) {
                    continue;
                }
                final RecoveryState recoveryState = recoveryInfo.recoveryState();
                String indexName = recoveryState.getShardId().getIndexName();
                if (shardResponses.containsKey(indexName) == false) {
                    shardResponses.put(indexName, new ArrayList<>());
                }
                if (request.activeOnly() == false || isActive(recoveryState)) {
                    shardResponses.get(indexName).add(recoveryInfo);
                }
            }
            return new RecoveryResponse(totalShards, successfulShards, failedShards, shardResponses, shardFailures);
        };
    }

    /// A recovery is active if it has not yet completed, including recoveries that are queued ([RecoveryState.Stage#CREATED])
    /// and those that have started. Only completed recoveries ([RecoveryState.Stage#DONE]) are excluded when `active_only` is set.
    private static boolean isActive(RecoveryState recoveryState) {
        return recoveryState.getStage() != RecoveryState.Stage.DONE;
    }

    @Override
    protected RecoveryRequest readRequestFrom(StreamInput in) throws IOException {
        return new RecoveryRequest(in);
    }

    @Override
    protected void shardOperation(
        RecoveryRequest request,
        ShardRouting shardRouting,
        Task task,
        PendingRecoverySnapshot nodeContext,
        ActionListener<ShardRecoveryInfo> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
            assert shardRouting.allocationId() != null;
            return new ShardRecoveryInfo(indexShard.recoveryState(), nodeContext.gateFor(shardRouting.allocationId().getId()));
        });
    }

    @Override
    protected PendingRecoverySnapshot createNodeContext() {
        return throttlingRecoveryService.pendingRecoveries();
    }

    @Override
    protected ShardsIterator shards(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.routingTable(projectResolver.getProjectId()).allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RecoveryRequest request) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
