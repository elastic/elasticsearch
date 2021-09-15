/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
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
public class TransportRecoveryAction extends TransportBroadcastByNodeAction<RecoveryRequest, RecoveryResponse, RecoveryState> {

    private final IndicesService indicesService;

    @Inject
    public TransportRecoveryAction(ClusterService clusterService,
                                   TransportService transportService, IndicesService indicesService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(RecoveryAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                RecoveryRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected RecoveryState readShardResult(StreamInput in) throws IOException {
        return RecoveryState.readRecoveryState(in);
    }


    @Override
    protected RecoveryResponse newResponse(RecoveryRequest request, int totalShards, int successfulShards, int failedShards,
                                           List<RecoveryState> responses, List<DefaultShardOperationFailedException> shardFailures,
                                           ClusterState clusterState) {
        Map<String, List<RecoveryState>> shardResponses = new HashMap<>();
        for (RecoveryState recoveryState : responses) {
            if (recoveryState == null) {
                continue;
            }
            String indexName = recoveryState.getShardId().getIndexName();
            if (shardResponses.containsKey(indexName) == false) {
                shardResponses.put(indexName, new ArrayList<>());
            }
            if (request.activeOnly()) {
                if (recoveryState.getStage() != RecoveryState.Stage.DONE) {
                    shardResponses.get(indexName).add(recoveryState);
                }
            } else {
                shardResponses.get(indexName).add(recoveryState);
            }
        }
        return new RecoveryResponse(totalShards, successfulShards, failedShards, shardResponses, shardFailures);
    }

    @Override
    protected RecoveryRequest readRequestFrom(StreamInput in) throws IOException {
        return new RecoveryRequest(in);
    }

    @Override
    protected void shardOperation(RecoveryRequest request, ShardRouting shardRouting, Task task, ActionListener<RecoveryState> listener) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;
            runOnShardOperation();
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
            return indexShard.recoveryState();
        });
    }

    @Override
    protected ShardsIterator shards(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RecoveryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Nullable // unless running tests that inject extra behaviour
    private volatile Runnable onShardOperation;

    private void runOnShardOperation() {
        final Runnable onShardOperation = this.onShardOperation;
        if (onShardOperation != null) {
            onShardOperation.run();
        }
    }

    // exposed for tests: inject some extra behaviour that runs when shardOperation() is called
    void setOnShardOperation(@Nullable Runnable onShardOperation) {
        this.onShardOperation = onShardOperation;
    }
}
