/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportGetDesiredBalanceAction extends TransportMasterNodeReadAction<DesiredBalanceRequest, DesiredBalanceResponse> {

    @Nullable
    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator;

    @Inject
    public TransportGetDesiredBalanceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ShardsAllocator shardsAllocator
    ) {
        super(
            GetDesiredBalanceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DesiredBalanceRequest::new,
            indexNameExpressionResolver,
            DesiredBalanceResponse::from,
            ThreadPool.Names.MANAGEMENT
        );
        this.desiredBalanceShardsAllocator = shardsAllocator instanceof DesiredBalanceShardsAllocator
            ? (DesiredBalanceShardsAllocator) shardsAllocator
            : null;
    }

    @Override
    protected void masterOperation(
        Task task,
        DesiredBalanceRequest request,
        ClusterState state,
        ActionListener<DesiredBalanceResponse> listener
    ) throws Exception {
        String allocatorName = ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.get(state.metadata().settings());
        if (allocatorName.equals(ClusterModule.DESIRED_BALANCE_ALLOCATOR) == false || desiredBalanceShardsAllocator == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    "Expected the shard balance allocator to be `desired_balance`, but got `" + allocatorName + "`"
                )
            );
            return;
        }

        DesiredBalance latestDesiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();
        if (latestDesiredBalance == null) {
            listener.onFailure(new ResourceNotFoundException("Desired balance is not computed yet"));
            return;
        }
        Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> routingTable = new HashMap<>();
        for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
            Map<Integer, DesiredBalanceResponse.DesiredShards> indexDesiredShards = new HashMap<>();
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
                ShardAssignment shardAssignment = latestDesiredBalance.assignments().get(shardRoutingTable.shardId());
                List<DesiredBalanceResponse.ShardView> shardViews = new ArrayList<>();
                for (int idx = 0; idx < shardRoutingTable.size(); idx++) {
                    ShardRouting shard = shardRoutingTable.shard(idx);
                    shardViews.add(
                        new DesiredBalanceResponse.ShardView(
                            shard.state(),
                            shard.primary(),
                            shard.currentNodeId(),
                            shard.currentNodeId() != null
                                && shardAssignment != null
                                && shardAssignment.nodeIds().contains(shard.currentNodeId()),
                            shard.relocatingNodeId(),
                            shard.relocatingNodeId() != null
                                && shardAssignment != null
                                && shardAssignment.nodeIds().contains(shard.relocatingNodeId()),
                            shard.shardId().id(),
                            shard.getIndexName(),
                            shard.allocationId()
                        )
                    );
                }
                indexDesiredShards.put(
                    shardId,
                    new DesiredBalanceResponse.DesiredShards(
                        shardViews,
                        shardAssignment != null
                            ? new DesiredBalanceResponse.ShardAssignmentView(
                                shardAssignment.nodeIds(),
                                shardAssignment.total(),
                                shardAssignment.unassigned(),
                                shardAssignment.ignored()
                            )
                            : null
                    )
                );
            }
            routingTable.put(indexRoutingTable.getIndex().getName(), indexDesiredShards);
        }
        listener.onResponse(new DesiredBalanceResponse(desiredBalanceShardsAllocator.getStats(), routingTable));
    }

    @Override
    protected ClusterBlockException checkBlock(DesiredBalanceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
