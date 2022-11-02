/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.desiredbalance;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportGetDesiredBalanceAction extends TransportMasterNodeReadAction<DesiredBalanceRequest, DesiredBalanceResponse> {

    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator;

    @Inject
    public TransportGetDesiredBalanceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DesiredBalanceShardsAllocator desiredBalanceShardsAllocator
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
            ThreadPool.Names.SAME
        );
        this.desiredBalanceShardsAllocator = desiredBalanceShardsAllocator;
    }

    @Override
    protected void masterOperation(
        Task task,
        DesiredBalanceRequest request,
        ClusterState state,
        ActionListener<DesiredBalanceResponse> listener
    ) throws Exception {
        DesiredBalance latestDesiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();
        if (latestDesiredBalance == null) {
            listener.onFailure(new ResourceNotFoundException("Desired balance not found"));
            return;
        }
        Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> routingTable = new HashMap<>();
        for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
            Map<Integer, DesiredBalanceResponse.DesiredShards> indexDesiredShards = new HashMap<>();
            for (ShardRouting shard : indexRoutingTable.randomAllActiveShardsIt()) {
                ShardAssignment shardAssignment = latestDesiredBalance.assignments().get(shard.shardId());
                if (shardAssignment == null) {
                    continue;
                }
                indexDesiredShards.put(
                    shard.id(),
                    new DesiredBalanceResponse.DesiredShards(
                        new DesiredBalanceResponse.ShardView(
                            shard.state(),
                            shard.primary(),
                            shard.currentNodeId(),
                            shardAssignment.nodeIds().contains(shard.currentNodeId()),
                            shard.relocatingNodeId(),
                            shardAssignment.nodeIds().contains(shard.relocatingNodeId()),
                            shard.shardId().id(),
                            shard.getIndexName(),
                            shard.allocationId()
                        ),
                        new DesiredBalanceResponse.ShardAssignmentView(
                            shardAssignment.nodeIds(),
                            shardAssignment.total(),
                            shardAssignment.unassigned(),
                            shardAssignment.ignored()
                        )
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
