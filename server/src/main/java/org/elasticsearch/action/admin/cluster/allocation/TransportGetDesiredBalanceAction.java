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
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats;
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
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class TransportGetDesiredBalanceAction extends TransportMasterNodeReadAction<DesiredBalanceRequest, DesiredBalanceResponse> {

    @Nullable
    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator;
    private final ClusterInfoService clusterInfoService;
    private final WriteLoadForecaster writeLoadForecaster;

    @Inject
    public TransportGetDesiredBalanceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        WriteLoadForecaster writeLoadForecaster
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
        this.desiredBalanceShardsAllocator = shardsAllocator instanceof DesiredBalanceShardsAllocator allocator ? allocator : null;
        this.clusterInfoService = clusterInfoService;
        this.writeLoadForecaster = writeLoadForecaster;
    }

    @Override
    protected void masterOperation(
        Task task,
        DesiredBalanceRequest request,
        ClusterState state,
        ActionListener<DesiredBalanceResponse> listener
    ) throws Exception {
        if (desiredBalanceShardsAllocator == null) {
            listener.onFailure(new ResourceNotFoundException("Desired balance allocator is not in use, no desired balance found"));
            return;
        }

        DesiredBalance latestDesiredBalance = desiredBalanceShardsAllocator.getDesiredBalance();
        if (latestDesiredBalance == null) {
            listener.onFailure(new ResourceNotFoundException("Desired balance is not computed yet"));
            return;
        }
        var clusterInfo = clusterInfoService.getClusterInfo();
        listener.onResponse(
            new DesiredBalanceResponse(
                desiredBalanceShardsAllocator.getStats(),
                ClusterBalanceStats.createFrom(state, clusterInfo, writeLoadForecaster),
                createRoutingTable(state, latestDesiredBalance),
                clusterInfo
            )
        );
    }

    private Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> createRoutingTable(
        ClusterState state,
        DesiredBalance latestDesiredBalance
    ) {
        Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> routingTable = new HashMap<>();
        for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
            Map<Integer, DesiredBalanceResponse.DesiredShards> indexDesiredShards = new HashMap<>();
            IndexMetadata indexMetadata = state.metadata().index(indexRoutingTable.getIndex());
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
                ShardAssignment shardAssignment = latestDesiredBalance.assignments().get(shardRoutingTable.shardId());
                List<DesiredBalanceResponse.ShardView> shardViews = new ArrayList<>();
                for (int idx = 0; idx < shardRoutingTable.size(); idx++) {
                    ShardRouting shard = shardRoutingTable.shard(idx);
                    OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(indexMetadata);
                    OptionalLong forecastedShardSizeInBytes = indexMetadata.getForecastedShardSizeInBytes();
                    shardViews.add(
                        new DesiredBalanceResponse.ShardView(
                            shard.state(),
                            shard.primary(),
                            shard.currentNodeId(),
                            isDesired(shard.currentNodeId(), shardAssignment),
                            shard.relocatingNodeId(),
                            shard.relocatingNodeId() != null ? isDesired(shard.relocatingNodeId(), shardAssignment) : null,
                            shard.shardId().id(),
                            shard.getIndexName(),
                            forecastedWriteLoad.isPresent() ? forecastedWriteLoad.getAsDouble() : null,
                            forecastedShardSizeInBytes.isPresent() ? forecastedShardSizeInBytes.getAsLong() : null,
                            indexMetadata.getTierPreference()
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
        return routingTable;
    }

    private static boolean isDesired(@Nullable String nodeId, @Nullable ShardAssignment assignment) {
        return nodeId != null && assignment != null && assignment.nodeIds().contains(nodeId);
    }

    @Override
    protected ClusterBlockException checkBlock(DesiredBalanceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
