/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation.DebugMode;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * The {@code TransportClusterAllocationExplainAction} is responsible for actually executing the explanation of a shard's allocation on the
 * master node in the cluster.
 */
public class TransportClusterAllocationExplainAction extends TransportMasterNodeAction<
    ClusterAllocationExplainRequest,
    ClusterAllocationExplainResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterAllocationExplainAction.class);

    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;

    @Inject
    public TransportClusterAllocationExplainAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        AllocationDeciders allocationDeciders,
        AllocationService allocationService
    ) {
        super(
            ClusterAllocationExplainAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterAllocationExplainRequest::new,
            indexNameExpressionResolver,
            ClusterAllocationExplainResponse::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.allocationDeciders = allocationDeciders;
        this.allocationService = allocationService;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterAllocationExplainRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterAllocationExplainRequest request,
        final ClusterState state,
        final ActionListener<ClusterAllocationExplainResponse> listener
    ) {
        final ClusterInfo clusterInfo = clusterInfoService.getClusterInfo();
        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            state,
            clusterInfo,
            snapshotsInfoService.snapshotShardSizes(),
            System.nanoTime()
        );

        ShardRouting shardRouting = findShardToExplain(request, allocation);
        logger.debug("explaining the allocation for [{}], found shard [{}]", request, shardRouting);

        ClusterAllocationExplanation cae = explainShard(
            shardRouting,
            allocation,
            request.includeDiskInfo() ? clusterInfo : null,
            request.includeYesDecisions(),
            request.useAnyUnassignedShard() == false,
            allocationService
        );
        listener.onResponse(new ClusterAllocationExplainResponse(cae));
    }

    // public for testing
    public static ClusterAllocationExplanation explainShard(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ClusterInfo clusterInfo,
        boolean includeYesDecisions,
        boolean isSpecificShard,
        AllocationService allocationService
    ) {

        allocation.setDebugMode(includeYesDecisions ? DebugMode.ON : DebugMode.EXCLUDE_YES_DECISIONS);

        ShardAllocationDecision shardDecision;
        if (shardRouting.initializing() || shardRouting.relocating()) {
            shardDecision = ShardAllocationDecision.NOT_TAKEN;
        } else {
            shardDecision = allocationService.explainShardAllocation(shardRouting, allocation);
        }

        var desiredNodeIds = allocationService.getShardsAllocator() instanceof DesiredBalanceShardsAllocator dbsa
            ? dbsa.getDesiredBalance().getNodeIds(shardRouting.shardId())
            : null;

        return new ClusterAllocationExplanation(
            isSpecificShard,
            shardRouting,
            shardRouting.currentNodeId() != null ? allocation.nodes().get(shardRouting.currentNodeId()) : null,
            shardRouting.relocatingNodeId() != null ? allocation.nodes().get(shardRouting.relocatingNodeId()) : null,
            clusterInfo,
            shardDecision,
            desiredNodeIds
        );
    }

    // public for testing
    public static ShardRouting findShardToExplain(ClusterAllocationExplainRequest request, RoutingAllocation allocation) {
        ShardRouting foundShard = null;
        if (request.useAnyUnassignedShard()) {
            // If we can use any shard, return the first unassigned primary (if there is one) or the first unassigned replica (if not)
            for (ShardRouting unassigned : allocation.routingNodes().unassigned()) {
                if (foundShard == null || unassigned.primary()) {
                    foundShard = unassigned;
                }
                if (foundShard.primary()) {
                    break;
                }
            }
            if (foundShard == null) {
                throw new IllegalArgumentException(
                    "No shard was specified in the request which means the response should explain a randomly-chosen unassigned shard, "
                        + "but there are no unassigned shards in this cluster. To explain the allocation of an assigned shard you must "
                        + "specify the target shard in the request."
                );
            }
        } else {
            String index = request.getIndex();
            int shard = request.getShard();
            if (request.isPrimary()) {
                // If we're looking for the primary shard, there's only one copy, so pick it directly
                foundShard = allocation.routingTable().shardRoutingTable(index, shard).primaryShard();
                if (request.getCurrentNode() != null) {
                    DiscoveryNode primaryNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    // the primary is assigned to a node other than the node specified in the request
                    if (primaryNode.getId().equals(foundShard.currentNodeId()) == false) {
                        throw new IllegalArgumentException(
                            "unable to find primary shard assigned to node [" + request.getCurrentNode() + "]"
                        );
                    }
                }
            } else {
                // If looking for a replica, go through all the replica shards
                List<ShardRouting> replicaShardRoutings = allocation.routingTable().shardRoutingTable(index, shard).replicaShards();
                if (request.getCurrentNode() != null) {
                    // the request is to explain a replica shard already assigned on a particular node,
                    // so find that shard copy
                    DiscoveryNode replicaNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    for (ShardRouting replica : replicaShardRoutings) {
                        if (replicaNode.getId().equals(replica.currentNodeId())) {
                            foundShard = replica;
                            break;
                        }
                    }
                    if (foundShard == null) {
                        throw new IllegalArgumentException(
                            "unable to find a replica shard assigned to node [" + request.getCurrentNode() + "]"
                        );
                    }
                } else {
                    if (replicaShardRoutings.size() > 0) {
                        // Pick the first replica at the very least
                        foundShard = replicaShardRoutings.get(0);
                        for (ShardRouting replica : replicaShardRoutings) {
                            // In case there are multiple replicas where some are assigned and some aren't,
                            // try to find one that is unassigned at least
                            if (replica.unassigned()) {
                                foundShard = replica;
                                break;
                            } else if (replica.started() && (foundShard.initializing() || foundShard.relocating())) {
                                // prefer started shards to initializing or relocating shards because started shards
                                // can be explained
                                foundShard = replica;
                            }
                        }
                    }
                }
            }
        }

        if (foundShard == null) {
            throw new IllegalArgumentException("unable to find any shards to explain [" + request + "] in the routing table");
        }
        return foundShard;
    }
}
