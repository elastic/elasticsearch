/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransportGetShutdownStatusAction extends TransportMasterNodeAction<
    GetShutdownStatusAction.Request,
    GetShutdownStatusAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportGetShutdownStatusAction.class);

    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final PluginShutdownService pluginShutdownService;

    @Inject
    public TransportGetShutdownStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        AllocationDeciders allocationDeciders,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        PluginShutdownService pluginShutdownService
    ) {
        super(
            GetShutdownStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShutdownStatusAction.Request::readFrom,
            indexNameExpressionResolver,
            GetShutdownStatusAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.allocationService = allocationService;
        this.allocationDeciders = allocationDeciders;
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.pluginShutdownService = pluginShutdownService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShutdownStatusAction.Request request,
        ClusterState state,
        ActionListener<GetShutdownStatusAction.Response> listener
    ) throws Exception {
        NodesShutdownMetadata nodesShutdownMetadata = state.metadata().custom(NodesShutdownMetadata.TYPE);

        GetShutdownStatusAction.Response response;
        if (nodesShutdownMetadata == null) {
            response = new GetShutdownStatusAction.Response(new ArrayList<>());
        } else if (request.getNodeIds().length == 0) {
            final List<SingleNodeShutdownStatus> shutdownStatuses = nodesShutdownMetadata.getAllNodeMetadataMap()
                .values()
                .stream()
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(
                            state,
                            ns.getNodeId(),
                            ns.getType(),
                            ns.getNodeSeen(),
                            clusterInfoService,
                            snapshotsInfoService,
                            allocationService,
                            allocationDeciders
                        ),
                        new ShutdownPersistentTasksStatus(),
                        new ShutdownPluginsStatus(pluginShutdownService.readyToShutdown(ns.getNodeId(), ns.getType()))
                    )
                )
                .collect(Collectors.toList());
            response = new GetShutdownStatusAction.Response(shutdownStatuses);
        } else {
            new ArrayList<>();
            final Map<String, SingleNodeShutdownMetadata> nodeShutdownMetadataMap = nodesShutdownMetadata.getAllNodeMetadataMap();
            final List<SingleNodeShutdownStatus> shutdownStatuses = Arrays.stream(request.getNodeIds())
                .map(nodeShutdownMetadataMap::get)
                .filter(Objects::nonNull)
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(
                            state,
                            ns.getNodeId(),
                            ns.getType(),
                            ns.getNodeSeen(),
                            clusterInfoService,
                            snapshotsInfoService,
                            allocationService,
                            allocationDeciders
                        ),
                        new ShutdownPersistentTasksStatus(),
                        new ShutdownPluginsStatus(pluginShutdownService.readyToShutdown(ns.getNodeId(), ns.getType()))
                    )

                )
                .collect(Collectors.toList());
            response = new GetShutdownStatusAction.Response(shutdownStatuses);
        }

        listener.onResponse(response);
    }

    // pkg-private for testing
    static ShutdownShardMigrationStatus shardMigrationStatus(
        ClusterState currentState,
        String nodeId,
        SingleNodeShutdownMetadata.Type shutdownType,
        boolean nodeSeen,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        AllocationService allocationService,
        AllocationDeciders allocationDeciders
    ) {
        // Only REMOVE-type shutdowns will try to move shards, so RESTART-type shutdowns should immediately complete
        if (SingleNodeShutdownMetadata.Type.RESTART.equals(shutdownType)) {
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.COMPLETE,
                0,
                "no shard relocation is necessary for a node restart"
            );
        }

        if (currentState.nodes().get(nodeId) == null && nodeSeen == false) {
            // The node isn't in the cluster
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.NOT_STARTED,
                0,
                "node is not currently part of the cluster"
            );
        }

        // The node is in `DiscoveryNodes`, but not `RoutingNodes` - so there are no shards assigned to it. We're done.
        if (currentState.getRoutingNodes().node(nodeId) == null) {
            // We don't know about that node
            return new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0);
        }

        // First, check if there are any shards currently on this node, and if there are any relocating shards
        int startedShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.STARTED);
        int relocatingShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.RELOCATING);
        int initializingShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.INITIALIZING);
        int totalRemainingShards = relocatingShards + startedShards + initializingShards;

        // If there's relocating shards, or no shards on this node, we'll just use the number of shards left to move
        if (relocatingShards > 0 || totalRemainingShards == 0) {
            SingleNodeShutdownMetadata.Status shardStatus = totalRemainingShards == 0
                ? SingleNodeShutdownMetadata.Status.COMPLETE
                : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
            return new ShutdownShardMigrationStatus(shardStatus, totalRemainingShards);
        } else if (initializingShards > 0 && relocatingShards == 0 && startedShards == 0) {
            // If there's only initializing shards left, return now with a note that only initializing shards are left
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                totalRemainingShards,
                "all remaining shards are currently INITIALIZING and must finish before they can be moved off this node"
            );
        }

        // If there's no relocating shards and shards still on this node, we need to figure out why
        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            currentState.getRoutingNodes(),
            currentState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            System.nanoTime()
        );
        allocation.setDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        // Explain shard allocations until we find one that can't move, then stop (as `findFirst` short-circuits)
        final Optional<ShardRouting> unmovableShard = currentState.getRoutingNodes()
            .node(nodeId)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .map(shardRouting -> new Tuple<>(shardRouting, allocationService.explainShardAllocation(shardRouting, allocation)))
            // Given that we're checking the status of a node that's shutting down, no shards should be allowed to remain
            .filter(pair -> {
                assert pair.v2().getMoveDecision().canRemain() == false
                    : "shard [" + pair + "] can remain on node [" + nodeId + "], but that node is shutting down";
                return pair.v2().getMoveDecision().canRemain() == false;
            })
            // It's okay if some are throttled, they'll move eventually
            .filter(pair -> pair.v2().getMoveDecision().getAllocationDecision().equals(AllocationDecision.THROTTLED) == false)
            // These shards will move as soon as possible
            .filter(pair -> pair.v2().getMoveDecision().getAllocationDecision().equals(AllocationDecision.YES) == false)
            .peek(pair -> {
                if (logger.isTraceEnabled()) { // don't serialize the decision unless we have to
                    logger.trace(
                        "node [{}] shutdown of type [{}] stalled: found shard [{}][{}] from index [{}] with negative decision: [{}]",
                        nodeId,
                        shutdownType,
                        pair.v1().getId(),
                        pair.v1().primary() ? "primary" : "replica",
                        pair.v1().shardId().getIndexName(),
                        Strings.toString(pair.v2())
                    );
                }
            })
            .map(Tuple::v1)
            .findFirst();

        if (unmovableShard.isPresent()) {
            // We found a shard that can't be moved, so shard relocation is stalled. Blame the unmovable shard.
            ShardRouting shardRouting = unmovableShard.get();

            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.STALLED,
                totalRemainingShards,
                new ParameterizedMessage(
                    "shard [{}] [{}] of index [{}] cannot move, use the Cluster Allocation Explain API on this shard for details",
                    shardRouting.shardId().getId(),
                    shardRouting.primary() ? "primary" : "replica",
                    shardRouting.index().getName()
                ).getFormattedMessage()
            );
        } else {
            // We couldn't find any shards that can't be moved, so we're just waiting for other recoveries or initializing shards
            return new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.IN_PROGRESS, totalRemainingShards);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetShutdownStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
