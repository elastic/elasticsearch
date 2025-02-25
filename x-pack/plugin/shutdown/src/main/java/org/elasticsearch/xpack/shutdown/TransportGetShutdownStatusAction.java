/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus.NODE_ALLOCATION_DECISION_KEY;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;

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
        AllocationService allocationService,
        AllocationDeciders allocationDeciders,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        PluginShutdownService pluginShutdownService
    ) {
        super(
            GetShutdownStatusAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShutdownStatusAction.Request::new,
            GetShutdownStatusAction.Response::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
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
    ) {
        CancellableTask cancellableTask = (CancellableTask) task;
        NodesShutdownMetadata nodesShutdownMetadata = state.metadata().custom(NodesShutdownMetadata.TYPE);

        GetShutdownStatusAction.Response response;
        if (nodesShutdownMetadata == null) {
            response = new GetShutdownStatusAction.Response(new ArrayList<>());
        } else if (request.getNodeIds().length == 0) {
            final List<SingleNodeShutdownStatus> shutdownStatuses = nodesShutdownMetadata.getAll()
                .values()
                .stream()
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(
                            cancellableTask,
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
            final List<SingleNodeShutdownStatus> shutdownStatuses = Arrays.stream(request.getNodeIds())
                .map(nodesShutdownMetadata::get)
                .filter(Objects::nonNull)
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(
                            cancellableTask,
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
        CancellableTask cancellableTask,
        ClusterState currentState,
        String nodeId,
        SingleNodeShutdownMetadata.Type shutdownType,
        boolean nodeSeen,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        AllocationService allocationService,
        AllocationDeciders allocationDeciders
    ) {
        assert Transports.assertNotTransportThread("doing O(#shards) work must be forked");

        // Only REMOVE-type shutdowns will try to move shards, so RESTART-type shutdowns should immediately complete
        if (SingleNodeShutdownMetadata.Type.RESTART.equals(shutdownType)) {
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.COMPLETE,
                0,
                "no shard relocation is necessary for a node restart",
                null
            );
        }

        if (currentState.nodes().get(nodeId) == null && nodeSeen == false) {
            // The node isn't in the cluster
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.NOT_STARTED,
                0,
                "node is not currently part of the cluster",
                null
            );
        }

        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            currentState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            System.nanoTime()
        );
        allocation.setDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        // We also need the set of node IDs which are currently shutting down.
        Set<String> shuttingDownNodes = currentState.metadata().nodeShutdowns().getAll().keySet();

        // Check if we have any unassigned primary shards that have this nodeId as their lastAllocatedNodeId
        var unassignedShards = currentState.getRoutingNodes()
            .unassigned()
            .stream()
            .peek(s -> cancellableTask.ensureNotCancelled())
            .filter(s -> Objects.equals(s.unassignedInfo().lastAllocatedNodeId(), nodeId))
            .filter(s -> s.primary() || hasShardCopyOnAnotherNode(currentState, s, shuttingDownNodes) == false)
            .toList();

        if (unassignedShards.isEmpty() == false) {
            var shardRouting = unassignedShards.get(0);
            ShardAllocationDecision decision = allocationService.explainShardAllocation(shardRouting, allocation);

            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.STALLED,
                unassignedShards.size(),
                format(
                    "shard [%s] [%s] of index [%s] is unassigned, see [%s] for details or use the cluster allocation explain API",
                    shardRouting.shardId().getId(),
                    shardRouting.primary() ? "primary" : "replica",
                    shardRouting.index().getName(),
                    NODE_ALLOCATION_DECISION_KEY
                ),
                decision
            );
        }

        // The node is in `DiscoveryNodes`, but not `RoutingNodes` - so there are no shards assigned to it. We're done.
        if (currentState.getRoutingNodes().node(nodeId) == null) {
            // We don't know about that node
            return new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0, 0, 0);
        }

        // Check if there are any shards currently on this node, and if there are any relocating shards
        int startedShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.STARTED);
        int relocatingShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.RELOCATING);
        int initializingShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.INITIALIZING);
        int totalRemainingShards = relocatingShards + startedShards + initializingShards;

        // If there's relocating shards, or no shards on this node, we'll just use the number of shards left to move
        if (totalRemainingShards == 0) {
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.COMPLETE,
                startedShards,
                relocatingShards,
                initializingShards
            );
        } else if (relocatingShards > 0) {
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                startedShards,
                relocatingShards,
                initializingShards
            );
        } else if (initializingShards > 0 && relocatingShards == 0 && startedShards == 0) {
            // If there's only initializing shards left, return now with a note that only initializing shards are left
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                startedShards,
                relocatingShards,
                initializingShards,
                "all remaining shards are currently INITIALIZING and must finish before they can be moved off this node"
            );
        }

        // Get all shard explanations
        var unmovableShards = currentState.getRoutingNodes()
            .node(nodeId)
            .shardsWithState(ShardRoutingState.STARTED)
            .peek(s -> cancellableTask.ensureNotCancelled())
            .map(shardRouting -> new Tuple<>(shardRouting, allocationService.explainShardAllocation(shardRouting, allocation)))
            // Given that we're checking the status of a node that's shutting down, no shards should be allowed to remain
            .filter(pair -> {
                assert pair.v2().getMoveDecision().canRemain() == false
                    : "shard [" + pair + "] can remain on node [" + nodeId + "], but that node is shutting down";
                return pair.v2().getMoveDecision().canRemain() == false;
            })
            // These shards will move as soon as possible
            .filter(pair -> pair.v2().getMoveDecision().getAllocationDecision().equals(AllocationDecision.YES) == false)
            .toList();

        // If there's no relocating shards and shards still on this node, we need to figure out why
        AtomicInteger shardsToIgnoreForFinalStatus = new AtomicInteger(0);

        // Find first one that can not move permanently
        var unmovableShard = unmovableShards.stream()
            .filter(pair -> pair.v2().getMoveDecision().getAllocationDecision().equals(AllocationDecision.THROTTLED) == false)
            // If the shard that can't move is on every node in the cluster, we shouldn't be `STALLED` on it.
            .filter(pair -> {
                final boolean hasShardCopyOnOtherNode = hasShardCopyOnAnotherNode(currentState, pair.v1(), shuttingDownNodes);
                if (hasShardCopyOnOtherNode) {
                    shardsToIgnoreForFinalStatus.incrementAndGet();
                }
                return hasShardCopyOnOtherNode == false;
            })
            // If ILM is shrinking the index this shard is part of, it'll look like it's unmovable, but we can just wait for ILM to finish
            .filter(pair -> isIlmRestrictingShardMovement(currentState.metadata().getProject(), pair.v1()) == false)
            .peek(
                pair -> logger.debug(
                    "node [{}] shutdown of type [{}] stalled: found shard [{}][{}] from index [{}] with negative decision: [{}]",
                    nodeId,
                    shutdownType,
                    pair.v1().getId(),
                    pair.v1().primary() ? "primary" : "replica",
                    pair.v1().shardId().getIndexName(),
                    Strings.toString(pair.v2())
                )
            )
            .findFirst();

        var temporarilyUnmovableShards = unmovableShards.stream()
            .filter(pair -> pair.v2().getMoveDecision().getAllocationDecision().equals(AllocationDecision.THROTTLED))
            .toList();

        if (totalRemainingShards == shardsToIgnoreForFinalStatus.get() && unmovableShard.isEmpty()) {
            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.COMPLETE,
                0,
                "["
                    + shardsToIgnoreForFinalStatus.get()
                    + "] shards cannot be moved away from this node but have at least one copy on another node in the cluster",
                null
            );
        } else if (unmovableShard.isPresent()) {
            // We found a shard that can't be moved, so shard relocation is stalled. Blame the unmovable shard.
            ShardRouting shardRouting = unmovableShard.get().v1();
            ShardAllocationDecision decision = unmovableShard.get().v2();

            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.STALLED,
                totalRemainingShards,
                format(
                    "shard [%s] [%s] of index [%s] cannot move, see [%s] for details or use the cluster allocation explain API",
                    shardRouting.shardId().getId(),
                    shardRouting.primary() ? "primary" : "replica",
                    shardRouting.index().getName(),
                    NODE_ALLOCATION_DECISION_KEY
                ),
                decision
            );
        } else if (relocatingShards == 0
            && initializingShards == 0
            && startedShards > 0
            && temporarilyUnmovableShards.size() == startedShards) {
                // We found a shard that can't be moved temporarily,
                // report it so that the cause of the throttling could be addressed if it is taking significant time
                ShardRouting shardRouting = temporarilyUnmovableShards.get(0).v1();
                ShardAllocationDecision decision = temporarilyUnmovableShards.get(0).v2();

                return new ShutdownShardMigrationStatus(
                    SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                    startedShards,
                    relocatingShards,
                    initializingShards,
                    format(
                        "shard [%s] [%s] of index [%s] is waiting to be moved, see [%s] "
                            + "for details or use the cluster allocation explain API",
                        shardRouting.shardId().getId(),
                        shardRouting.primary() ? "primary" : "replica",
                        shardRouting.index().getName(),
                        NODE_ALLOCATION_DECISION_KEY
                    ),
                    decision
                );
            } else {
                return new ShutdownShardMigrationStatus(
                    SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                    startedShards,
                    relocatingShards,
                    initializingShards
                );
            }
    }

    private static boolean isIlmRestrictingShardMovement(ProjectMetadata projectMetadata, ShardRouting pair) {
        if (OperationMode.STOPPED.equals(currentILMMode(projectMetadata)) == false) {
            LifecycleExecutionState ilmState = projectMetadata.index(pair.index()).getLifecycleExecutionState();
            // Specifically, if 1) ILM is running, 2) ILM is currently shrinking the index this shard is part of, and 3) it hasn't
            // errored out, we can disregard this shard under the assumption that ILM will get it movable eventually
            boolean ilmWillMoveShardEventually = ilmState != null
                && ShrinkAction.NAME.equals(ilmState.action())
                && ErrorStep.NAME.equals(ilmState.step()) == false;
            if (ilmWillMoveShardEventually) {
                logger.debug(
                    format(
                        "shard [%s] [%s] of index [%s] cannot move, but ILM is shrinking that index so assuming it will move",
                        pair.shardId().getId(),
                        pair.primary() ? "primary" : "replica",
                        pair.index().getName()
                    )
                );
            }
            return ilmWillMoveShardEventually;
        }
        return false;
    }

    private static boolean hasShardCopyOnAnotherNode(ClusterState clusterState, ShardRouting shardRouting, Set<String> shuttingDownNodes) {
        return clusterState.routingTable()
            .shardRoutingTable(shardRouting.shardId())
            .allShards()
            .filter(sr -> sr.role().equals(shardRouting.role()))
            // If any shards are both 1) `STARTED` and 2) are not on a node that's shutting down, we have at least one copy
            // of this shard safely on a node that's not shutting down, so we don't want to report `STALLED` because of this shard.
            .filter(ShardRouting::started)
            .anyMatch(routing -> shuttingDownNodes.contains(routing.currentNodeId()) == false);
    }

    @Override
    protected ClusterBlockException checkBlock(GetShutdownStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
