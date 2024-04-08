/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotsInfoService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 */
public class AllocationService {

    private static final Logger logger = LogManager.getLogger(AllocationService.class);

    private final AllocationDeciders allocationDeciders;
    private Map<String, ExistingShardsAllocator> existingShardsAllocators;
    private final ShardsAllocator shardsAllocator;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;

    // only for tests that use the GatewayAllocator as the unique ExistingShardsAllocator
    @SuppressWarnings("this-escape")
    public AllocationService(
        AllocationDeciders allocationDeciders,
        GatewayAllocator gatewayAllocator,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        this(allocationDeciders, shardsAllocator, clusterInfoService, snapshotsInfoService, shardRoutingRoleStrategy);
        setExistingShardsAllocators(Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator));
    }

    public AllocationService(
        AllocationDeciders allocationDeciders,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocator = shardsAllocator;
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
    }

    /**
     * Inject the {@link ExistingShardsAllocator}s to use. May only be called once.
     */
    public void setExistingShardsAllocators(Map<String, ExistingShardsAllocator> existingShardsAllocators) {
        assert this.existingShardsAllocators == null : "cannot set allocators " + existingShardsAllocators + " twice";
        assert existingShardsAllocators.isEmpty() == false : "must add at least one ExistingShardsAllocator";
        this.existingShardsAllocators = Collections.unmodifiableMap(existingShardsAllocators);
    }

    /**
     * @return The allocation deciders that the allocation service has been configured with.
     */
    public AllocationDeciders getAllocationDeciders() {
        return allocationDeciders;
    }

    public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
        return shardRoutingRoleStrategy;
    }

    /**
     * Applies the started shards. Note, only initializing ShardRouting instances that exist in the routing table should be
     * provided as parameter and no duplicates should be contained.
     * <p>
     * If the same instance of the {@link ClusterState} is returned, then no change has been made.</p>
     */
    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        assert assertInitialized();
        if (startedShards.isEmpty()) {
            return clusterState;
        }
        RoutingAllocation allocation = createRoutingAllocation(clusterState, currentNanoTime());
        // as starting a primary relocation target can reinitialize replica shards, start replicas first
        startedShards = new ArrayList<>(startedShards);
        startedShards.sort(Comparator.comparing(ShardRouting::primary));
        applyStartedShards(allocation, startedShards);
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyStartedShards(startedShards, allocation);
        }
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
        String startedShardsAsString = firstListElementsToCommaDelimitedString(
            startedShards,
            s -> s.shardId().toString(),
            logger.isDebugEnabled()
        );
        return buildResultAndLogHealthChange(clusterState, allocation, "shards started [" + startedShardsAsString + "]");
    }

    private static ClusterState buildResultAndLogHealthChange(ClusterState oldState, RoutingAllocation allocation, String reason) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = RoutingTable.of(oldRoutingTable.version(), newRoutingNodes);
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata);
        final RestoreInProgress restoreInProgress = RestoreInProgress.get(allocation.getClusterState());
        RestoreInProgress updatedRestoreInProgress = allocation.updateRestoreInfoWithRoutingChanges(restoreInProgress);
        if (updatedRestoreInProgress != restoreInProgress) {
            ImmutableOpenMap.Builder<String, ClusterState.Custom> customsBuilder = ImmutableOpenMap.builder(
                allocation.getClusterState().getCustoms()
            );
            customsBuilder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            newStateBuilder.customs(customsBuilder.build());
        }
        final ClusterState newState = newStateBuilder.build();

        logClusterHealthStateChange(oldState, newState, reason);

        return newState;
    }

    /**
     * Applies the failed shards. Note, only assigned ShardRouting instances that exist in the routing table should be
     * provided as parameter. Also applies a list of allocation ids to remove from the in-sync set for shard copies for which there
     * are no routing entries in the routing table.
     *
     * <p>
     * If the same instance of ClusterState is returned, then no change has been made.</p>
     */
    public ClusterState applyFailedShards(
        final ClusterState clusterState,
        final List<FailedShard> failedShards,
        final List<StaleShard> staleShards
    ) {
        assert assertInitialized();
        if (staleShards.isEmpty() && failedShards.isEmpty()) {
            return clusterState;
        }
        ClusterState tmpState = IndexMetadataUpdater.removeStaleIdsWithoutRoutings(clusterState, staleShards, logger);

        long currentNanoTime = currentNanoTime();
        RoutingAllocation allocation = createRoutingAllocation(tmpState, currentNanoTime);

        for (FailedShard failedShardEntry : failedShards) {
            ShardRouting shardToFail = failedShardEntry.routingEntry();
            assert allocation.metadata().hasIndex(shardToFail.shardId().getIndex());
            allocation.addIgnoreShardForNode(shardToFail.shardId(), shardToFail.currentNodeId());
            // failing a primary also fails initializing replica shards, re-resolve ShardRouting
            ShardRouting failedShard = allocation.routingNodes()
                .getByAllocationId(shardToFail.shardId(), shardToFail.allocationId().getId());
            if (failedShard != null) {
                if (failedShard != shardToFail) {
                    logger.trace(
                        "{} shard routing modified in an earlier iteration (previous: {}, current: {})",
                        shardToFail.shardId(),
                        shardToFail,
                        failedShard
                    );
                }
                int failedAllocations = failedShard.unassignedInfo() != null ? failedShard.unassignedInfo().getNumFailedAllocations() : 0;
                final Set<String> failedNodeIds;
                if (failedShard.unassignedInfo() != null) {
                    failedNodeIds = Sets.newHashSetWithExpectedSize(failedShard.unassignedInfo().getFailedNodeIds().size() + 1);
                    failedNodeIds.addAll(failedShard.unassignedInfo().getFailedNodeIds());
                    failedNodeIds.add(failedShard.currentNodeId());
                } else {
                    failedNodeIds = Collections.emptySet();
                }
                String message = "failed shard on node [" + shardToFail.currentNodeId() + "]: " + failedShardEntry.message();
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.ALLOCATION_FAILED,
                    message,
                    failedShardEntry.failure(),
                    failedAllocations + 1,
                    currentNanoTime,
                    System.currentTimeMillis(),
                    false,
                    AllocationStatus.NO_ATTEMPT,
                    failedNodeIds,
                    shardToFail.currentNodeId()
                );
                if (failedShardEntry.markAsStale()) {
                    allocation.removeAllocationId(failedShard);
                }
                logger.warn(() -> "failing shard [" + failedShardEntry + "]", failedShardEntry.failure());
                allocation.routingNodes().failShard(logger, failedShard, unassignedInfo, allocation.changes());
            } else {
                logger.trace("{} shard routing failed in an earlier iteration (routing: {})", shardToFail.shardId(), shardToFail);
            }
        }
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyFailedShards(failedShards, allocation);
        }

        reroute(
            allocation,
            routingAllocation -> shardsAllocator.allocate(
                routingAllocation,
                rerouteCompletionIsNotRequired() /* this is not triggered by a user request */
            )
        );

        String failedShardsAsString = firstListElementsToCommaDelimitedString(
            failedShards,
            s -> s.routingEntry().shardId().toString(),
            logger.isDebugEnabled()
        );
        return buildResultAndLogHealthChange(clusterState, allocation, "shards failed [" + failedShardsAsString + "]");
    }

    /**
     * unassigned an shards that are associated with nodes that are no longer part of the cluster, potentially promoting replicas
     * if needed.
     */
    public ClusterState disassociateDeadNodes(ClusterState clusterState, boolean reroute, String reason) {
        RoutingAllocation allocation = createRoutingAllocation(clusterState, currentNanoTime());

        // first, clear from the shards any node id they used to belong to that is now dead
        disassociateDeadNodes(allocation);

        if (allocation.routingNodesChanged()) {
            clusterState = buildResultAndLogHealthChange(clusterState, allocation, reason);
        }
        if (reroute) {
            return reroute(clusterState, reason, rerouteCompletionIsNotRequired());// this is not triggered by a user request
        } else {
            return clusterState;
        }
    }

    /**
     * Checks if there are replicas with the auto-expand feature that need to be adapted.
     * Returns an updated cluster state if changes were necessary, or the identical cluster if no changes were required.
     */
    public ClusterState adaptAutoExpandReplicas(ClusterState clusterState) {
        final Supplier<RoutingAllocation> allocationSupplier = () -> new RoutingAllocation(
            allocationDeciders,
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );
        final Map<Integer, List<String>> autoExpandReplicaChanges = AutoExpandReplicas.getAutoExpandReplicaChanges(
            clusterState.metadata(),
            allocationSupplier
        );
        if (autoExpandReplicaChanges.isEmpty()) {
            return clusterState;
        } else {
            final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(shardRoutingRoleStrategy, clusterState.routingTable());
            final Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            for (Map.Entry<Integer, List<String>> entry : autoExpandReplicaChanges.entrySet()) {
                final int numberOfReplicas = entry.getKey();
                final String[] indices = entry.getValue().toArray(Strings.EMPTY_ARRAY);
                // we do *not* update the in sync allocation ids as they will be removed upon the first index
                // operation which make these copies stale
                routingTableBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                metadataBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                // update settings version for each index
                for (final String index : indices) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index);
                    final IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexMetadata).settingsVersion(
                        1 + indexMetadata.getSettingsVersion()
                    );
                    metadataBuilder.put(indexMetadataBuilder);
                }
                logger.info("updating number_of_replicas to [{}] for indices {}", numberOfReplicas, indices);
            }
            final ClusterState fixedState = ClusterState.builder(clusterState)
                .routingTable(routingTableBuilder.build())
                .metadata(metadataBuilder)
                .build();
            assert AutoExpandReplicas.getAutoExpandReplicaChanges(fixedState.metadata(), allocationSupplier).isEmpty();
            return fixedState;
        }
    }

    /**
     * Internal helper to cap the number of elements in a potentially long list for logging.
     *
     * @param elements  The elements to log. May be any non-null list. Must not be null.
     * @param formatter A function that can convert list elements to a String. Must not be null.
     * @param <T>       The list element type.
     * @return A comma-separated string of the first few elements.
     */
    public static <T> String firstListElementsToCommaDelimitedString(
        List<T> elements,
        Function<T, String> formatter,
        boolean isDebugEnabled
    ) {
        final int maxNumberOfElements = 10;
        if (isDebugEnabled || elements.size() <= maxNumberOfElements) {
            return elements.stream().map(formatter).collect(Collectors.joining(", "));
        } else {
            return elements.stream().limit(maxNumberOfElements).map(formatter).collect(Collectors.joining(", "))
                + ", ... ["
                + elements.size()
                + " items in total]";
        }
    }

    public CommandsResult reroute(
        ClusterState clusterState,
        AllocationCommands commands,
        boolean explain,
        boolean retryFailed,
        boolean dryRun,
        ActionListener<Void> reroute
    ) {
        RoutingAllocation allocation = createRoutingAllocation(clusterState, currentNanoTime());
        var explanations = shardsAllocator.execute(allocation, commands, explain, retryFailed);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        if (dryRun == false) {
            reroute(allocation, routingAllocation -> shardsAllocator.allocate(routingAllocation, reroute));
        } else {
            reroute.onResponse(null);
        }
        return new CommandsResult(explanations, buildResultAndLogHealthChange(clusterState, allocation, "reroute commands"));
    }

    /**
     * Computes the next step towards a fully allocated and balanced cluster and records this step in the routing table of the returned
     * state. Should be called after every change to the cluster that affects the routing table and/or the balance of shards.
     * <p>
     * This method is expensive in larger clusters. Wherever possible you should invoke this method asynchronously using
     * {@link RerouteService#reroute} to batch up invocations rather than calling the method directly.
     *
     * @return an updated cluster state, or the same instance that was passed as an argument if no changes were made.
     */
    public ClusterState reroute(ClusterState clusterState, String reason, ActionListener<Void> listener) {
        return executeWithRoutingAllocation(
            clusterState,
            reason,
            routingAllocation -> shardsAllocator.allocate(routingAllocation, listener)
        );
    }

    /**
     * Computes the next step towards a fully allocated and balanced cluster and records this step in the routing table of the returned
     * state. Should be called after every change to the cluster that affects the routing table and/or the balance of shards.
     * <p>
     * This method is expensive in larger clusters. Wherever possible you should invoke this method asynchronously using
     * {@link RerouteService#reroute} to batch up invocations rather than calling the method directly.
     *
     * @return an updated cluster state, or the same instance that was passed as an argument if no changes were made.
     */
    public ClusterState executeWithRoutingAllocation(ClusterState clusterState, String reason, RerouteStrategy rerouteStrategy) {
        ClusterState fixedClusterState = adaptAutoExpandReplicas(clusterState);
        RoutingAllocation allocation = createRoutingAllocation(fixedClusterState, currentNanoTime());
        reroute(allocation, rerouteStrategy);
        if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
            return clusterState;
        }
        return buildResultAndLogHealthChange(clusterState, allocation, reason);
    }

    @FunctionalInterface
    public interface RerouteStrategy {

        /**
         * Removes delay markers from unassigned shards based on current time stamp.
         */
        default void removeDelayMarkers(RoutingAllocation allocation) {
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
            final Metadata metadata = allocation.metadata();
            while (unassignedIterator.hasNext()) {
                ShardRouting shardRouting = unassignedIterator.next();
                UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
                if (unassignedInfo.isDelayed()) {
                    final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(
                        allocation.getCurrentNanoTime(),
                        metadata.getIndexSafe(shardRouting.index()).getSettings(),
                        metadata.nodeShutdowns()
                    );
                    if (newComputedLeftDelayNanos == 0) {
                        unassignedIterator.updateUnassigned(
                            new UnassignedInfo(
                                unassignedInfo.getReason(),
                                unassignedInfo.getMessage(),
                                unassignedInfo.getFailure(),
                                unassignedInfo.getNumFailedAllocations(),
                                unassignedInfo.getUnassignedTimeInNanos(),
                                unassignedInfo.getUnassignedTimeInMillis(),
                                false,
                                unassignedInfo.getLastAllocationStatus(),
                                unassignedInfo.getFailedNodeIds(),
                                unassignedInfo.getLastAllocatedNodeId()
                            ),
                            shardRouting.recoverySource(),
                            allocation.changes()
                        );
                    }
                }
            }
        }

        /**
         * Generic action to be executed on preconfigured allocation
         */
        void execute(RoutingAllocation allocation);
    }

    private static void logClusterHealthStateChange(final ClusterState previousState, final ClusterState newState, String reason) {
        ClusterHealthStatus previousHealth = getHealthStatus(previousState);
        ClusterHealthStatus currentHealth = getHealthStatus(newState);

        if (previousHealth.equals(currentHealth) == false) {
            logger.info(
                new ESLogMessage("Cluster health status changed from [{}] to [{}] (reason: [{}]).").argAndField(
                    "previous.health",
                    previousHealth
                ).argAndField("current.health", currentHealth).argAndField("reason", reason)
            );

        }
    }

    public static ClusterHealthStatus getHealthStatus(final ClusterState clusterState) {
        if (clusterState.blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
            return ClusterHealthStatus.RED;
        }

        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        for (String index : clusterState.metadata().getConcreteAllIndices()) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            if (indexRoutingTable == null) {
                continue;
            }
            if (indexRoutingTable.allShardsActive()) {
                // GREEN index
                continue;
            }

            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(i);
                ShardRouting primary = indexShardRoutingTable.primaryShard();
                if (primary.active()) {
                    // index has inactive replicas
                    computeStatus = ClusterHealthStatus.YELLOW;
                    continue;
                }
                computeStatus = getInactivePrimaryHealth(primary);
                if (computeStatus == ClusterHealthStatus.RED) {
                    logger.debug("One of inactive primary shard {} causes cluster state RED.", primary.shardId());
                    return ClusterHealthStatus.RED;
                }
            }
        }
        return computeStatus;
    }

    private static boolean hasDeadNodes(RoutingAllocation allocation) {
        for (RoutingNode routingNode : allocation.routingNodes()) {
            if (allocation.nodes().getDataNodes().containsKey(routingNode.nodeId()) == false) {
                return true;
            }
        }
        return false;
    }

    private void reroute(RoutingAllocation allocation, RerouteStrategy rerouteStrategy) {
        assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See disassociateDeadNodes";
        assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metadata(), () -> allocation).isEmpty()
            : "auto-expand replicas out of sync with number of nodes in the cluster";
        assert assertInitialized();
        rerouteStrategy.removeDelayMarkers(allocation);
        allocateExistingUnassignedShards(allocation); // try to allocate existing shard copies first
        rerouteStrategy.execute(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
    }

    private void allocateExistingUnassignedShards(RoutingAllocation allocation) {
        allocation.routingNodes().unassigned().sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.beforeAllocation(allocation);
        }

        final RoutingNodes.UnassignedShards.UnassignedIterator primaryIterator = allocation.routingNodes().unassigned().iterator();
        while (primaryIterator.hasNext()) {
            final ShardRouting shardRouting = primaryIterator.next();
            if (shardRouting.primary()) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, primaryIterator);
            }
        }

        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.afterPrimariesBeforeReplicas(
                allocation,
                shardRouting -> getAllocatorForShard(shardRouting, allocation) == existingShardsAllocator
            );
        }

        final RoutingNodes.UnassignedShards.UnassignedIterator replicaIterator = allocation.routingNodes().unassigned().iterator();
        while (replicaIterator.hasNext()) {
            final ShardRouting shardRouting = replicaIterator.next();
            if (shardRouting.primary() == false) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, replicaIterator);
            }
        }
    }

    private static void disassociateDeadNodes(RoutingAllocation allocation) {
        for (Iterator<RoutingNode> it = allocation.routingNodes().mutableIterator(); it.hasNext();) {
            RoutingNode node = it.next();
            if (allocation.nodes().getDataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }

            var nodeShutdownMetadata = allocation.metadata().nodeShutdowns().get(node.nodeId(), Type.RESTART);
            var unassignedReason = nodeShutdownMetadata != null ? UnassignedInfo.Reason.NODE_RESTARTING : UnassignedInfo.Reason.NODE_LEFT;
            boolean delayedDueToKnownRestart = nodeShutdownMetadata != null && nodeShutdownMetadata.getAllocationDelay().nanos() > 0;

            // now, go over all the shards routing on the node, and fail them
            for (ShardRouting shardRouting : node.copyShards()) {
                final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
                boolean delayed = delayedDueToKnownRestart
                    || INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetadata.getSettings()).nanos() > 0;

                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    unassignedReason,
                    "node_left [" + node.nodeId() + "]",
                    null,
                    0,
                    allocation.getCurrentNanoTime(),
                    System.currentTimeMillis(),
                    delayed,
                    AllocationStatus.NO_ATTEMPT,
                    Collections.emptySet(),
                    shardRouting.currentNodeId()
                );
                allocation.routingNodes().failShard(logger, shardRouting, unassignedInfo, allocation.changes());
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
    }

    private static void applyStartedShards(RoutingAllocation routingAllocation, List<ShardRouting> startedShardEntries) {
        assert startedShardEntries.isEmpty() == false : "non-empty list of started shard entries expected";
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.initializing() : "only initializing shards can be started";
            assert routingAllocation.metadata().index(startedShard.shardId().getIndex()) != null
                : "shard started for unknown index (shard entry: " + startedShard + ")";
            assert startedShard == routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId())
                : "shard routing to start does not exist in routing table, expected: "
                    + startedShard
                    + " but was: "
                    + routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId());
            long expectedShardSize = routingAllocation.metadata().getIndexSafe(startedShard.index()).isSearchableSnapshot()
                ? startedShard.getExpectedShardSize()
                : ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
            routingNodes.startShard(logger, startedShard, routingAllocation.changes(), expectedShardSize);
        }
    }

    private RoutingAllocation createRoutingAllocation(ClusterState clusterState, long currentNanoTime) {
        return new RoutingAllocation(
            allocationDeciders,
            clusterState.mutableRoutingNodes(),
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime
        );
    }

    /** override this to control time based decisions during allocation */
    protected long currentNanoTime() {
        return System.nanoTime();
    }

    public void cleanCaches() {
        assert assertInitialized();
        existingShardsAllocators.values().forEach(ExistingShardsAllocator::cleanCaches);
    }

    public int getNumberOfInFlightFetches() {
        assert assertInitialized();
        return existingShardsAllocators.values().stream().mapToInt(ExistingShardsAllocator::getNumberOfInFlightFetches).sum();
    }

    public ShardAllocationDecision explainShardAllocation(ShardRouting shardRouting, RoutingAllocation allocation) {
        assert allocation.debugDecision();
        AllocateUnassignedDecision allocateDecision = shardRouting.unassigned()
            ? explainUnassignedShardAllocation(shardRouting, allocation)
            : AllocateUnassignedDecision.NOT_TAKEN;
        if (allocateDecision.isDecisionTaken()) {
            return new ShardAllocationDecision(allocateDecision, MoveDecision.NOT_TAKEN);
        } else {
            return shardsAllocator.decideShardAllocation(shardRouting, allocation);
        }
    }

    private AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        assert assertInitialized();
        final ExistingShardsAllocator existingShardsAllocator = getAllocatorForShard(shardRouting, routingAllocation);
        final AllocateUnassignedDecision decision = existingShardsAllocator.explainUnassignedShardAllocation(
            shardRouting,
            routingAllocation
        );
        if (decision.isDecisionTaken()) {
            return decision;
        }
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    private ExistingShardsAllocator getAllocatorForShard(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert assertInitialized();
        final String allocatorName = ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            routingAllocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
        );
        final ExistingShardsAllocator existingShardsAllocator = existingShardsAllocators.get(allocatorName);
        return existingShardsAllocator != null ? existingShardsAllocator : new NotFoundAllocator(allocatorName);
    }

    private boolean assertInitialized() {
        assert existingShardsAllocators != null : "must have set allocators first";
        return true;
    }

    // exposed for tests whose behaviour depends on this
    boolean isBalancedShardsAllocator() {
        return shardsAllocator instanceof BalancedShardsAllocator;
    }

    private static class NotFoundAllocator implements ExistingShardsAllocator {
        private final String allocatorName;

        private NotFoundAllocator(String allocatorName) {
            this.allocatorName = allocatorName;
        }

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {}

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            unassignedAllocationHandler.removeAndIgnore(AllocationStatus.NO_VALID_SHARD_COPY, allocation.changes());
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation allocation) {
            assert unassignedShard.unassigned();
            assert allocation.debugDecision();
            final List<NodeAllocationResult> nodeAllocationResults = new ArrayList<>(allocation.nodes().getSize());
            for (DiscoveryNode discoveryNode : allocation.nodes()) {
                nodeAllocationResults.add(
                    new NodeAllocationResult(
                        discoveryNode,
                        null,
                        allocation.decision(
                            Decision.NO,
                            "allocator_plugin",
                            "finding the previous copies of this shard requires an allocator called [%s] but "
                                + "that allocator was not found; perhaps the corresponding plugin is not installed",
                            allocatorName
                        )
                    )
                );
            }
            return AllocateUnassignedDecision.no(AllocationStatus.NO_VALID_SHARD_COPY, nodeAllocationResults);
        }

        @Override
        public void cleanCaches() {}

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    /**
     * this class is used to describe results of applying a set of
     * {@link org.elasticsearch.cluster.routing.allocation.command.AllocationCommand}
     */
    public record CommandsResult(
        /**
         * Explanation for the reroute actions
         */
        RoutingExplanations explanations,
        /**
         * Resulting cluster state, to be removed when REST compatibility with
         * {@link org.elasticsearch.Version#V_8_6_0} / {@link RestApiVersion#V_8} no longer needed
         */
        ClusterState clusterState
    ) {}
}
