/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.REPLACE;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;

/**
 * Given the current allocation of shards and the desired balance, performs the next (legal) shard movements towards the goal.
 */
public class DesiredBalanceReconciler {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceReconciler.class);

    /**
     * The minimum interval that log messages will be written if the number of undesired shard allocations reaches the percentage of total
     * shards set by {@link #UNDESIRED_ALLOCATIONS_LOG_THRESHOLD_SETTING}.
     */
    public static final Setting<TimeValue> UNDESIRED_ALLOCATIONS_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.undesired_allocations.log_interval",
        TimeValue.timeValueHours(1),
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Warning log messages may be periodically written if the number of shards that are on undesired nodes reaches this percentage setting.
     * Works together with {@link #UNDESIRED_ALLOCATIONS_LOG_INTERVAL_SETTING} to log on a periodic basis.
     */
    public static final Setting<Double> UNDESIRED_ALLOCATIONS_LOG_THRESHOLD_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.desired_balance.undesired_allocations.threshold",
        0.1,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final FrequencyCappedAction undesiredAllocationLogInterval;
    private double undesiredAllocationsLogThreshold;
    private final NodeAllocationOrdering allocationOrdering = new NodeAllocationOrdering();
    private final NodeAllocationOrdering moveOrdering = new NodeAllocationOrdering();

    public DesiredBalanceReconciler(ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.undesiredAllocationLogInterval = new FrequencyCappedAction(
            threadPool.relativeTimeInMillisSupplier(),
            TimeValue.timeValueMinutes(5)
        );
        clusterSettings.initializeAndWatch(UNDESIRED_ALLOCATIONS_LOG_INTERVAL_SETTING, this.undesiredAllocationLogInterval::setMinInterval);
        clusterSettings.initializeAndWatch(
            UNDESIRED_ALLOCATIONS_LOG_THRESHOLD_SETTING,
            value -> this.undesiredAllocationsLogThreshold = value
        );
    }

    /**
     * Applies a desired shard allocation to the routing table by initializing and relocating shards in the cluster state.
     *
     * @param desiredBalance The new desired cluster shard allocation
     * @param allocation Cluster state information with which to make decisions, contains routing table metadata that will be modified to
     *                   reach the given desired balance.
     * @return {@link DesiredBalanceMetrics.AllocationStats} for this round of reconciliation changes.
     */
    public DesiredBalanceMetrics.AllocationStats reconcile(DesiredBalance desiredBalance, RoutingAllocation allocation) {
        var nodeIds = allocation.routingNodes().getAllNodeIds();
        allocationOrdering.retainNodes(nodeIds);
        moveOrdering.retainNodes(nodeIds);
        return new Reconciliation(desiredBalance, allocation).run();
    }

    public void clear() {
        allocationOrdering.clear();
        moveOrdering.clear();
    }

    /**
     * Handles updating the {@code RoutingNodes} to reflect the next steps towards the new {@code DesiredBalance}. Updates are limited by
     * throttling (there are limits on the number of concurrent shard moves) or resource constraints (some shard moves might not be
     * immediately possible until other shards move first).
     */
    private class Reconciliation {

        private final DesiredBalance desiredBalance;
        private final RoutingAllocation allocation;
        private final RoutingNodes routingNodes;

        Reconciliation(DesiredBalance desiredBalance, RoutingAllocation allocation) {
            this.desiredBalance = desiredBalance;
            this.allocation = allocation;
            this.routingNodes = allocation.routingNodes();
        }

        DesiredBalanceMetrics.AllocationStats run() {
            try (var ignored = allocation.withReconcilingFlag()) {

                logger.debug("Reconciling desired balance for [{}]", desiredBalance.lastConvergedIndex());

                if (routingNodes.size() == 0) {
                    // no data nodes, so fail allocation to report red health
                    failAllocationOfNewPrimaries(allocation);
                    logger.trace("no nodes available, nothing to reconcile");
                    return DesiredBalanceMetrics.EMPTY_ALLOCATION_STATS;
                }

                if (desiredBalance.assignments().isEmpty()) {
                    // no desired state yet but it is on its way and we'll reroute again when it is ready
                    logger.trace("desired balance is empty, nothing to reconcile");
                    return DesiredBalanceMetrics.EMPTY_ALLOCATION_STATS;
                }

                // compute next moves towards current desired balance:

                // 1. allocate unassigned shards first
                logger.trace("Reconciler#allocateUnassigned");
                allocateUnassigned();
                assert allocateUnassignedInvariant();

                // 2. move any shards that cannot remain where they are
                logger.trace("Reconciler#moveShards");
                moveShards();

                // 3. move any other shards that are desired elsewhere
                // This is the rebalancing work. The previous calls were necessary, to assign unassigned shard copies, and move shards that
                // violate resource thresholds. Now we run moves to improve the relative node resource loads.
                logger.trace("Reconciler#balance");
                DesiredBalanceMetrics.AllocationStats allocationStats = balance();

                logger.debug("Reconciliation is complete");
                return allocationStats;
            }
        }

        /**
         * Checks whether every shard is either assigned or ignored. Expected to be called after {@link #allocateUnassigned()}.
         */
        private boolean allocateUnassignedInvariant() {
            assert routingNodes.unassigned().isEmpty();

            final var shardCounts = allocation.metadata()
                .projects()
                .values()
                .stream()
                .flatMap(ProjectMetadata::stream)
                .filter(indexMetadata ->
                // skip any pre-7.2 closed indices which have no routing table entries at all
                indexMetadata.getCreationVersion().onOrAfter(IndexVersions.V_7_2_0)
                    || indexMetadata.getState() == IndexMetadata.State.OPEN
                    || MetadataIndexStateService.isIndexVerifiedBeforeClosed(indexMetadata))
                .flatMap(
                    indexMetadata -> IntStream.range(0, indexMetadata.getNumberOfShards())
                        .mapToObj(
                            shardId -> Tuple.tuple(new ShardId(indexMetadata.getIndex(), shardId), indexMetadata.getNumberOfReplicas() + 1)
                        )
                )
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

            for (final var shardRouting : routingNodes.unassigned().ignored()) {
                shardCounts.computeIfPresent(shardRouting.shardId(), (ignored, count) -> count == 1 ? null : count - 1);
            }

            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    shardCounts.computeIfPresent(shardRouting.shardId(), (ignored, count) -> count == 1 ? null : count - 1);
                }
            }

            assert shardCounts.isEmpty() : shardCounts;

            return true;
        }

        private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
            RoutingNodes routingNodes = allocation.routingNodes();
            assert routingNodes.size() == 0 : routingNodes;
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
            while (unassignedIterator.hasNext()) {
                final ShardRouting shardRouting = unassignedIterator.next();
                final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
                if (shardRouting.primary() && unassignedInfo.lastAllocationStatus() == AllocationStatus.NO_ATTEMPT) {
                    unassignedIterator.updateUnassigned(
                        new UnassignedInfo(
                            unassignedInfo.reason(),
                            unassignedInfo.message(),
                            unassignedInfo.failure(),
                            unassignedInfo.failedAllocations(),
                            unassignedInfo.unassignedTimeNanos(),
                            unassignedInfo.unassignedTimeMillis(),
                            unassignedInfo.delayed(),
                            AllocationStatus.DECIDERS_NO,
                            unassignedInfo.failedNodeIds(),
                            unassignedInfo.lastAllocatedNodeId()
                        ),
                        shardRouting.recoverySource(),
                        allocation.changes()
                    );
                }
            }
        }

        private void allocateUnassigned() {
            RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            if (logger.isTraceEnabled()) {
                logger.trace("Start allocating unassigned shards: {}", routingNodes.toString());
            }
            if (unassigned.isEmpty()) {
                return;
            }

            /*
             * Create some comparators to sort the unassigned shard copies in priority to allocate order.
             * TODO: We could be smarter here and group the shards by index and then
             * use the sorter to save some iterations.
             */
            final PriorityComparator indexPriorityComparator = PriorityComparator.getAllocationComparator(allocation);
            final Comparator<ShardRouting> shardAllocationPriorityComparator = (o1, o2) -> {
                // Prioritize assigning a primary shard copy, if one is a primary and the other is not.
                if (o1.primary() ^ o2.primary()) {
                    return o1.primary() ? -1 : 1;
                }

                // Then order shards in the same index arbitrarily by shard ID.
                if (o1.getIndexName().compareTo(o2.getIndexName()) == 0) {
                    return o1.getId() - o2.getId();
                }

                // Lastly, prioritize system indices, then use index priority of non-system indices, then by age, etc.
                //
                // this comparator is more expensive than all the others up there
                // that's why it's added last even though it could be easier to read
                // if we'd apply it earlier. this comparator will only differentiate across
                // indices all shards of the same index is treated equally.
                final int secondaryComparison = indexPriorityComparator.compare(o1, o2);
                assert secondaryComparison != 0 : "Index names are equal, should be returned early.";
                return secondaryComparison;
            };

            /*
             * we use 2 arrays and move replicas to the second array once we allocated an identical
             * replica in the current iteration to make sure all indices get allocated in the same manner.
             * The arrays are sorted by primaries first and then by index and shard ID so 2 indices with
             * 2 replica and 1 shard would look like:
             * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
             * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
             * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
             */
            ShardRouting[] orderedShardAllocationList = unassigned.drain();
            ShardRouting[] deferredShardAllocationList = new ShardRouting[orderedShardAllocationList.length];
            int deferredShardAllocationListLength = 0;
            int orderedShardAllocationListLength = orderedShardAllocationList.length;
            ArrayUtil.timSort(orderedShardAllocationList, shardAllocationPriorityComparator);

            do {
                nextShard: for (int i = 0; i < orderedShardAllocationListLength; i++) {
                    final var shard = orderedShardAllocationList[i];
                    final var assignment = desiredBalance.getAssignment(shard.shardId());
                    // An ignored shard copy is one that has no desired balance assignment.
                    final boolean ignored = assignment == null || isIgnored(routingNodes, shard, assignment);

                    AllocationStatus unallocatedStatus;
                    if (ignored) {
                        unallocatedStatus = AllocationStatus.NO_ATTEMPT;
                    } else {
                        unallocatedStatus = AllocationStatus.DECIDERS_NO;
                        final var nodeIdsIterator = new NodeIdsIterator(shard, assignment, routingNodes);
                        while (nodeIdsIterator.hasNext()) {
                            final var nodeId = nodeIdsIterator.next();
                            final var routingNode = routingNodes.node(nodeId);
                            if (routingNode == null) {
                                // desired node no longer exists
                                continue;
                            }
                            if (routingNode.getByShardId(shard.shardId()) != null) {
                                // node already contains same shard.
                                // Skipping it allows us to exclude NO decisions from SameShardAllocationDecider and only log more relevant
                                // NO or THROTTLE decisions of the preventing shard from starting on assigned node
                                continue;
                            }
                            final var decision = allocation.deciders().canAllocate(shard, routingNode, allocation);
                            switch (decision.type()) {
                                case YES -> {
                                    logger.debug("Assigning shard [{}] to {} [{}]", shard, nodeIdsIterator.source, nodeId);
                                    long shardSize = getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation);
                                    routingNodes.initializeShard(shard, nodeId, null, shardSize, allocation.changes());
                                    allocationOrdering.recordAllocation(nodeId);
                                    if (shard.primary() == false) {
                                        // copy over the same replica shards to the secondary array so they will get allocated
                                        // in a subsequent iteration, allowing replicas of other shards to be allocated first
                                        while (i < orderedShardAllocationListLength - 1
                                            && shardAllocationPriorityComparator.compare(
                                                orderedShardAllocationList[i],
                                                orderedShardAllocationList[i + 1]
                                            ) == 0) {
                                            deferredShardAllocationList[deferredShardAllocationListLength++] =
                                                orderedShardAllocationList[++i];
                                        }
                                    }
                                    continue nextShard;
                                }
                                case THROTTLE -> {
                                    nodeIdsIterator.wasThrottled = true;
                                    unallocatedStatus = AllocationStatus.DECIDERS_THROTTLED;
                                    logger.debug("Couldn't assign shard [{}] to [{}]: {}", shard.shardId(), nodeId, decision);
                                }
                                case NO -> {
                                    logger.debug("Couldn't assign shard [{}] to [{}]: {}", shard.shardId(), nodeId, decision);
                                }
                            }
                        }
                    }

                    logger.debug("No eligible node found to assign shard [{}]", shard);
                    unassigned.ignoreShard(shard, unallocatedStatus, allocation.changes());
                    if (shard.primary() == false) {
                        // We could not allocate the shard copy and the copy is a replica: check if we can ignore the other unassigned
                        // replicas.
                        while (i < orderedShardAllocationListLength - 1
                            && shardAllocationPriorityComparator.compare(
                                orderedShardAllocationList[i],
                                orderedShardAllocationList[i + 1]
                            ) == 0) {
                            unassigned.ignoreShard(orderedShardAllocationList[++i], unallocatedStatus, allocation.changes());
                        }
                    }
                }
                ShardRouting[] tmp = orderedShardAllocationList;
                orderedShardAllocationList = deferredShardAllocationList;
                deferredShardAllocationList = tmp;
                orderedShardAllocationListLength = deferredShardAllocationListLength;
                deferredShardAllocationListLength = 0;
            } while (orderedShardAllocationListLength > 0);
        }

        private final class NodeIdsIterator implements Iterator<String> {

            private final ShardRouting shard;
            private final RoutingNodes routingNodes;
            /**
             * Contains the source of the nodeIds used for shard assignment.
             */
            private NodeIdSource source;
            private Iterator<String> nodeIds;

            private boolean wasThrottled = false;

            NodeIdsIterator(ShardRouting shard, ShardAssignment assignment, RoutingNodes routingNodes) {
                this.shard = shard;
                this.routingNodes = routingNodes;

                var forcedInitialAllocation = allocation.deciders().getForcedInitialShardAllocationToNodes(shard, allocation);
                if (forcedInitialAllocation.isPresent()) {
                    logger.debug("Shard [{}] initial allocation is forced to {}", shard.shardId(), forcedInitialAllocation.get());
                    nodeIds = allocationOrdering.sort(forcedInitialAllocation.get()).iterator();
                    source = NodeIdSource.FORCED_INITIAL_ALLOCATION;
                } else {
                    nodeIds = allocationOrdering.sort(assignment.nodeIds()).iterator();
                    source = NodeIdSource.DESIRED;
                }
            }

            @Override
            public boolean hasNext() {
                if (nodeIds.hasNext() == false
                    && source == NodeIdSource.DESIRED
                    && wasThrottled == false
                    && useFallback(shard, routingNodes)) {
                    var fallbackNodeIds = allocation.routingNodes().getAllNodeIds();
                    logger.debug("Shard [{}] assignment is temporarily not possible. Falling back to {}", shard.shardId(), fallbackNodeIds);
                    nodeIds = allocationOrdering.sort(fallbackNodeIds).iterator();
                    source = NodeIdSource.FALLBACK;
                }
                return nodeIds.hasNext();
            }

            private boolean useFallback(ShardRouting shard, RoutingNodes routingNodes) {
                if (shard.primary()) {
                    return true;
                }
                if (shard.role().equals(ShardRouting.Role.SEARCH_ONLY)) {
                    // Allow only one search shard to fall back to allocation to an undesired node
                    return routingNodes.assignedShards(shard.shardId())
                        .stream()
                        .noneMatch(s -> s.role().equals(ShardRouting.Role.SEARCH_ONLY));
                }
                return false;
            }

            @Override
            public String next() {
                return nodeIds.next();
            }
        }

        private enum NodeIdSource {
            // Using desired nodes.
            DESIRED,
            // Initial allocation is forced to certain nodes by shrink/split/clone index operation.
            FORCED_INITIAL_ALLOCATION,
            // Assigning the primary shard is temporarily not possible on desired nodes, and it is assigned elsewhere in the cluster.
            FALLBACK;
        }

        /**
         * Checks whether the {@code shard} copy has been assigned to a node or not in {@code assignment}.
         * @param routingNodes The current routing information
         * @param shard A particular shard copy
         * @param assignment The assignments for shard primary and replica copies
         * @return Whether the shard has a node assignment.
         */
        private boolean isIgnored(RoutingNodes routingNodes, ShardRouting shard, ShardAssignment assignment) {
            if (assignment.ignored() == 0) {
                // no shards are ignored
                return false;
            }
            if (assignment.ignored() == assignment.total()) {
                // all shards are ignored
                return true;
            }
            if (assignment.total() - assignment.ignored() == 1) {
                // all shard copies except primary are ignored
                return shard.primary() == false;
            }
            // only some of the replicas might be ignored
            // please note: it is not safe to use routing table here as it is not updated with changes from routing nodes yet
            int assigned = 0;
            for (RoutingNode routingNode : routingNodes) {
                var assignedShard = routingNode.getByShardId(shard.shardId());
                if (assignedShard != null && assignedShard.relocating() == false) {
                    assigned++;
                }
            }
            return assignment.total() - assignment.ignored() <= assigned;
        }

        private void moveShards() {
            // Iterate over all started shards and check if they can remain. In the presence of throttling shard movements,
            // the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are offloading the shards.
            for (final var iterator = OrderedShardsIterator.createForNecessaryMoves(allocation, moveOrdering); iterator.hasNext();) {
                final var shardRouting = iterator.next();

                if (shardRouting.started() == false) {
                    // can only move started shards
                    continue;
                }

                final var assignment = desiredBalance.getAssignment(shardRouting.shardId());
                if (assignment == null) {
                    // balance is not computed
                    continue;
                }

                if (assignment.nodeIds().contains(shardRouting.currentNodeId())) {
                    // shard is already on a desired node
                    continue;
                }

                if (allocation.deciders().canAllocate(shardRouting, allocation).type() != Decision.Type.YES) {
                    // cannot allocate anywhere, no point in looking for a target node
                    continue;
                }

                final var routingNode = routingNodes.node(shardRouting.currentNodeId());
                final var canRemainDecision = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
                if (canRemainDecision.type() != Decision.Type.NO) {
                    // it's desired elsewhere but technically it can remain on its current node. Defer its movement until later on to give
                    // priority to shards that _must_ move.
                    continue;
                }

                final var moveTarget = findRelocationTarget(shardRouting, assignment.nodeIds());
                if (moveTarget != null) {
                    logger.debug("Moving shard {} from {} to {}", shardRouting.shardId(), shardRouting.currentNodeId(), moveTarget.getId());
                    routingNodes.relocateShard(
                        shardRouting,
                        moveTarget.getId(),
                        allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                        "move",
                        allocation.changes()
                    );
                    iterator.dePrioritizeNode(shardRouting.currentNodeId());
                    moveOrdering.recordAllocation(shardRouting.currentNodeId());
                }
            }
        }

        private DesiredBalanceMetrics.AllocationStats balance() {
            int unassignedShards = routingNodes.unassigned().size() + routingNodes.unassigned().ignored().size();
            int totalAllocations = 0;
            int undesiredAllocationsExcludingShuttingDownNodes = 0;

            // Iterate over all started shards and try to move any which are on undesired nodes. In the presence of throttling shard
            // movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are offloading the
            // shards.
            for (final var iterator = OrderedShardsIterator.createForBalancing(allocation, moveOrdering); iterator.hasNext();) {
                final var shardRouting = iterator.next();

                totalAllocations++;

                if (shardRouting.started() == false) {
                    // can only rebalance started shards
                    continue;
                }

                final var assignment = desiredBalance.getAssignment(shardRouting.shardId());
                if (assignment == null) {
                    // balance is not computed
                    continue;
                }

                if (assignment.nodeIds().contains(shardRouting.currentNodeId())) {
                    // shard is already on a desired node
                    continue;
                }

                if (allocation.metadata().nodeShutdowns().contains(shardRouting.currentNodeId()) == false) {
                    // shard is not on a shutting down node, nor is it on a desired node per the previous check.
                    undesiredAllocationsExcludingShuttingDownNodes++;
                }

                if (allocation.deciders().canRebalance(allocation).type() != Decision.Type.YES) {
                    // Rebalancing is disabled, we're just here to collect the AllocationStats to return.
                    continue;
                }

                if (allocation.deciders().canRebalance(shardRouting, allocation).type() != Decision.Type.YES) {
                    // rebalancing disabled for this shard
                    continue;
                }

                if (allocation.deciders().canAllocate(shardRouting, allocation).type() != Decision.Type.YES) {
                    // cannot allocate anywhere, no point in looking for a target node
                    continue;
                }

                final var rebalanceTarget = findRelocationTarget(shardRouting, assignment.nodeIds(), this::decideCanAllocate);
                if (rebalanceTarget != null) {
                    logger.debug(
                        "Rebalancing shard {} from {} to {}",
                        shardRouting.shardId(),
                        shardRouting.currentNodeId(),
                        rebalanceTarget.getId()
                    );

                    routingNodes.relocateShard(
                        shardRouting,
                        rebalanceTarget.getId(),
                        allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                        "rebalance",
                        allocation.changes()
                    );
                    iterator.dePrioritizeNode(shardRouting.currentNodeId());
                    moveOrdering.recordAllocation(shardRouting.currentNodeId());
                }
            }

            maybeLogUndesiredAllocationsWarning(totalAllocations, undesiredAllocationsExcludingShuttingDownNodes, routingNodes.size());
            return new DesiredBalanceMetrics.AllocationStats(
                unassignedShards,
                totalAllocations,
                undesiredAllocationsExcludingShuttingDownNodes
            );
        }

        private void maybeLogUndesiredAllocationsWarning(int totalAllocations, int undesiredAllocations, int nodeCount) {
            // more shards than cluster can relocate with one reroute
            final boolean nonEmptyRelocationBacklog = undesiredAllocations > 2L * nodeCount;
            final boolean warningThresholdReached = undesiredAllocations > undesiredAllocationsLogThreshold * totalAllocations;
            if (totalAllocations > 0 && nonEmptyRelocationBacklog && warningThresholdReached) {
                undesiredAllocationLogInterval.maybeExecute(
                    () -> logger.warn(
                        "[{}] of assigned shards ({}/{}) are not on their desired nodes, which exceeds the warn threshold of [{}]",
                        Strings.format1Decimals(100.0 * undesiredAllocations / totalAllocations, "%"),
                        undesiredAllocations,
                        totalAllocations,
                        Strings.format1Decimals(100.0 * undesiredAllocationsLogThreshold, "%")
                    )
                );
            }
        }

        private DiscoveryNode findRelocationTarget(final ShardRouting shardRouting, Set<String> desiredNodeIds) {
            final var moveDecision = findRelocationTarget(shardRouting, desiredNodeIds, this::decideCanAllocate);
            if (moveDecision != null) {
                return moveDecision;
            }

            final var shardsOnReplacedNode = allocation.metadata().nodeShutdowns().contains(shardRouting.currentNodeId(), REPLACE);
            if (shardsOnReplacedNode) {
                return findRelocationTarget(shardRouting, desiredNodeIds, this::decideCanForceAllocateForVacate);
            }
            return null;
        }

        private DiscoveryNode findRelocationTarget(
            ShardRouting shardRouting,
            Set<String> desiredNodeIds,
            BiFunction<ShardRouting, RoutingNode, Decision> canAllocateDecider
        ) {
            // First sort by allocation ordering so we distribute relocated shards evenly
            final List<String> allocationPreference = allocationOrdering.sort(desiredNodeIds);
            for (final var nodeId : allocationPreference) {
                // TODO consider ignored nodes here too?
                if (nodeId.equals(shardRouting.currentNodeId())) {
                    continue;
                }
                final var node = routingNodes.node(nodeId);
                if (node == null) { // node left the cluster while reconciliation is still in progress
                    continue;
                }
                final var decision = canAllocateDecider.apply(shardRouting, node);
                logger.trace("relocate {} to {}: {}", shardRouting, nodeId, decision);
                if (decision.type() == Decision.Type.YES) {
                    allocationOrdering.recordAllocation(nodeId);
                    return node.node();
                }
            }

            return null;
        }

        private Decision decideCanAllocate(ShardRouting shardRouting, RoutingNode target) {
            assert target != null : "Target node is not found";
            return allocation.deciders().canAllocate(shardRouting, target, allocation);
        }

        private Decision decideCanForceAllocateForVacate(ShardRouting shardRouting, RoutingNode target) {
            assert target != null : "Target node is not found";
            return allocation.deciders().canForceAllocateDuringReplace(shardRouting, target, allocation);
        }
    }
}
