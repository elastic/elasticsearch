/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger();

    private final BalancedShardsAllocator balancedShardsAllocator;
    private final Supplier<RerouteService> rerouteServiceSupplier;

    private final ContinuousComputation<RerouteInput> desiredBalanceComputation;

    private volatile DesiredBalance currentDesiredBalance;

    public DesiredBalanceShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.rerouteServiceSupplier = rerouteServiceSupplier;
        this.balancedShardsAllocator = new BalancedShardsAllocator(settings, clusterSettings);

        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {
            @Override
            protected void processInput(RerouteInput actualRoutingAllocation) {
                updateDesiredBalanceAndReroute(actualRoutingAllocation);
            }
        };
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
            : Thread.currentThread().getName();
        // assert allocation.debugDecision() == false; set to true when called via the reroute API
        assert allocation.ignoreDisable() == false;

        desiredBalanceComputation.onNewInput(
            new RerouteInput(allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );

        new Reconciler(currentDesiredBalance, allocation).run();

    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return balancedShardsAllocator.decideShardAllocation(shard, allocation);
    }

    /**
     * @param routingAllocation a copy of the immutable parts of the allocation decision process context
     * @param ignoredShards     a copy of the shards for which earlier allocators have claimed responsibility
     */
    record RerouteInput(RoutingAllocation routingAllocation, List<ShardRouting> ignoredShards) {}

    private void updateDesiredBalanceAndReroute(RerouteInput rerouteInput) {

        final var routingAllocation = rerouteInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = routingAllocation.routingNodes();
        final var ignoredShards = new HashSet<>(rerouteInput.ignoredShards());
        final var desiredBalance = currentDesiredBalance;
        final var changes = routingAllocation.changes();
        final var knownNodeIds = routingAllocation.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());

        // we assume that all ongoing recoveries will complete
        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                if (shardRouting.initializing()) {
                    routingNodes.startShard(logger, shardRouting, changes);
                    // TODO adjust disk usage info to reflect the assumed shard movement
                }
            }
        }

        // we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
        // unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
        final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
        for (final var shardRouting : unassigned) {
            if (ignoredShards.contains(shardRouting)) {
                unassigned.ignoreShard(shardRouting, UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
            }
        }

        // we can assume that all possible shards will be allocated/relocated to one of their desired locations
        final var unassignedShardsToInitialize = new HashMap<ShardRouting, LinkedList<String>>();
        for (final var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            final var shardId = shardAndAssignments.getKey();
            final List<ShardRouting> shardRoutings = shardAndAssignments.getValue();

            final var shardsToAssign = new ArrayList<ShardRouting>();
            // treesets so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeSet<>(Comparator.comparing(ShardRouting::currentNodeId));
            final var targetNodes = new TreeSet<>(desiredBalance.getDesiredNodeIds(shardId));
            targetNodes.retainAll(knownNodeIds);

            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.started()) {
                    if (targetNodes.remove(shardRouting.currentNodeId()) == false) {
                        shardsToRelocate.add(shardRouting);
                    }
                } else {
                    assert shardRouting.unassigned() : shardRouting;
                    shardsToAssign.add(shardRouting);
                }
            }

            final var targetNodesIterator = targetNodes.iterator();
            final var shardsIterator = Iterators.concat(shardsToRelocate.iterator(), shardsToAssign.iterator());
            while (targetNodesIterator.hasNext() && shardsIterator.hasNext()) {
                final ShardRouting shardRouting = shardsIterator.next();
                if (shardRouting.started()) {
                    routingNodes.startShard(
                        logger,
                        routingNodes.relocateShard(shardRouting, targetNodesIterator.next(), 0L, changes).v2(),
                        changes
                    );
                } else {
                    unassignedShardsToInitialize.computeIfAbsent(shardRouting, ignored -> new LinkedList<>())
                        .add(targetNodesIterator.next());
                }
            }
        }

        final var unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final var shardRouting = unassignedIterator.next();
            final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
            if (nodeIds != null && nodeIds.isEmpty() == false) {
                final String nodeId = nodeIds.removeFirst();
                routingNodes.startShard(logger, unassignedIterator.initialize(nodeId, null, 0L, changes), changes);
            }
            // TODO must also reset failure counter to bypass MaxRetryAllocationDecider
            // TODO must also bypass ResizeAllocationDecider
            // TODO must also bypass RestoreInProgressAllocationDecider
            // TODO what about delayed allocation?
        }

        boolean hasChanges = true;
        while (hasChanges) {
            balancedShardsAllocator.allocate(routingAllocation);

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        routingNodes.startShard(logger, shardRouting, changes);
                        // TODO adjust disk usage info to reflect the assumed shard movement
                    }
                }
            }

            // TODO what if we never converge?
            // TODO maybe expose interim desired balances computed here
            // TODO maybe stop iterating if a new input becomes available
        }

        final var desiredAssignments = new HashMap<ShardId, List<String>>();
        for (var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            desiredAssignments.put(
                shardAndAssignments.getKey(),
                shardAndAssignments.getValue().stream().map(ShardRouting::currentNodeId).collect(Collectors.toList())
            );
        }

        final DesiredBalance newDesiredBalance = new DesiredBalance(desiredAssignments);
        assert desiredBalance == currentDesiredBalance;
        if (newDesiredBalance.equals(desiredBalance) == false) {
            currentDesiredBalance = newDesiredBalance;
            rerouteServiceSupplier.get().reroute("desired balance changed", Priority.HIGH, ActionListener.wrap(() -> {}));
        }
    }

    private record DesiredBalance(Map<ShardId, List<String>> desiredAssignments) {
        List<String> getDesiredNodeIds(ShardId shardId) {
            return desiredAssignments.getOrDefault(shardId, Collections.emptyList());
        }
    }

    private static final class Reconciler {

        @Nullable
        private final DesiredBalance desiredBalance;
        private final RoutingAllocation allocation; // TODO rename
        private final RoutingNodes routingNodes;

        private Reconciler(@Nullable DesiredBalance desiredBalance, RoutingAllocation routingAllocation) {
            this.desiredBalance = desiredBalance;
            this.allocation = routingAllocation;
            routingNodes = routingAllocation.routingNodes();
        }


        void run() {
            if (desiredBalance == null) {
                // no desired state yet but it is on its way and we'll reroute again when its ready
                return;
            }

            if (allocation.routingNodes().size() == 0) {
                // no data nodes, so fail allocation to report red health
                failAllocationOfNewPrimaries(allocation);
                return;
            }

            // compute next moves towards current desired balance:

            // 1. allocate unassigned shards first
            allocateUnassigned();
            // 2. move any shards that cannot remain where they are
            moveShards();
            // 3. move any other shards that are desired elsewhere
            balance();
        }

        private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
            RoutingNodes routingNodes = allocation.routingNodes();
            assert routingNodes.size() == 0 : routingNodes;
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
            while (unassignedIterator.hasNext()) {
                final ShardRouting shardRouting = unassignedIterator.next();
                final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
                if (shardRouting.primary() && unassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT) {
                    unassignedIterator.updateUnassigned(
                        new UnassignedInfo(
                            unassignedInfo.getReason(),
                            unassignedInfo.getMessage(),
                            unassignedInfo.getFailure(),
                            unassignedInfo.getNumFailedAllocations(),
                            unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(),
                            unassignedInfo.isDelayed(),
                            UnassignedInfo.AllocationStatus.DECIDERS_NO,
                            unassignedInfo.getFailedNodeIds(),
                            unassignedInfo.getLastAllocatedNodeId()
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
                logger.trace("Start allocating unassigned shards");
            }
            if (unassigned.isEmpty()) {
                return;
            }

            /*
             * TODO: We could be smarter here and group the shards by index and then
             * use the sorter to save some iterations.
             */
            final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
            final Comparator<ShardRouting> comparator = (o1, o2) -> {
                if (o1.primary() ^ o2.primary()) {
                    return o1.primary() ? -1 : 1;
                }
                if (o1.getIndexName().compareTo(o2.getIndexName()) == 0) {
                    return o1.getId() - o2.getId();
                }
                // this comparator is more expensive than all the others up there
                // that's why it's added last even though it could be easier to read
                // if we'd apply it earlier. this comparator will only differentiate across
                // indices all shards of the same index is treated equally.
                final int secondary = secondaryComparator.compare(o1, o2);
                assert secondary != 0 : "Index names are equal, should be returned early.";
                return secondary;
            };
            /*
             * we use 2 arrays and move replicas to the second array once we allocated an identical
             * replica in the current iteration to make sure all indices get allocated in the same manner.
             * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with
             * 2 replica and 1 shard would look like:
             * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
             * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
             * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
             */
            ShardRouting[] primary = unassigned.drain();
            ShardRouting[] secondary = new ShardRouting[primary.length];
            int secondaryLength = 0;
            int primaryLength = primary.length;
            ArrayUtil.timSort(primary, comparator);
            do {
                for (int i = 0; i < primaryLength; i++) {
                    final ShardRouting shard = primary[i];
                    for (final var desiredNodeId : desiredBalance.getDesiredNodeIds(shard.shardId())) {
                        final var routingNode = routingNodes.node(desiredNodeId);
                        if (routingNode == null) {
                            continue;
                        }

                        final var canAllocateDecision = allocation.deciders().canAllocate(shard, routingNode, allocation);
                        if (canAllocateDecision.type() == Decision.Type.YES) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Assigned shard [{}] to [{}]", shard, desiredNodeId);
                            }

                            final long shardSize = DiskThresholdDecider.getExpectedShardSize(
                                shard,
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                                allocation.clusterInfo(),
                                allocation.snapshotShardSizeInfo(),
                                allocation.metadata(),
                                allocation.routingTable()
                            );
                            routingNodes.initializeShard(shard, desiredNodeId, null, shardSize, allocation.changes());
                            if (shard.primary() == false) {
                                // copy over the same replica shards to the secondary array so they will get allocated
                                // in a subsequent iteration, allowing replicas of other shards to be allocated first
                                while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                                    secondary[secondaryLength++] = primary[++i];
                                }
                            }
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace(
                                    "No eligible node found to assign shard [{}] canAllocateDecision [{}]",
                                    shard,
                                    canAllocateDecision
                                );
                            }

                            final var allocationStatus = UnassignedInfo.AllocationStatus.fromDecision(canAllocateDecision.type());
                            unassigned.ignoreShard(shard, allocationStatus, allocation.changes());
                            if (shard.primary() == false) {
                                // we could not allocate it and we are a replica - check if we can ignore the other replicas
                                while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                                    unassigned.ignoreShard(primary[++i], allocationStatus, allocation.changes());
                                }
                            }
                        }
                    }
                }
                primaryLength = secondaryLength;
                ShardRouting[] tmp = primary;
                primary = secondary;
                secondary = tmp;
                secondaryLength = 0;
            } while (primaryLength > 0);
        }

        private void balance() {

        }

        private void moveShards() {

        }

    }

}
