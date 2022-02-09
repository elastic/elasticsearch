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
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

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

        final var desiredBalance = currentDesiredBalance;

        if (desiredBalance == null) {
            // no desired state yet
            return;
        }

        // 2. compute next moves towards current desired balance

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

        final var routingAllocation = rerouteInput.routingAllocation().mutableClone();
        final var routingNodes = routingAllocation.routingNodes();
        final var ignoredShards = new HashSet<>(rerouteInput.ignoredShards());
        final var desiredBalance = currentDesiredBalance;
        final var changes = routingAllocation.changes();

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

            // treesets so that we are consistent about the order of future relocations
            final var targetNodes = new TreeSet<>(desiredBalance.getDesiredNodeIds(shardId));
            final var shardsToRelocate = new TreeSet<>(Comparator.comparing(ShardRouting::currentNodeId));
            final var shardsToAssign = new ArrayList<ShardRouting>();

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
        }

        boolean hasChanges = true;
        while (hasChanges) {
            // TODO need to disable any transient deciders so we get an accurate picture of the desired state
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
        }

        // we are not responsible for allocating existing primaries, just leave them unallocated until the PrimaryShardAllocator gets them
        // also need to let the ReplicaShardAllocator do its thing.

        // adjust desired balance according to rerouteInput
        // if it is changed then call reroute() again

        currentDesiredBalance = new DesiredBalance(routingTable);
    }

    static class DesiredBalance {
        final RoutingTable routingTable;

        DesiredBalance(RoutingTable routingTable) {
            this.routingTable = routingTable;
        }

        public Set<String> getDesiredNodeIds(ShardId shardId) {
            throw new UnsupportedOperationException("TODO");
        }
    }
}
