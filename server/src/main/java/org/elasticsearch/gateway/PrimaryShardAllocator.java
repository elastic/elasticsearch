/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStoreInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.AsyncShardFetch.FetchResult;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * The primary shard allocator allocates unassigned primary shards to nodes that hold
 * valid copies of the unassigned primaries.  It does this by iterating over all unassigned
 * primary shards in the routing table and fetching shard metadata from each node in the cluster
 * that holds a copy of the shard.  The shard metadata from each node is compared against the
 * set of valid allocation IDs and for all valid shard copies (if any), the primary shard allocator
 * executes the allocation deciders to chose a copy to assign the primary shard to.
 *
 * Note that the PrimaryShardAllocator does *not* allocate primaries on index creation
 * (see {@link org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator}),
 * nor does it allocate primaries when a primary shard failed and there is a valid replica
 * copy that can immediately be promoted to primary, as this takes place in {@link RoutingNodes#failShard}.
 */
public abstract class PrimaryShardAllocator extends BaseGatewayShardAllocator {
    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     */
    private static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() // must be primary
            && shard.unassigned() // must be unassigned
            // only handle either an existing store or a snapshot recovery
            && (shard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
                || shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT);
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(
        final ShardRouting unassignedShard,
        final RoutingAllocation allocation,
        final Logger logger
    ) {
        if (isResponsibleFor(unassignedShard) == false) {
            // this allocator is not responsible for allocating this shard
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        final boolean explain = allocation.debugDecision();

        if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
            && allocation.snapshotShardSizeInfo().getShardSize(unassignedShard) == null) {
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }

        final FetchResult<NodeGatewayStartedShards> shardState = fetchData(unassignedShard, allocation);
        if (shardState.hasData() == false) {
            allocation.setHasPendingAsyncFetch();
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }

        // don't create a new IndexSetting object for every shard as this could cause a lot of garbage
        // on cluster restart if we allocate a boat load of shards
        final IndexMetadata indexMetadata = allocation.metadata().indexMetadata(unassignedShard.index());
        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
        final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

        assert inSyncAllocationIds.isEmpty() == false;
        // use in-sync allocation ids to select nodes
        final NodeShardsResult nodeShardsResult = buildNodeShardsResult(
            unassignedShard,
            snapshotRestore,
            allocation.getIgnoreNodes(unassignedShard.shardId()),
            inSyncAllocationIds,
            shardState,
            logger
        );
        final boolean enoughAllocationsFound = nodeShardsResult.orderedAllocationCandidates.size() > 0;
        logger.debug(
            "[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]",
            unassignedShard.index(),
            unassignedShard.id(),
            nodeShardsResult.orderedAllocationCandidates.size(),
            unassignedShard,
            inSyncAllocationIds
        );

        if (enoughAllocationsFound == false) {
            if (snapshotRestore) {
                // let BalancedShardsAllocator take care of allocating this shard
                logger.debug(
                    "[{}][{}]: missing local data, will restore from [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard.recoverySource()
                );
                return AllocateUnassignedDecision.NOT_TAKEN;
            } else {
                // We have a shard that was previously allocated, but we could not find a valid shard copy to allocate the primary.
                // We could just be waiting for the node that holds the primary to start back up, in which case the allocation for
                // this shard will be picked up when the node joins and we do another allocation reroute
                logger.debug(
                    "[{}][{}]: not allocating, number_of_allocated_shards_found [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    nodeShardsResult.allocationsFound
                );
                return AllocateUnassignedDecision.no(
                    AllocationStatus.NO_VALID_SHARD_COPY,
                    explain ? buildNodeDecisions(null, shardState, inSyncAllocationIds) : null
                );
            }
        }

        AllocationChoice choice = chooseFromCandidates(
            allocation,
            nodeShardsResult.orderedAllocationCandidates,
            unassignedShard,
            false,
            logger
        );
        final NodesToAllocate nodesToAllocate = choice.nodesToAllocate();
        final DiscoveryNode node = choice.node();
        final String allocationId = choice.allocationId();
        final Decision.Type decisionType = choice.decisionType();

        List<NodeAllocationResult> nodeResults = null;
        if (explain) {
            nodeResults = buildNodeDecisions(nodesToAllocate, shardState, inSyncAllocationIds);
        }
        if (allocation.hasPendingAsyncFetch()) {
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeResults);
        } else if (decisionType == Decision.Type.YES || decisionType == Decision.Type.NOT_PREFERRED) {
            assert node != null : "node must not be null if decisionType is YES or NOT_PREFERRED";
            return AllocateUnassignedDecision.yes(node, allocationId, nodeResults, false);
        } else if (decisionType == Decision.Type.THROTTLE) {
            return AllocateUnassignedDecision.throttle(nodeResults);
        } else {
            return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeResults, true);
        }
    }

    /**
     * Builds a map of nodes to the corresponding allocation decisions for those nodes.
     */
    private static List<NodeAllocationResult> buildNodeDecisions(
        NodesToAllocate nodesToAllocate,
        FetchResult<NodeGatewayStartedShards> fetchedShardData,
        Set<String> inSyncAllocationIds
    ) {
        List<NodeAllocationResult> nodeResults = new ArrayList<>();
        Collection<NodeGatewayStartedShards> ineligibleShards;
        if (nodesToAllocate != null) {
            final Set<DiscoveryNode> discoNodes = new HashSet<>();
            for (DecidedNode dnode : nodesToAllocate) {
                discoNodes.add(dnode.nodeShardState.getNode());
                nodeResults.add(
                    new NodeAllocationResult(
                        dnode.nodeShardState.getNode(),
                        shardStoreInfo(dnode.nodeShardState, inSyncAllocationIds),
                        dnode.decision
                    )
                );
            }
            ineligibleShards = fetchedShardData.getData()
                .values()
                .stream()
                .filter(shardData -> discoNodes.contains(shardData.getNode()) == false)
                .toList();
        } else {
            // there were no shard copies that were eligible for being assigned the allocation,
            // so all fetched shard data are ineligible shards
            ineligibleShards = fetchedShardData.getData().values();
        }

        nodeResults.addAll(
            ineligibleShards.stream()
                .map(shardData -> new NodeAllocationResult(shardData.getNode(), shardStoreInfo(shardData, inSyncAllocationIds), null))
                .toList()
        );

        return nodeResults;
    }

    private static ShardStoreInfo shardStoreInfo(NodeGatewayStartedShards nodeShardState, Set<String> inSyncAllocationIds) {
        final Exception storeErr = nodeShardState.storeException();
        final boolean inSync = nodeShardState.allocationId() != null && inSyncAllocationIds.contains(nodeShardState.allocationId());
        return new ShardStoreInfo(nodeShardState.allocationId(), inSync, storeErr);
    }

    private static final Comparator<NodeGatewayStartedShards> NO_STORE_EXCEPTION_FIRST_COMPARATOR = Comparator.comparing(
        (NodeGatewayStartedShards state) -> state.storeException() == null
    ).reversed();
    private static final Comparator<NodeGatewayStartedShards> PRIMARY_FIRST_COMPARATOR = Comparator.comparing(
        NodeGatewayStartedShards::primary
    ).reversed();

    /**
     * Builds a list of nodes. If matchAnyShard is set to false, only nodes that have an allocation id matching
     * inSyncAllocationIds are added to the list. Otherwise, any node that has a shard is added to the list, but
     * entries with matching allocation id are always at the front of the list.
     */
    private static NodeShardsResult buildNodeShardsResult(
        ShardRouting shard,
        boolean matchAnyShard,
        Set<String> ignoreNodes,
        Set<String> inSyncAllocationIds,
        FetchResult<NodeGatewayStartedShards> shardState,
        Logger logger
    ) {
        List<NodeGatewayStartedShards> nodeShardStates = new ArrayList<>();
        int numberOfAllocationsFound = 0;
        for (NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            DiscoveryNode node = nodeShardState.getNode();
            String allocationId = nodeShardState.allocationId();

            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (allocationId == null) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, nodeShardState.getNode());
                } else {
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, nodeShardState.getNode(), allocationId);
                }
            } else {
                final String finalAllocationId = allocationId;
                if (nodeShardState.storeException() instanceof ShardLockObtainFailedException) {
                    logger.trace(
                        () -> format(
                            "[%s] on node [%s] has allocation id [%s] but the store can not be "
                                + "opened as it's locked, treating as valid shard",
                            shard,
                            nodeShardState.getNode(),
                            finalAllocationId
                        ),
                        nodeShardState.storeException()
                    );
                } else {
                    logger.trace(
                        () -> format(
                            "[%s] on node [%s] has allocation id [%s] but the store can not be " + "opened, treating as no allocation id",
                            shard,
                            nodeShardState.getNode(),
                            finalAllocationId
                        ),
                        nodeShardState.storeException()
                    );
                    allocationId = null;
                }
            }

            if (allocationId != null) {
                assert nodeShardState.storeException() == null || nodeShardState.storeException() instanceof ShardLockObtainFailedException
                    : "only allow store that can be opened or that throws a ShardLockObtainFailedException while being opened but got a "
                        + "store throwing "
                        + nodeShardState.storeException();
                numberOfAllocationsFound++;
                if (matchAnyShard || inSyncAllocationIds.contains(nodeShardState.allocationId())) {
                    nodeShardStates.add(nodeShardState);
                }
            }
        }

        final Comparator<NodeGatewayStartedShards> comparator; // allocation preference
        if (matchAnyShard) {
            // prefer shards with matching allocation ids
            Comparator<NodeGatewayStartedShards> matchingAllocationsFirst = Comparator.comparing(
                (NodeGatewayStartedShards state) -> inSyncAllocationIds.contains(state.allocationId())
            ).reversed();
            comparator = matchingAllocationsFirst.thenComparing(NO_STORE_EXCEPTION_FIRST_COMPARATOR)
                .thenComparing(PRIMARY_FIRST_COMPARATOR);
        } else {
            comparator = NO_STORE_EXCEPTION_FIRST_COMPARATOR.thenComparing(PRIMARY_FIRST_COMPARATOR);
        }

        nodeShardStates.sort(comparator);

        if (logger.isTraceEnabled()) {
            logger.trace(
                "{} candidates for allocation: {}",
                shard,
                nodeShardStates.stream().map(s -> s.getNode().getName()).collect(Collectors.joining(", "))
            );
        }
        return new NodeShardsResult(nodeShardStates, numberOfAllocationsFound);
    }

    /**
     * Split the list of node shard states into groups by allocation decider decision.
     */
    private static NodesToAllocate buildNodesToAllocate(
        RoutingAllocation allocation,
        List<NodeGatewayStartedShards> nodeShardStates,
        ShardRouting shardRouting,
        boolean forceAllocate
    ) {
        List<DecidedNode> yesNodeShards = new ArrayList<>();
        List<DecidedNode> throttleNodeShards = new ArrayList<>();
        List<DecidedNode> notPreferredNodeShards = new ArrayList<>();
        List<DecidedNode> noNodeShards = new ArrayList<>();
        for (NodeGatewayStartedShards nodeShardState : nodeShardStates) {
            RoutingNode node = allocation.routingNodes().node(nodeShardState.getNode().getId());
            if (node == null) {
                continue;
            }

            Decision decision = forceAllocate
                ? allocation.deciders().canForceAllocatePrimary(shardRouting, node, allocation)
                : allocation.deciders().canAllocate(shardRouting, node, allocation);
            DecidedNode decidedNode = new DecidedNode(nodeShardState, decision);
            (switch (decision.type()) {
                case YES -> yesNodeShards;
                case THROTTLE -> throttleNodeShards;
                case NOT_PREFERRED -> notPreferredNodeShards;
                case NO -> noNodeShards;
            }).add(decidedNode);
        }
        return new NodesToAllocate(
            Collections.unmodifiableList(yesNodeShards),
            Collections.unmodifiableList(throttleNodeShards),
            Collections.unmodifiableList(notPreferredNodeShards),
            Collections.unmodifiableList(noNodeShards)
        );
    }

    protected abstract FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation);

    private record NodeShardsResult(List<NodeGatewayStartedShards> orderedAllocationCandidates, int allocationsFound) {}

    /**
     * Chooses a node (or throttle/deny) from allocation candidates. If the only decisions are NO and
     * force allocation has not been tried yet, recurses with {@code forceAllocate == true}.
     */
    private static AllocationChoice chooseFromCandidates(
        RoutingAllocation allocation,
        List<NodeGatewayStartedShards> orderedAllocationCandidates,
        ShardRouting unassignedShard,
        boolean forceAllocate,
        Logger logger
    ) {
        NodesToAllocate nodesToAllocate = buildNodesToAllocate(allocation, orderedAllocationCandidates, unassignedShard, forceAllocate);
        String allocationContext = forceAllocate ? "forced primary allocation" : "primary allocation";

        if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
            DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
            logger.debug(
                "[{}][{}]: allocating [{}] to [{}] on {}",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                decidedNode.nodeShardState.getNode(),
                allocationContext
            );
            return new AllocationChoice(
                decidedNode.nodeShardState.getNode(),
                decidedNode.nodeShardState.allocationId(),
                Decision.Type.YES,
                nodesToAllocate
            );
        }
        if (nodesToAllocate.throttleNodeShards.isEmpty() == false) {
            logger.debug(
                "[{}][{}]: throttling allocation [{}] to [{}] on {}",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                nodesToAllocate.throttleNodeShards,
                allocationContext
            );
            return new AllocationChoice(null, null, Decision.Type.THROTTLE, nodesToAllocate);
        }
        if (nodesToAllocate.notPreferredNodeShards.isEmpty() == false) {
            DecidedNode decidedNode = nodesToAllocate.notPreferredNodeShards.get(0);
            logger.debug(
                "[{}][{}]: allocating [{}] to [{}] on {} (not preferred)",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                decidedNode.nodeShardState.getNode(),
                allocationContext
            );
            return new AllocationChoice(
                decidedNode.nodeShardState.getNode(),
                decidedNode.nodeShardState.allocationId(),
                Decision.Type.NOT_PREFERRED,
                nodesToAllocate
            );
        }
        if (nodesToAllocate.noNodeShards.isEmpty() == false) {
            if (forceAllocate) {
                logger.debug(
                    "[{}][{}]: forced primary allocation denied [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard
                );
            } else {
                return chooseFromCandidates(allocation, orderedAllocationCandidates, unassignedShard, true, logger);
            }
        }
        return new AllocationChoice(null, null, Decision.Type.NO, nodesToAllocate);
    }

    /**
     * Result of choosing a node from allocation candidates, possibly after trying force allocation.
     * The decision type is one of YES, NOT_PREFERRED, THROTTLE, or NO.
     */
    private record AllocationChoice(DiscoveryNode node, String allocationId, Decision.Type decisionType, NodesToAllocate nodesToAllocate) {
        AllocationChoice {
            assert (node != null) == (decisionType == Decision.Type.YES || decisionType == Decision.Type.NOT_PREFERRED)
                : "node must be non-null iff decisionType is YES or NOT_PREFERRED";
        }
    }

    record NodesToAllocate(
        List<DecidedNode> yesNodeShards,
        List<DecidedNode> throttleNodeShards,
        List<DecidedNode> notPreferredNodeShards,
        List<DecidedNode> noNodeShards
    ) implements Iterable<DecidedNode> {

        /**
         * Iterate over all decided nodes in order: YES, THROTTLE, NOT_PREFERRED, NO.
         */
        @Override
        public Iterator<DecidedNode> iterator() {
            return Iterators.concat(
                yesNodeShards.iterator(),
                throttleNodeShards.iterator(),
                notPreferredNodeShards.iterator(),
                noNodeShards.iterator()
            );
        }
    }

    /**
     * Encapsulates the shard state retrieved from a node and the decision that was made
     * by the allocator for allocating to the node that holds the shard copy.
     * Package-visible for testing.
     */
    record DecidedNode(NodeGatewayStartedShards nodeShardState, Decision decision) {}
}
