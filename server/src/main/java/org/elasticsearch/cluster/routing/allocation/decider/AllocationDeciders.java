/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Combines the decision of multiple {@link AllocationDecider} implementations into a single allocation decision.
 */
public class AllocationDeciders {

    private static final Logger logger = LogManager.getLogger(AllocationDeciders.class);

    private static final Decision NO_IGNORING_SHARD_FOR_NODE = Decision.single(
        Decision.Type.NO,
        "ignored_shards_for_node",
        "shard temporarily ignored for node due to earlier failure"
    );

    private final AllocationDecider[] deciders;

    public AllocationDeciders(Collection<? extends AllocationDecider> deciders) {
        this.deciders = deciders.toArray(AllocationDecider[]::new);
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return withDeciders(
            allocation,
            decider -> decider.canAllocate(shardRouting, allocation),
            (decider, decision) -> Strings.format("Can not allocate [%s] on any node. [%s]: %s", shardRouting, decider, decision)
        );
    }

    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return withDeciders(
            allocation,
            decider -> decider.canAllocate(indexMetadata, node, allocation),
            (decider, decision) -> Strings.format(
                "Can not allocate [%s] on node [%s]. [%s]: %s",
                indexMetadata.getIndex().getName(),
                node.node(),
                decider,
                decision
            )
        );
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return withDecidersCheckingShardIgnoredNodes(
            allocation,
            shardRouting,
            node,
            decider -> decider.canAllocate(shardRouting, node, allocation),
            (decider, decision) -> Strings.format(
                "Can not allocate [%s] on node [%s]. [%s]: %s",
                shardRouting,
                node.node(),
                decider,
                decision
            )
        );
    }

    public Decision canRebalance(RoutingAllocation allocation) {
        return withDeciders(
            allocation,
            decider -> decider.canRebalance(allocation),
            (decider, decision) -> Strings.format("Can not rebalance. [%s]: %s", decider, decision)
        );
    }

    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        assert shardRouting.started() : "Only started shard could be rebalanced: " + shardRouting;
        return withDeciders(
            allocation,
            decider -> decider.canRebalance(shardRouting, allocation),
            (decider, decision) -> Strings.format("Can not rebalance [%s]. [%s]: %s", shardRouting, decider, decision)
        );
    }

    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        return withDecidersCheckingShardIgnoredNodes(
            allocation,
            shardRouting,
            node,
            decider -> decider.canRemain(indexMetadata, shardRouting, node, allocation),
            (decider, decision) -> Strings.format("Can not remain [%s] on node [%s]. [%s]: %s", shardRouting, node, decider, decision)
        );
    }

    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return withDeciders(
            allocation,
            decider -> decider.shouldAutoExpandToNode(indexMetadata, node, allocation),
            (decider, decision) -> Strings.format(
                "Should not auto expand [%s] to node [%s]. [%s]: %s",
                indexMetadata.getIndex().getName(),
                node,
                decider,
                decision
            )
        );
    }

    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard routing " + shardRouting;
        return withDecidersCheckingShardIgnoredNodes(
            allocation,
            shardRouting,
            node,
            decider -> decider.canForceAllocatePrimary(shardRouting, node, allocation),
            (decider, decision) -> Strings.format(
                "Can not force allocate shard [%s] on node [%s]. [%s]: %s",
                shardRouting,
                node,
                decider,
                decision
            )
        );
    }

    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return withDeciders(
            allocation,
            decider -> decider.canForceAllocateDuringReplace(shardRouting, node, allocation),
            (decider, decision) -> Strings.format(
                "Can not force allocate during replace shard [%s] on node [%s]. [%s]: %s",
                shardRouting,
                node,
                decider,
                decision
            )
        );
    }

    public Decision canAllocateReplicaWhenThereIsRetentionLease(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return withDecidersCheckingShardIgnoredNodes(
            allocation,
            shardRouting,
            node,
            decider -> decider.canAllocateReplicaWhenThereIsRetentionLease(shardRouting, node, allocation),
            (decider, decision) -> Strings.format(
                "Can not allocate replica when there is retention lease shard [%s] on node [%s]. [%s]: %s",
                shardRouting,
                node,
                decider,
                decision
            )
        );
    }

    private Decision withDeciders(
        RoutingAllocation allocation,
        Function<AllocationDecider, Decision> deciderAction,
        BiFunction<String, Decision, String> logMessageCreator
    ) {
        return withDeciders(allocation.getDebugMode(), deciderAction, logMessageCreator);
    }

    private Decision withDecidersCheckingShardIgnoredNodes(
        RoutingAllocation allocation,
        ShardRouting shardRouting,
        RoutingNode node,
        Function<AllocationDecider, Decision> deciderAction,
        BiFunction<String, Decision, String> logMessageCreator
    ) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            if (logger.isTraceEnabled()) {
                logger.trace(() -> logMessageCreator.apply(AllocationDeciders.class.getSimpleName(), NO_IGNORING_SHARD_FOR_NODE));
            }
            return NO_IGNORING_SHARD_FOR_NODE;
        }
        return withDeciders(allocation.getDebugMode(), deciderAction, logMessageCreator);
    }

    private Decision withDeciders(
        RoutingAllocation.DebugMode debugMode,
        Function<AllocationDecider, Decision> deciderAction,
        BiFunction<String, Decision, String> logMessageCreator
    ) {
        if (debugMode == RoutingAllocation.DebugMode.OFF) {
            var result = Decision.YES;
            for (AllocationDecider decider : deciders) {
                var decision = deciderAction.apply(decider);
                if (decision.type() == Decision.Type.NO) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(() -> logMessageCreator.apply(decider.getClass().getSimpleName(), decision));
                    }
                    return decision;
                } else if (result.type() == Decision.Type.YES && decision.type() == Decision.Type.THROTTLE) {
                    result = decision;
                }
            }
            return result;
        } else {
            var result = new Decision.Multi();
            for (AllocationDecider decider : deciders) {
                var decision = deciderAction.apply(decider);
                if (logger.isTraceEnabled() && decision.type() == Decision.Type.NO) {
                    logger.trace(() -> logMessageCreator.apply(decider.getClass().getSimpleName(), decision));
                }
                if (decision != Decision.ALWAYS && (debugMode == RoutingAllocation.DebugMode.ON || decision.type() != Decision.Type.YES)) {
                    result.add(decision);
                }
            }
            return result;
        }
    }

    public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
        var result = Optional.<Set<String>>empty();
        for (AllocationDecider decider : deciders) {
            var forcedInitialNodeIds = decider.getForcedInitialShardAllocationToNodes(shardRouting, allocation);
            if (forcedInitialNodeIds.isPresent()) {
                result = result.map(nodeIds -> Sets.intersection(nodeIds, forcedInitialNodeIds.get())).or(() -> forcedInitialNodeIds);
            }
        }
        return result;
    }
}
