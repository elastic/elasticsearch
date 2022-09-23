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

import java.util.Collection;

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

    private final AllocationDecider[] allocations;

    public AllocationDeciders(Collection<AllocationDecider> allocations) {
        this.allocations = allocations.toArray(AllocationDecider[]::new);
    }

    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return NO_IGNORING_SHARD_FOR_NODE;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Can not allocate [{}] on node [{}] due to [{}]",
                        shardRouting,
                        node.node(),
                        allocationDecider.getClass().getSimpleName()
                    );
                }
                // short circuit only if debugging is not enabled
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Shard [{}] should be ignored for node [{}]", shardRouting, node.nodeId());
            }
            return NO_IGNORING_SHARD_FOR_NODE;
        }
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (allocation.debugDecision()) {
            Decision.Multi ret = new Decision.Multi();
            for (AllocationDecider allocationDecider : allocations) {
                Decision decision = allocationDecider.canRemain(indexMetadata, shardRouting, node, allocation);
                // short track if a NO is returned.
                if (decision.type() == Decision.Type.NO) {
                    maybeTraceLogNoDecision(shardRouting, node, allocationDecider);
                    ret.add(decision);
                } else {
                    addDecision(ret, decision, allocation);
                }
            }
            return ret;
        } else {
            // tighter loop if debug information is not collected: don't collect yes decisions + break out right away on NO
            Decision ret = Decision.YES;
            for (AllocationDecider allocationDecider : allocations) {
                switch (allocationDecider.canRemain(indexMetadata, shardRouting, node, allocation).type()) {
                    case NO -> {
                        maybeTraceLogNoDecision(shardRouting, node, allocationDecider);
                        return Decision.NO;
                    }
                    case THROTTLE -> ret = Decision.THROTTLE;
                }
            }
            return ret;
        }
    }

    private void maybeTraceLogNoDecision(ShardRouting shardRouting, RoutingNode node, AllocationDecider allocationDecider) {
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Shard [{}] can not remain on node [{}] due to [{}]",
                shardRouting,
                node.nodeId(),
                allocationDecider.getClass().getSimpleName()
            );
        }
    }

    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(indexMetadata, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.shouldAutoExpandToNode(indexMetadata, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canRebalance(RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard routing " + shardRouting;

        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return NO_IGNORING_SHARD_FOR_NODE;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider decider : allocations) {
            Decision decision = decider.canForceAllocatePrimary(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Shard [{}] can not be forcefully allocated to node [{}] due to [{}].",
                        shardRouting.shardId(),
                        node.nodeId(),
                        decider.getClass().getSimpleName()
                    );
                }
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canForceAllocateDuringReplace(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision canAllocateReplicaWhenThereIsRetentionLease(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return NO_IGNORING_SHARD_FOR_NODE;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocateReplicaWhenThereIsRetentionLease(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision.type() == Decision.Type.NO) {
                if (allocation.debugDecision() == false) {
                    return Decision.NO;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    private static void addDecision(Decision.Multi ret, Decision decision, RoutingAllocation allocation) {
        // We never add ALWAYS decisions and only add YES decisions when requested by debug mode (since Multi default is YES).
        if (decision != Decision.ALWAYS
            && (allocation.getDebugMode() == RoutingAllocation.DebugMode.ON || decision.type() != Decision.Type.YES)) {
            ret.add(decision);
        }
    }
}
