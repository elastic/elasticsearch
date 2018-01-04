/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.cluster.routing.allocation.RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS;

/**
 * A composite {@link AllocationDecider} combining the "decision" of multiple
 * {@link AllocationDecider} implementations into a single allocation decision.
 */
public class AllocationDeciders extends AllocationDecider {

    private final Collection<AllocationDecider> allocations;

    public AllocationDeciders(Settings settings, Collection<AllocationDecider> allocations) {
        super(settings);
        this.allocations = Collections.unmodifiableCollection(allocations);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Can not allocate [{}] on node [{}] due to [{}]", shardRouting, node.node(), allocationDecider.getClass().getSimpleName());
                }
                // short circuit only if debugging is not enabled
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                // the assumption is that a decider that returns the static instance Decision#ALWAYS
                // does not really implements canAllocate
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Shard [{}] should be ignored for node [{}]", shardRouting, node.nodeId());
            }
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRemain(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Shard [{}] can not remain on node [{}] due to [{}]", shardRouting, node.nodeId(), allocationDecider.getClass().getSimpleName());
                }
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canAllocate(IndexMetaData indexMetaData, RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(indexMetaData, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard routing " + shardRouting;

        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider decider : allocations) {
            Decision decision = decider.canForceAllocatePrimary(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Shard [{}] can not be forcefully allocated to node [{}] due to [{}].",
                        shardRouting.shardId(), node.nodeId(), decider.getClass().getSimpleName());
                }
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else if (decision != Decision.ALWAYS
                        && (allocation.getDebugMode() != EXCLUDE_YES_DECISIONS || decision.type() != Decision.Type.YES)) {
                ret.add(decision);
            }
        }
        return ret;
    }
    
    /**
     * Aggregates {@link Decision}} from various {@link AllocationDecider} and selects the best {@link Decision}} based
     * on the below criteria
     * <ul>
     * <li>{@link Decision#NO}} for a particular {@link AllocationDecider}} would be the best decision.
     * <li>{@link Decision#YES}} for a particular {@link AllocationDecider}} would need all the other {@link AllocationDeciders}} to return
     *     {@link Decision#YES}} for a {@link RoutingNode}} to skip iteration for all the {@link ShardRouting}}
     * </ul>
     */
    @Override
    public Decision canRemainOnNode(RoutingNode node, RoutingAllocation allocation) {
        Decision decision = Decision.ALWAYS;
        for (AllocationDecider decider : allocations) {
            Decision throttlingDecision = decider.canMoveAnyShardFromNode(node, allocation);
            if (throttlingDecision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Pre-emptively returning decision [{}] from decider [{}] for node [{}]", throttlingDecision.type(),
                    decider.getClass().getSimpleName(), node.nodeId());
                }
                return Decision.THROTTLE;
            }
        }
        for (AllocationDecider decider : allocations) {
            decision = decider.canRemainOnNode(node, allocation);
            if (decision == Decision.NO)
                break;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Returning decision after iterating all the deciders best decision [{}] for node [{}]", decision.type(),
            node.nodeId());
        }
        return decision;
    }
}
