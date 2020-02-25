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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A composite {@link AllocationDecider} combining the "decision" of multiple
 * {@link AllocationDecider} implementations into a single allocation decision.
 */
public class AllocationDeciders /*extends AllocationDecider*/ {

    private static final Logger logger = LogManager.getLogger(AllocationDeciders.class);

    private final List<AllocationDecider> allocations;

    /**
     * Create deciders based on an ordered collection of deciders.
     */
    public AllocationDeciders(Collection<AllocationDecider> allocations) {
        this.allocations = Collections.unmodifiableList(new ArrayList<>(allocations));
    }

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
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    // todo: remove canAllocate and canRemain from here? Can lead to performance issues if using these unqualified.
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shardDecisions(shardRouting, allocation).canAllocate(node);
    }

    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shardDecisions(shardRouting, allocation).canRemain(node);
    }

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
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    public Decision shouldAutoExpandToNode(IndexMetaData indexMetaData, DiscoveryNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.shouldAutoExpandToNode(indexMetaData, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
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
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

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
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
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
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    private void addDecision(Decision.Multi ret, Decision decision, RoutingAllocation allocation) {
        if (shouldAddDecision(decision, allocation)) {
            ret.add(decision);
        }
    }

    private boolean shouldAddDecision(Decision decision, RoutingAllocation allocation) {
        // We never add ALWAYS decisions and only add YES decisions when requested by debug mode (since Multi default is YES).
        return decision != Decision.ALWAYS
            && (allocation.getDebugMode() == RoutingAllocation.DebugMode.ON || decision.type() != Decision.Type.YES);
    }

    public ShardDecisions shardDecisions(ShardRouting shardRouting, RoutingAllocation allocation) {
        class LazyShardDecisions implements ShardDecisions {
            private ShardDecisions cached;

            public ShardDecisions get() {
                if (cached == null) {
                    this.cached = calculateDecisions(shardRouting, allocation);
                }
                return cached;
            }

            @Override
            public Decision canAllocate(RoutingNode node) {
                // we do this here, since we do not want this to affect tiered allocation like awareness.
                if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
                    return Decision.NO;
                }

                return get().canAllocate(node);
            }

            @Override
            public Decision canRemain(RoutingNode node) {
                if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Shard [{}] should be ignored for node [{}]", shardRouting, node.nodeId());
                    }
                    return Decision.NO;
                }
                return get().canRemain(node);
            }
        }
        return new LazyShardDecisions();
    }

    private ShardDecisions calculateDecisions(ShardRouting shardRouting, RoutingAllocation allocation) {
        // todo: make this evaluation more lazy, possibly by keeping the decision for every node for tiered
        //  deciders.
        Set<RoutingNode> candidates = StreamSupport.stream(allocation.routingNodes().spliterator(), false)
            .collect(Collectors.toSet());
        Map<RoutingNode, Decision> decisions = candidates.stream().collect(Collectors.toMap(Function.identity(), k -> Decision.ALWAYS));
        Decision remainDecision = Decision.ALWAYS;
        RoutingNode currentNode = shardRouting.assignedToNode()
            ? allocation.routingNodes().node(shardRouting.currentNodeId())
            : null;

        BiFunction<Decision, Decision, Decision> decisionMerger = (oldDecision, newDecision) -> mergeDecision(oldDecision, newDecision,
            allocation);
        TieredAllocationDecider.LowerTierDecider lowerTierDecider = new TieredAllocationDecider.LowerTierDecider() {
            @Override
            public Set<RoutingNode> candidates() {
                return candidates;
            }

            @Override
            public Decision decisionFor(RoutingNode node) {
                return decisions.getOrDefault(node, Decision.YES);
            }
        };

        for (AllocationDecider decider : allocations) {
            boolean tiered = decider instanceof TieredAllocationDecider;
            Map<RoutingNode, Decision> newDecisions = new HashMap<>();
            Set<RoutingNode> nodesToCheck = allocation.debugDecision() ? decisions.keySet() : candidates;
            for (RoutingNode node : nodesToCheck) {
                Decision decision = tiered
                    ? ((TieredAllocationDecider) decider).canAllocate(shardRouting, node, allocation, lowerTierDecider)
                    : decider.canAllocate(shardRouting, node, allocation);
                if (decision == Decision.NO && logger.isTraceEnabled()) {
                    logger.trace("Can not allocate [{}] on node [{}] due to [{}]: [{}]",
                        shardRouting, node.node(), decider.getClass().getSimpleName(), decision);
                }

                newDecisions.put(node, decision);
            }
            if (currentNode != null && (remainDecision.type() != Decision.Type.NO || allocation.debugDecision())) {
                Decision decision = tiered
                    ? ((TieredAllocationDecider) decider).canRemain(shardRouting, currentNode, allocation, lowerTierDecider)
                    : decider.canRemain(shardRouting, currentNode, allocation);
                if (decision == Decision.NO && logger.isTraceEnabled()) {
                    logger.trace("Can not remain [{}] on node [{}] due to [{}]: [{}]",
                        shardRouting, currentNode.node(), decider.getClass().getSimpleName(), decision);
                }

                remainDecision = decisionMerger.apply(remainDecision, decision);
            }

            candidates.removeIf(n -> newDecisions.get(n) == Decision.NO);
            newDecisions.forEach((n, d) -> decisions.merge(n, d, decisionMerger));
        }

        final Decision finalRemainDecision = remainDecision;
        return new ShardDecisions() {
            @Override
            public Decision canAllocate(RoutingNode node) {
                return toMulti(decisions.get(node), allocation);
            }

            @Override
            public Decision canRemain(RoutingNode node) {
                if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
                    throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
                }

                return toMulti(finalRemainDecision, allocation);
            }
        };
    }

    private Decision toMulti(Decision decision, RoutingAllocation allocation) {
        // todo: we can likely remove this compatibility.
        if (decision instanceof Decision.Single) {
            Decision.Multi result = new Decision.Multi();
            if (shouldAddDecision(decision, allocation)) {
                result.add(decision);
            }
            decision = result;
        }
        return decision;
    }

    /**
     * Get a decisions object for the allocation. Returned object is only valid until the allocation/routing nodes changes.
     */
    public Decisions decisions(RoutingAllocation allocation) {
        Map<ShardRouting, ShardDecisions> cache = new HashMap<>();
        return shardRouting -> cache.computeIfAbsent(shardRouting, s -> shardDecisions(s, allocation));
    }

    /**
     * A representation of canAllocate and canRemain decisions at shard level.
     * Notice that this might be lazy evaluated and should not be used after RoutingNodes have changed.
     */
    public interface ShardDecisions {
        Decision canAllocate(RoutingNode node);

        Decision canRemain(RoutingNode node);
    }

    /**
     * A representation of canAllocate and canRemain decisions across shards.
     * Notice that this might be lazy evaluated and should not be used after RoutingNodes have changed.
     */
    public interface Decisions {
        ShardDecisions shard(ShardRouting shardRouting);
    }

    private Decision mergeDecision(Decision oldDecision, Decision newDecision, RoutingAllocation allocation) {
        if (shouldAddDecision(newDecision, allocation) == false) {
            return oldDecision;
        } else if (oldDecision == Decision.ALWAYS) {
            return newDecision;
        } else if (allocation.debugDecision() == false) {
            // higherThan means yes > throttle > no
            if (oldDecision.type().higherThan(newDecision.type())) {
                return newDecision;
            } else {
                return oldDecision;
            }
        } else {
            Decision.Multi ret = new Decision.Multi();
            oldDecision.getDecisions().forEach(ret::add);
            ret.add(newDecision);
            return ret;
        }
    }
}
