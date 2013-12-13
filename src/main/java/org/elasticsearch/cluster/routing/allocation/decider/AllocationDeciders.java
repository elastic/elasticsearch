/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Set;

/**
 * A composite {@link AllocationDecider} combining the "decision" of multiple
 * {@link AllocationDecider} implementations into a single allocation decision.
 */
public class AllocationDeciders extends AllocationDecider {

    private final AllocationDecider[] allocations;

    /**
     * Create a new {@link AllocationDeciders} instance. The different deciders
     * should be added in order, as looping over them will stop when the first 
     * return a {@link Decision#THROTTLE} or {@link Decision#NO}. For performance
     * reasons, those more likely to return either of these, and those with
     * cheap execution should be executed first.
     *
     * Performance characteristics:
     * {@link ConcurrentRebalanceAllocationDecider} numerical comparison of a counter in {@link org.elasticsearch.cluster.routing.RoutingNodes},
     * constant performance, likely to be triggered.
     * {@link DisableAllocationDecider} lookup of setting. Constant performance, not as
     * likely to be triggered.
     * {@link ClusterRebalanceAllocationDecider} checks for unassigned primaries, inactive primaries and
     * a rebalance already happening in replica set. 
     * {@link DiskThresholdDecider} one numerical comparison per node in cluster.
     * {@link FilterAllocationDecider} checks all allocation include/exclude filters in the cluster against the
     * node's attributes.
     * {@link RebalanceOnlyWhenActiveAllocationDecider} checks if all shards are active.
     * {@link ReplicaAfterPrimaryActiveAllocationDecider} finds primary in replica set, checks whether it
     * is started.
     * {@link ShardsLimitAllocationDecider} loops over shards allocated on a node, filters out non-relocating
     * shards of the same index to do a count comparison.
     * {@link AwarenessAllocationDecider} loops over all shards in cluster.
     * {@link SameShardAllocationDecider} loops over shards on node.
     * {@link ThrottlingAllocationDecider} checks primaries initializing (looping over shards on node) for a primary
     * to be allocated, for replicas loops over all shards on node.
     *
     * @param settings            settings to use
     * @param nodeSettingsService per-node settings to use
     */
    public AllocationDeciders(Settings settings, NodeSettingsService nodeSettingsService) {
        this(settings, ImmutableSet.<AllocationDecider>builder()
                .add(new ConcurrentRebalanceAllocationDecider(settings, nodeSettingsService))
                .add(new DisableAllocationDecider(settings, nodeSettingsService))
                .add(new ClusterRebalanceAllocationDecider(settings))
                .add(new DiskThresholdDecider(settings, nodeSettingsService))
                .add(new FilterAllocationDecider(settings, nodeSettingsService))
                .add(new RebalanceOnlyWhenActiveAllocationDecider(settings))
                .add(new ReplicaAfterPrimaryActiveAllocationDecider(settings))
                .add(new ShardsLimitAllocationDecider(settings))
                .add(new AwarenessAllocationDecider(settings, nodeSettingsService))
                .add(new SameShardAllocationDecider(settings))
                .add(new ThrottlingAllocationDecider(settings, nodeSettingsService))
                .build()
        );
    }

    @Inject
    public AllocationDeciders(Settings settings, Set<AllocationDecider> allocations) {
        super(settings);
        this.allocations = allocations.toArray(new AllocationDecider[allocations.size()]);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                return decision;
            } else if (decision != Decision.ALWAYS) {
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
                    logger.trace("Can not allocate [{}] on node [{}] due to [{}]", shardRouting, node.nodeId(), allocationDecider.getClass().getSimpleName());
                }
                return decision;
            } else if (decision != Decision.ALWAYS) {
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
                return decision;
            } else if (decision != Decision.ALWAYS) {
                ret.add(decision);
            }
        }
        return ret;
    }
}
