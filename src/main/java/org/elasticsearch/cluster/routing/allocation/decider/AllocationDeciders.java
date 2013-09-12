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
     * Create a new {@link AllocationDeciders} instance
     * @param settings  settings to use
     * @param nodeSettingsService per-node settings to use
     */
    public AllocationDeciders(Settings settings, NodeSettingsService nodeSettingsService) {
        this(settings, ImmutableSet.<AllocationDecider>builder()
                .add(new SameShardAllocationDecider(settings))
                .add(new FilterAllocationDecider(settings, nodeSettingsService))
                .add(new ReplicaAfterPrimaryActiveAllocationDecider(settings))
                .add(new ThrottlingAllocationDecider(settings, nodeSettingsService))
                .add(new RebalanceOnlyWhenActiveAllocationDecider(settings))
                .add(new ClusterRebalanceAllocationDecider(settings))
                .add(new ConcurrentRebalanceAllocationDecider(settings, nodeSettingsService))
                .add(new DisableAllocationDecider(settings, nodeSettingsService))
                .add(new AwarenessAllocationDecider(settings, nodeSettingsService))
                .add(new ShardsLimitAllocationDecider(settings))
                .add(new DiskThresholdDecider(settings, nodeSettingsService))
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
            if (decision != Decision.ALWAYS) {
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
            // the assumption is that a decider that returns the static instance Decision#ALWAYS
            // does not really implements canAllocate
            if (decision != Decision.ALWAYS) {
                ret.add(decision);
            }
        }
        return ret;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRemain(shardRouting, node, allocation);
            if (decision != Decision.ALWAYS) {
                ret.add(decision);
            }
        }
        return ret;
    }
}
