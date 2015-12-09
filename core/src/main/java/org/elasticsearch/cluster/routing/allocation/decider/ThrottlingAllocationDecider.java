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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link ThrottlingAllocationDecider} controls the recovery process per node in
 * the cluster. It exposes two settings via the cluster update API that allow
 * changes in real-time:
 * <ul>
 * <li><tt>cluster.routing.allocation.node_initial_primaries_recoveries</tt> -
 * restricts the number of initial primary shard recovery operations on a single
 * node. The default is <tt>4</tt></li>
 * <li><tt>cluster.routing.allocation.node_concurrent_recoveries</tt> -
 * restricts the number of total concurrent shards initializing on a single node. The
 * default is <tt>2</tt></li>
 * </ul>
 * <p>
 * If one of the above thresholds is exceeded per node this allocation decider
 * will return {@link Decision#THROTTLE} as a hit to upstream logic to throttle
 * the allocation process to prevent overloading nodes due to too many concurrent recovery
 * processes.
 */
public class ThrottlingAllocationDecider extends AllocationDecider {

    public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES = 2;
    public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES = 4;
    public static final String NAME = "throttling";
    public static final String CLUSTER_ROUTING_ALLOCATION_CONCURRENT_RECOVERIES = "cluster.routing.allocation.concurrent_recoveries";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING = Setting.intSetting("cluster.routing.allocation.node_initial_primaries_recoveries", DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES, true, Setting.Scope.Cluster);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING = new Setting<>("cluster.routing.allocation.node_concurrent_recoveries", (s) -> s.get(CLUSTER_ROUTING_ALLOCATION_CONCURRENT_RECOVERIES,Integer.toString(DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES)), Integer::parseInt, true, Setting.Scope.Cluster);

    private volatile int primariesInitialRecoveries;
    private volatile int concurrentRecoveries;

    @Inject
    public ThrottlingAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.primariesInitialRecoveries = CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.get(settings);
        this.concurrentRecoveries = CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.get(settings);
        logger.debug("using node_concurrent_recoveries [{}], node_initial_primaries_recoveries [{}]", concurrentRecoveries, primariesInitialRecoveries);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING, this::setPrimariesInitialRecoveries);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING, this::setConcurrentRecoveries);
    }

    private void setConcurrentRecoveries(int concurrentRecoveries) {
        this.concurrentRecoveries = concurrentRecoveries;
    }

    private void setPrimariesInitialRecoveries(int primariesInitialRecoveries) {
        this.primariesInitialRecoveries = primariesInitialRecoveries;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            assert shardRouting.unassigned() || shardRouting.active();
            if (shardRouting.unassigned()) {
                // primary is unassigned, means we are going to do recovery from gateway
                // count *just the primary* currently doing recovery on the node and check against concurrent_recoveries
                int primariesInRecovery = 0;
                for (ShardRouting shard : node) {
                    // when a primary shard is INITIALIZING, it can be because of *initial recovery* or *relocation from another node*
                    // we only count initial recoveries here, so we need to make sure that relocating node is null
                    if (shard.initializing() && shard.primary() && shard.relocatingNodeId() == null) {
                        primariesInRecovery++;
                    }
                }
                if (primariesInRecovery >= primariesInitialRecoveries) {
                    return allocation.decision(Decision.THROTTLE, NAME, "too many primaries currently recovering [%d], limit: [%d]",
                            primariesInRecovery, primariesInitialRecoveries);
                } else {
                    return allocation.decision(Decision.YES, NAME, "below primary recovery limit of [%d]", primariesInitialRecoveries);
                }
            }
        }

        // either primary or replica doing recovery (from peer shard)

        // count the number of recoveries on the node, its for both target (INITIALIZING) and source (RELOCATING)
        return canAllocate(node, allocation);
    }

    @Override
    public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
        int currentRecoveries = 0;
        for (ShardRouting shard : node) {
            if (shard.initializing()) {
                currentRecoveries++;
            }
        }
        if (currentRecoveries >= concurrentRecoveries) {
            return allocation.decision(Decision.THROTTLE, NAME, "too many shards currently recovering [%d], limit: [%d]",
                    currentRecoveries, concurrentRecoveries);
        } else {
            return allocation.decision(Decision.YES, NAME, "below shard recovery limit of [%d]", concurrentRecoveries);
        }
    }
}
