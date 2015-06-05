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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * Similar to the {@link ClusterRebalanceAllocationDecider} this
 * {@link AllocationDecider} controls the number of currently in-progress
 * re-balance (relocation) operations and restricts node allocations if the
 * configured threashold is reached. The default number of concurrent rebalance
 * operations is set to <tt>2</tt>
 * <p/>
 * Re-balance operations can be controlled in real-time via the cluster update API using
 * <tt>cluster.routing.allocation.cluster_concurrent_rebalance</tt>. Iff this
 * setting is set to <tt>-1</tt> the number of concurrent re-balance operations
 * are unlimited.
 */
public class ConcurrentRebalanceAllocationDecider extends AllocationDecider {

    public static final String NAME = "concurrent_rebalance";

    public static final String CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE = "cluster.routing.allocation.cluster_concurrent_rebalance";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int clusterConcurrentRebalance = settings.getAsInt(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, ConcurrentRebalanceAllocationDecider.this.clusterConcurrentRebalance);
            if (clusterConcurrentRebalance != ConcurrentRebalanceAllocationDecider.this.clusterConcurrentRebalance) {
                logger.info("updating [cluster.routing.allocation.cluster_concurrent_rebalance] from [{}], to [{}]", ConcurrentRebalanceAllocationDecider.this.clusterConcurrentRebalance, clusterConcurrentRebalance);
                ConcurrentRebalanceAllocationDecider.this.clusterConcurrentRebalance = clusterConcurrentRebalance;
            }
        }
    }

    private volatile int clusterConcurrentRebalance;

    @Inject
    public ConcurrentRebalanceAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.clusterConcurrentRebalance = settings.getAsInt(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 2);
        logger.debug("using [cluster_concurrent_rebalance] with [{}]", clusterConcurrentRebalance);
        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (clusterConcurrentRebalance == -1) {
            return allocation.decision(Decision.YES, NAME, "all concurrent rebalances are allowed");
        }
        if (allocation.routingNodes().getRelocatingShardCount() >= clusterConcurrentRebalance) {
            return allocation.decision(Decision.NO, NAME, "too man concurrent rebalances [%d], limit: [%d]",
                    allocation.routingNodes().getRelocatingShardCount(), clusterConcurrentRebalance);
        }
        return allocation.decision(Decision.YES, NAME, "below threshold [%d] for concurrent rebalances", clusterConcurrentRebalance);
    }
}
