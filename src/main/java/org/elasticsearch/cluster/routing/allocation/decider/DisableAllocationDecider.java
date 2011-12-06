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

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 */
public class DisableAllocationDecider extends AllocationDecider {

    static {
        MetaData.addDynamicSettings(
                "cluster.routing.allocation.disable_allocation",
                "cluster.routing.allocation.disable_replica_allocation"
        );
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean disableAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_allocation", DisableAllocationDecider.this.disableAllocation);
            if (disableAllocation != DisableAllocationDecider.this.disableAllocation) {
                logger.info("updating [cluster.routing.allocation.disable_allocation] from [{}] to [{}]", DisableAllocationDecider.this.disableAllocation, disableAllocation);
                DisableAllocationDecider.this.disableAllocation = disableAllocation;
            }

            boolean disableReplicaAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_replica_allocation", DisableAllocationDecider.this.disableReplicaAllocation);
            if (disableReplicaAllocation != DisableAllocationDecider.this.disableReplicaAllocation) {
                logger.info("updating [cluster.routing.allocation.disable_replica_allocation] from [{}] to [{}]", DisableAllocationDecider.this.disableReplicaAllocation, disableReplicaAllocation);
                DisableAllocationDecider.this.disableReplicaAllocation = disableReplicaAllocation;
            }
        }
    }

    private volatile boolean disableAllocation;
    private volatile boolean disableReplicaAllocation;

    @Inject
    public DisableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.disableAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_allocation", false);
        this.disableReplicaAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_replica_allocation", false);

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (disableAllocation) {
            return Decision.NO;
        }
        if (disableReplicaAllocation) {
            return shardRouting.primary() ? Decision.YES : Decision.NO;
        }
        return Decision.YES;
    }
}
