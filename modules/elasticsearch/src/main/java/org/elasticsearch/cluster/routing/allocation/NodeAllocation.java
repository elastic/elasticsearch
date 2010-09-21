/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * A pluggable logic allowing to control if allocation of a shard is allowed on a specific node.
 *
 * @author kimchy (shay.banon)
 */
public abstract class NodeAllocation extends AbstractComponent {

    public static enum Decision {
        YES {
            @Override public boolean allocate() {
                return true;
            }},
        NO {
            @Override public boolean allocate() {
                return false;
            }},
        THROTTLE {
            @Override public boolean allocate() {
                return false;
            }};

        public abstract boolean allocate();
    }

    protected NodeAllocation(Settings settings) {
        super(settings);
    }

    public void applyStartedShards(NodeAllocations nodeAllocations, StartedRerouteAllocation allocation) {
    }

    public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {

    }

    public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        return false;
    }

    public boolean canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return true;
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.YES;
    }
}
