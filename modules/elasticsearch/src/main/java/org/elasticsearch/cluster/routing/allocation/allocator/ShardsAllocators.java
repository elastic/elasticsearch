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

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.NodeAllocations;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.none.NoneGatewayAllocator;

/**
 */
public class ShardsAllocators extends AbstractComponent implements ShardsAllocator {

    private final GatewayAllocator gatewayAllocator;
    private final ShardsAllocator allocator;

    public ShardsAllocators() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public ShardsAllocators(Settings settings) {
        this(settings, new NoneGatewayAllocator(), new EvenShardsCountAllocator(settings));
    }

    @Inject public ShardsAllocators(Settings settings, GatewayAllocator gatewayAllocator, ShardsAllocator allocator) {
        super(settings);
        this.gatewayAllocator = gatewayAllocator;
        this.allocator = allocator;
    }

    @Override public void applyStartedShards(NodeAllocations nodeAllocations, StartedRerouteAllocation allocation) {
        gatewayAllocator.applyStartedShards(nodeAllocations, allocation);
        allocator.applyStartedShards(nodeAllocations, allocation);
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {
        gatewayAllocator.applyFailedShards(nodeAllocations, allocation);
        allocator.applyFailedShards(nodeAllocations, allocation);
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        boolean changed = false;
        changed |= gatewayAllocator.allocateUnassigned(nodeAllocations, allocation);
        changed |= allocator.allocateUnassigned(nodeAllocations, allocation);
        return changed;
    }

    @Override public boolean rebalance(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        return allocator.rebalance(nodeAllocations, allocation);
    }
}
