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

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;

/**
 * The {@link ShardsAllocator} class offers methods for allocating shard within a cluster.
 * These methods include moving shards and re-balancing the cluster. It also allows management
 * of shards by their state. 
 */
public class ShardsAllocators extends AbstractComponent implements ShardsAllocator {

    private final GatewayAllocator gatewayAllocator;
    private final ShardsAllocator allocator;

    public ShardsAllocators(GatewayAllocator allocator) {
        this(Settings.Builder.EMPTY_SETTINGS, allocator);
    }

    public ShardsAllocators(Settings settings, GatewayAllocator allocator) {
      this(settings, allocator, new BalancedShardsAllocator(settings));
    }

    @Inject
    public ShardsAllocators(Settings settings, GatewayAllocator gatewayAllocator, ShardsAllocator allocator) {
        super(settings);
        this.gatewayAllocator = gatewayAllocator;
        this.allocator = allocator;
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        gatewayAllocator.applyStartedShards(allocation);
        allocator.applyStartedShards(allocation);
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        gatewayAllocator.applyFailedShards(allocation);
        allocator.applyFailedShards(allocation);
    }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        changed |= gatewayAllocator.allocateUnassigned(allocation);
        changed |= allocator.allocateUnassigned(allocation);
        return changed;
    }

    protected long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        if (allocation.hasPendingAsyncFetch() == false) {
            /*
             * see https://github.com/elastic/elasticsearch/issues/14387
             * if we allow rebalance operations while we are still fetching shard store data
             * we might end up with unnecessary rebalance operations which can be super confusion/frustrating
             * since once the fetches come back we might just move all the shards back again.
             * Therefore we only do a rebalance if we have fetched all information.
             */
            return allocator.rebalance(allocation);
        } else {
            logger.debug("skipping rebalance due to in-flight shard/store fetches");
            return false;
        }
    }

    @Override
    public boolean moveShards(RoutingAllocation allocation) {
        return allocator.moveShards(allocation);
    }
}
