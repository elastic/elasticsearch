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

package org.elasticsearch.test.gateway;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;

import java.util.List;

/**
 * An allocator used for tests that doesn't do anything
 */
public class NoopGatewayAllocator extends GatewayAllocator {

    public static final NoopGatewayAllocator INSTANCE = new NoopGatewayAllocator();

    protected NoopGatewayAllocator() {
        super(Settings.EMPTY);
    }

    @Override
    public void applyStartedShards(RoutingAllocation allocation, List<ShardRouting> startedShards) {
        // noop
    }

    @Override
    public void applyFailedShards(RoutingAllocation allocation, List<FailedShard> failedShards) {
        // noop
    }

    @Override
    public void allocateUnassigned(RoutingAllocation allocation) {
        // noop
    }
}
