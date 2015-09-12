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

import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;

/**
 * An allocator used for tests that doesn't do anything
 */
public class NoopGatewayAllocator extends GatewayAllocator {

    public static final NoopGatewayAllocator INSTANCE = new NoopGatewayAllocator();

    private NoopGatewayAllocator() {
        super(Settings.EMPTY, null, null);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        // noop
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        // noop
    }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        return false;
    }
}
