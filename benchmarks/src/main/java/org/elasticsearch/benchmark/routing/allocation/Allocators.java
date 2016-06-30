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
package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayAllocator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Allocators {
    private static class NoopGatewayAllocator extends GatewayAllocator {
        public static final NoopGatewayAllocator INSTANCE = new NoopGatewayAllocator();

        protected NoopGatewayAllocator() {
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

    private Allocators() {
        throw new AssertionError("Do not instantiate");
    }


    public static AllocationService createAllocationService(Settings settings) throws NoSuchMethodException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
        return createAllocationService(settings, new ClusterSettings(Settings.Builder.EMPTY_SETTINGS, ClusterSettings
            .BUILT_IN_CLUSTER_SETTINGS));
    }

    public static AllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings) throws
        InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return new AllocationService(settings,
            defaultAllocationDeciders(settings, clusterSettings),
            NoopGatewayAllocator.INSTANCE, new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);
    }

    public static AllocationDeciders defaultAllocationDeciders(Settings settings, ClusterSettings clusterSettings) throws
        IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        List<AllocationDecider> list = new ArrayList<>();
        // Keep a deterministic order of allocation deciders for the benchmark
        for (Class<? extends AllocationDecider> deciderClass : ClusterModule.DEFAULT_ALLOCATION_DECIDERS) {
            try {
                Constructor<? extends AllocationDecider> constructor = deciderClass.getConstructor(Settings.class, ClusterSettings
                    .class);
                list.add(constructor.newInstance(settings, clusterSettings));
            } catch (NoSuchMethodException e) {
                Constructor<? extends AllocationDecider> constructor = deciderClass.getConstructor(Settings.class);
                list.add(constructor.newInstance(settings));
            }
        }
        return new AllocationDeciders(settings, list.toArray(new AllocationDecider[0]));

    }

    public static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode("", nodeId, DummyTransportAddress.INSTANCE, attributes, Sets.newHashSet(DiscoveryNode.Role.MASTER,
            DiscoveryNode.Role.DATA), Version.CURRENT);
    }
}
