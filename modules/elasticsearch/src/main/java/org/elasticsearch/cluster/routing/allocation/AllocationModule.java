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

import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class AllocationModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    private List<Class<? extends NodeAllocation>> allocations = Lists.newArrayList();

    public AllocationModule(Settings settings) {
        this.settings = settings;
    }

    @Override public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new ShardsAllocatorModule(settings));
    }

    public void addNodeAllocation(Class<? extends NodeAllocation> nodeAllocation) {
        allocations.add(nodeAllocation);
    }

    @Override protected void configure() {
        bind(AllocationService.class).asEagerSingleton();

        Multibinder<NodeAllocation> allocationMultibinder = Multibinder.newSetBinder(binder(), NodeAllocation.class);
        allocationMultibinder.addBinding().to(SameShardNodeAllocation.class);
        allocationMultibinder.addBinding().to(ReplicaAfterPrimaryActiveNodeAllocation.class);
        allocationMultibinder.addBinding().to(ThrottlingNodeAllocation.class);
        allocationMultibinder.addBinding().to(RebalanceOnlyWhenActiveNodeAllocation.class);
        allocationMultibinder.addBinding().to(ClusterRebalanceNodeAllocation.class);
        allocationMultibinder.addBinding().to(ConcurrentRebalanceNodeAllocation.class);
        for (Class<? extends NodeAllocation> allocation : allocations) {
            allocationMultibinder.addBinding().to(allocation);
        }

        bind(NodeAllocations.class).asEagerSingleton();
    }
}
