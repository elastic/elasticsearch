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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * This module configures several {@link AllocationDecider}s
 * that make configuration specific decisions if shards can be allocated on certain nodes.
 *
 * @see Decision
 * @see AllocationDecider
 */
public class AllocationDecidersModule extends AbstractModule {

    private final Settings settings;

    private List<Class<? extends AllocationDecider>> allocations = Lists.newArrayList();

    public AllocationDecidersModule(Settings settings) {
        this.settings = settings;
    }

    public AllocationDecidersModule add(Class<? extends AllocationDecider> allocationDecider) {
        this.allocations.add(allocationDecider);
        return this;
    }

    @Override
    protected void configure() {
        Multibinder<AllocationDecider> allocationMultibinder = Multibinder.newSetBinder(binder(), AllocationDecider.class);
        for (Class<? extends AllocationDecider> deciderClass : DEFAULT_ALLOCATION_DECIDERS) {
            allocationMultibinder.addBinding().to(deciderClass).asEagerSingleton();
        }
        for (Class<? extends AllocationDecider> allocation : allocations) {
            allocationMultibinder.addBinding().to(allocation).asEagerSingleton();
        }

        bind(AllocationDeciders.class).asEagerSingleton();
    }

    public static final ImmutableSet<Class<? extends AllocationDecider>> DEFAULT_ALLOCATION_DECIDERS = ImmutableSet.<Class<? extends AllocationDecider>>builder().
            add(SameShardAllocationDecider.class).
            add(FilterAllocationDecider.class).
            add(ReplicaAfterPrimaryActiveAllocationDecider.class).
            add(ThrottlingAllocationDecider.class).
            add(RebalanceOnlyWhenActiveAllocationDecider.class).
            add(ClusterRebalanceAllocationDecider.class).
            add(ConcurrentRebalanceAllocationDecider.class).
            add(EnableAllocationDecider.class). // new enable allocation logic should proceed old disable allocation logic
            add(DisableAllocationDecider.class).
            add(AwarenessAllocationDecider.class).
            add(ShardsLimitAllocationDecider.class).
            add(NodeVersionAllocationDecider.class).
            add(DiskThresholdDecider.class).
            add(SnapshotInProgressAllocationDecider.class).build();
}
