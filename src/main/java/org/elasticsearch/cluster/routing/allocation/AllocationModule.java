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

package org.elasticsearch.cluster.routing.allocation;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecidersModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;

/**
 * The {@link AllocationModule} manages several
 * modules related to the allocation process. To do so
 * it manages a {@link ShardsAllocatorModule} and an {@link AllocationDecidersModule}.
 */
public class AllocationModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    //TODO: Documentation
    public AllocationModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new ShardsAllocatorModule(settings), new AllocationDecidersModule(settings));
    }

    @Override
    protected void configure() {
        bind(AllocationService.class).asEagerSingleton();
    }
}
