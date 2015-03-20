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

package org.elasticsearch.indices;

import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.ttl.IndicesTTLService;

/**
 *
 */
public class IndicesModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public IndicesModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new IndicesQueriesModule(), new IndicesAnalysisModule());
    }

    @Override
    protected void configure() {
        bind(IndicesLifecycle.class).to(InternalIndicesLifecycle.class).asEagerSingleton();

        bind(IndicesService.class).asEagerSingleton();

        bind(RecoverySettings.class).asEagerSingleton();
        bind(RecoveryTarget.class).asEagerSingleton();
        bind(RecoverySource.class).asEagerSingleton();
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(IndexingMemoryController.class).asEagerSingleton();
        bind(IndicesFilterCache.class).asEagerSingleton();
        bind(IndicesQueryCache.class).asEagerSingleton();
        bind(IndicesFieldDataCache.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(IndicesTTLService.class).asEagerSingleton();
        bind(IndicesWarmer.class).asEagerSingleton();
        bind(UpdateHelper.class).asEagerSingleton();

        bind(IndicesFieldDataCacheListener.class).asEagerSingleton();
    }
}
