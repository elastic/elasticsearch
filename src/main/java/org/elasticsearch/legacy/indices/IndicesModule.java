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

package org.elasticsearch.legacy.indices;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.legacy.action.update.UpdateHelper;
import org.elasticsearch.legacy.common.inject.AbstractModule;
import org.elasticsearch.legacy.common.inject.Module;
import org.elasticsearch.legacy.common.inject.SpawnModules;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.legacy.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.legacy.indices.cache.filter.terms.IndicesTermsFilterCache;
import org.elasticsearch.legacy.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.legacy.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.legacy.indices.fielddata.breaker.InternalCircuitBreakerService;
import org.elasticsearch.legacy.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.legacy.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.legacy.indices.memory.IndexingMemoryController;
import org.elasticsearch.legacy.indices.query.IndicesQueriesModule;
import org.elasticsearch.legacy.indices.recovery.RecoverySettings;
import org.elasticsearch.legacy.indices.recovery.RecoverySource;
import org.elasticsearch.legacy.indices.recovery.RecoveryTarget;
import org.elasticsearch.legacy.indices.store.IndicesStore;
import org.elasticsearch.legacy.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.legacy.indices.ttl.IndicesTTLService;
import org.elasticsearch.legacy.indices.warmer.IndicesWarmer;
import org.elasticsearch.legacy.indices.warmer.InternalIndicesWarmer;

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

        bind(IndicesService.class).to(InternalIndicesService.class).asEagerSingleton();

        bind(RecoverySettings.class).asEagerSingleton();
        bind(RecoveryTarget.class).asEagerSingleton();
        bind(RecoverySource.class).asEagerSingleton();

        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(IndexingMemoryController.class).asEagerSingleton();
        bind(IndicesFilterCache.class).asEagerSingleton();
        bind(IndicesFieldDataCache.class).asEagerSingleton();
        bind(IndicesTermsFilterCache.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(IndicesTTLService.class).asEagerSingleton();
        bind(IndicesWarmer.class).to(InternalIndicesWarmer.class).asEagerSingleton();
        bind(UpdateHelper.class).asEagerSingleton();

        bind(CircuitBreakerService.class).to(InternalCircuitBreakerService.class).asEagerSingleton();
        bind(IndicesFieldDataCacheListener.class).asEagerSingleton();
    }
}
