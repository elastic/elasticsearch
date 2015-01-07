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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationModule;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ClusterModule extends AbstractModule implements SpawnModules {

    private final Settings settings;
    public static final String CLUSTER_SERVICE_IMPL = "cluster.info.service.type";

    private Set<Class<? extends IndexTemplateFilter>> indexTemplateFilters = new HashSet<>();

    public ClusterModule(Settings settings) {
        this.settings = settings;
    }

    public void registerIndexTemplateFilter(Class<? extends IndexTemplateFilter> indexTemplateFilter) {
        indexTemplateFilters.add(indexTemplateFilter);
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new AllocationModule(settings),
                new ClusterDynamicSettingsModule(),
                new IndexDynamicSettingsModule());
    }

    @Override
    protected void configure() {
        bind(DiscoveryNodeService.class).asEagerSingleton();
        bind(ClusterService.class).to(InternalClusterService.class).asEagerSingleton();
        bind(OperationRouting.class).asEagerSingleton();
        bind(MetaDataService.class).asEagerSingleton();
        bind(MetaDataCreateIndexService.class).asEagerSingleton();
        bind(MetaDataDeleteIndexService.class).asEagerSingleton();
        bind(MetaDataIndexStateService.class).asEagerSingleton();
        bind(MetaDataMappingService.class).asEagerSingleton();
        bind(MetaDataIndexAliasesService.class).asEagerSingleton();
        bind(MetaDataUpdateSettingsService.class).asEagerSingleton();
        bind(MetaDataIndexTemplateService.class).asEagerSingleton();

        bind(RoutingService.class).asEagerSingleton();

        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeIndexDeletedAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();

        bind(ClusterInfoService.class).to(settings.getAsClass(CLUSTER_SERVICE_IMPL, InternalClusterInfoService.class)).asEagerSingleton();

        Multibinder<IndexTemplateFilter> mbinder = Multibinder.newSetBinder(binder(), IndexTemplateFilter.class);
        for (Class<? extends IndexTemplateFilter> indexTemplateFilter : indexTemplateFilters) {
            mbinder.addBinding().to(indexTemplateFilter);
        }
    }
}