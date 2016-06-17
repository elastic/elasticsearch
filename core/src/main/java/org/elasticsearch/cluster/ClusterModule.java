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

import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.tasks.TaskPersistenceService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Configures classes and services that affect the entire cluster.
 */
public class ClusterModule extends AbstractModule {

    public static final String EVEN_SHARD_COUNT_ALLOCATOR = "even_shard";
    public static final String BALANCED_ALLOCATOR = "balanced"; // default
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING =
        new Setting<>("cluster.routing.allocation.type", BALANCED_ALLOCATOR, Function.identity(), Property.NodeScope);
    public static final List<Class<? extends AllocationDecider>> DEFAULT_ALLOCATION_DECIDERS =
        Collections.unmodifiableList(Arrays.asList(
            MaxRetryAllocationDecider.class,
            SameShardAllocationDecider.class,
            FilterAllocationDecider.class,
            ReplicaAfterPrimaryActiveAllocationDecider.class,
            ThrottlingAllocationDecider.class,
            RebalanceOnlyWhenActiveAllocationDecider.class,
            ClusterRebalanceAllocationDecider.class,
            ConcurrentRebalanceAllocationDecider.class,
            EnableAllocationDecider.class,
            AwarenessAllocationDecider.class,
            ShardsLimitAllocationDecider.class,
            NodeVersionAllocationDecider.class,
            DiskThresholdDecider.class,
            SnapshotInProgressAllocationDecider.class));

    private final Settings settings;
    private final ExtensionPoint.SelectedType<ShardsAllocator> shardsAllocators = new ExtensionPoint.SelectedType<>("shards_allocator", ShardsAllocator.class);
    private final ExtensionPoint.ClassSet<AllocationDecider> allocationDeciders = new ExtensionPoint.ClassSet<>("allocation_decider", AllocationDecider.class, AllocationDeciders.class);
    private final ExtensionPoint.ClassSet<IndexTemplateFilter> indexTemplateFilters = new ExtensionPoint.ClassSet<>("index_template_filter", IndexTemplateFilter.class);
    private final ClusterService clusterService;

    // pkg private so tests can mock
    Class<? extends ClusterInfoService> clusterInfoServiceImpl = InternalClusterInfoService.class;

    public ClusterModule(Settings settings, ClusterService clusterService) {
        this.settings = settings;
        for (Class<? extends AllocationDecider> decider : ClusterModule.DEFAULT_ALLOCATION_DECIDERS) {
            registerAllocationDecider(decider);
        }
        registerShardsAllocator(ClusterModule.BALANCED_ALLOCATOR, BalancedShardsAllocator.class);
        registerShardsAllocator(ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR, BalancedShardsAllocator.class);
        this.clusterService = clusterService;
    }

    public void registerAllocationDecider(Class<? extends AllocationDecider> allocationDecider) {
        allocationDeciders.registerExtension(allocationDecider);
    }

    public void registerShardsAllocator(String name, Class<? extends ShardsAllocator> clazz) {
        shardsAllocators.registerExtension(name, clazz);
    }

    public void registerIndexTemplateFilter(Class<? extends IndexTemplateFilter> indexTemplateFilter) {
        indexTemplateFilters.registerExtension(indexTemplateFilter);
    }

    @Override
    protected void configure() {
        // bind ShardsAllocator
        String shardsAllocatorType = shardsAllocators.bindType(binder(), settings, ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.BALANCED_ALLOCATOR);
        if (shardsAllocatorType.equals(ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR)) {
            final ESLogger logger = Loggers.getLogger(getClass(), settings);
            logger.warn("{} allocator has been removed in 2.0 using {} instead", ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR, ClusterModule.BALANCED_ALLOCATOR);
        }
        allocationDeciders.bind(binder());
        indexTemplateFilters.bind(binder());

        bind(ClusterInfoService.class).to(clusterInfoServiceImpl).asEagerSingleton();
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).asEagerSingleton();
        bind(DiscoveryNodeService.class).asEagerSingleton();
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetaDataCreateIndexService.class).asEagerSingleton();
        bind(MetaDataDeleteIndexService.class).asEagerSingleton();
        bind(MetaDataIndexStateService.class).asEagerSingleton();
        bind(MetaDataMappingService.class).asEagerSingleton();
        bind(MetaDataIndexAliasesService.class).asEagerSingleton();
        bind(MetaDataUpdateSettingsService.class).asEagerSingleton();
        bind(MetaDataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).asEagerSingleton();
        bind(RoutingService.class).asEagerSingleton();
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskPersistenceService.class).asEagerSingleton();
    }
}
