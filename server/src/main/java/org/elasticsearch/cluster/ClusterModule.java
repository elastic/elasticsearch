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
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
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
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.tasks.TaskResultsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Configures classes and services that affect the entire cluster.
 */
public class ClusterModule extends AbstractModule {

    public static final String BALANCED_ALLOCATOR = "balanced"; // default
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING =
        new Setting<>("cluster.routing.allocation.type", BALANCED_ALLOCATOR, Function.identity(), Property.NodeScope);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    // pkg private for tests
    final Collection<AllocationDecider> deciderList;
    final ShardsAllocator shardsAllocator;

    public ClusterModule(Settings settings, ClusterService clusterService, List<ClusterPlugin> clusterPlugins,
                         ClusterInfoService clusterInfoService) {
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(settings, deciderList);
        this.shardsAllocator = createShardsAllocator(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
        this.allocationService = new AllocationService(settings, allocationDeciders, shardsAllocator, clusterInfoService);
    }

    public static Map<String, Supplier<ClusterState.Custom>> getClusterStateCustomSuppliers(List<ClusterPlugin> clusterPlugins) {
        final Map<String, Supplier<ClusterState.Custom>> customSupplier = new HashMap<>();
        customSupplier.put(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress::new);
        customSupplier.put(RestoreInProgress.TYPE, RestoreInProgress::new);
        customSupplier.put(SnapshotsInProgress.TYPE, SnapshotsInProgress::new);
        for (ClusterPlugin plugin : clusterPlugins) {
            Map<String, Supplier<ClusterState.Custom>> initialCustomSupplier = plugin.getInitialClusterStateCustomSupplier();
            for (String key : initialCustomSupplier.keySet()) {
                if (customSupplier.containsKey(key)) {
                    throw new IllegalStateException("custom supplier key [" + key + "] is registered more than once");
                }
            }
            customSupplier.putAll(initialCustomSupplier);
        }
        return Collections.unmodifiableMap(customSupplier);
    }

    public static List<Entry> getNamedWriteables() {
        List<Entry> entries = new ArrayList<>();
        // Cluster State
        registerClusterCustom(entries, SnapshotsInProgress.TYPE, SnapshotsInProgress::new, SnapshotsInProgress::readDiffFrom);
        registerClusterCustom(entries, RestoreInProgress.TYPE, RestoreInProgress::new, RestoreInProgress::readDiffFrom);
        registerClusterCustom(entries, SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress::new,
            SnapshotDeletionsInProgress::readDiffFrom);
        // Metadata
        registerMetaDataCustom(entries, RepositoriesMetaData.TYPE, RepositoriesMetaData::new, RepositoriesMetaData::readDiffFrom);
        registerMetaDataCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerMetaDataCustom(entries, ScriptMetaData.TYPE, ScriptMetaData::new, ScriptMetaData::readDiffFrom);
        registerMetaDataCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(RepositoriesMetaData.TYPE),
            RepositoriesMetaData::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IngestMetadata.TYPE),
            IngestMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(ScriptMetaData.TYPE),
            ScriptMetaData::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexGraveyard.TYPE),
            IndexGraveyard::fromXContent));
        return entries;
    }

    private static <T extends ClusterState.Custom> void registerClusterCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                       Reader<NamedDiff> diffReader) {
        registerCustom(entries, ClusterState.Custom.class, name, reader, diffReader);
    }

    private static <T extends MetaData.Custom> void registerMetaDataCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                       Reader<NamedDiff> diffReader) {
        registerCustom(entries, MetaData.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(List<Entry> entries, Class<T> category, String name,
                                                                  Reader<? extends T> reader, Reader<NamedDiff> diffReader) {
        entries.add(new Entry(category, name, reader));
        entries.add(new Entry(NamedDiff.class, name, diffReader));
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    // TODO: this is public so allocation benchmark can access the default deciders...can we do that in another way?
    /** Return a new {@link AllocationDecider} instance with builtin deciders as well as those from plugins. */
    public static Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings,
                                                                         List<ClusterPlugin> clusterPlugins) {
        // collect deciders by class so that we can detect duplicates
        Map<Class, AllocationDecider> deciders = new LinkedHashMap<>();
        addAllocationDecider(deciders, new MaxRetryAllocationDecider(settings));
        addAllocationDecider(deciders, new ResizeAllocationDecider(settings));
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider(settings));
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider(settings));
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeVersionAllocationDecider(settings));
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider(settings));
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider(settings));
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new SameShardAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new AwarenessAllocationDecider(settings, clusterSettings));

        clusterPlugins.stream()
            .flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream())
            .forEach(d -> addAllocationDecider(deciders, d));

        return deciders.values();
    }

    /** Add the given allocation decider to the given deciders collection, erroring if the class name is already used. */
    private static void addAllocationDecider(Map<Class, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    private static ShardsAllocator createShardsAllocator(Settings settings, ClusterSettings clusterSettings,
                                                         List<ClusterPlugin> clusterPlugins) {
        Map<String, Supplier<ShardsAllocator>> allocators = new HashMap<>();
        allocators.put(BALANCED_ALLOCATOR, () -> new BalancedShardsAllocator(settings, clusterSettings));

        for (ClusterPlugin plugin : clusterPlugins) {
            plugin.getShardsAllocators(settings, clusterSettings).forEach((k, v) -> {
                if (allocators.put(k, v) != null) {
                    throw new IllegalArgumentException("ShardsAllocator [" + k + "] already defined");
                }
            });
        }
        String allocatorName = SHARDS_ALLOCATOR_TYPE_SETTING.get(settings);
        Supplier<ShardsAllocator> allocatorSupplier = allocators.get(allocatorName);
        if (allocatorSupplier == null) {
            throw new IllegalArgumentException("Unknown ShardsAllocator [" + allocatorName + "]");
        }
        return Objects.requireNonNull(allocatorSupplier.get(),
            "ShardsAllocator factory for [" + allocatorName + "] returned null");
    }

    public AllocationService getAllocationService() {
        return allocationService;
    }

    @Override
    protected void configure() {
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).toInstance(allocationService);
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetaDataCreateIndexService.class).asEagerSingleton();
        bind(MetaDataDeleteIndexService.class).asEagerSingleton();
        bind(MetaDataIndexStateService.class).asEagerSingleton();
        bind(MetaDataMappingService.class).asEagerSingleton();
        bind(MetaDataIndexAliasesService.class).asEagerSingleton();
        bind(MetaDataUpdateSettingsService.class).asEagerSingleton();
        bind(MetaDataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(RoutingService.class).asEagerSingleton();
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(allocationDeciders);
        bind(ShardsAllocator.class).toInstance(shardsAllocator);
    }
}
