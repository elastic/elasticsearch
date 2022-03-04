/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
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
import org.elasticsearch.cluster.routing.allocation.decider.NodeReplacementAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeShutdownAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksNodeService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.script.ScriptMetadata;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.ArrayList;
import java.util.Collection;
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
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING = new Setting<>(
        "cluster.routing.allocation.type",
        BALANCED_ALLOCATOR,
        Function.identity(),
        Property.NodeScope
    );

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final List<ClusterPlugin> clusterPlugins;
    private final MetadataDeleteIndexService metadataDeleteIndexService;
    // pkg private for tests
    final Collection<AllocationDecider> deciderList;
    final ShardsAllocator shardsAllocator;

    public ClusterModule(
        Settings settings,
        ClusterService clusterService,
        List<ClusterPlugin> clusterPlugins,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ThreadContext threadContext,
        SystemIndices systemIndices
    ) {
        this.clusterPlugins = clusterPlugins;
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(deciderList);
        this.shardsAllocator = createShardsAllocator(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext, systemIndices);
        this.allocationService = new AllocationService(allocationDeciders, shardsAllocator, clusterInfoService, snapshotsInfoService);
        this.metadataDeleteIndexService = new MetadataDeleteIndexService(settings, clusterService, allocationService);
    }

    public static List<Entry> getNamedWriteables() {
        List<Entry> entries = new ArrayList<>();
        // Cluster State
        registerClusterCustom(entries, SnapshotsInProgress.TYPE, SnapshotsInProgress::new, SnapshotsInProgress::readDiffFrom);
        registerClusterCustom(entries, RestoreInProgress.TYPE, RestoreInProgress::new, RestoreInProgress::readDiffFrom);
        registerClusterCustom(
            entries,
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress::new,
            SnapshotDeletionsInProgress::readDiffFrom
        );
        registerClusterCustom(
            entries,
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress::new,
            RepositoryCleanupInProgress::readDiffFrom
        );
        // Metadata
        registerMetadataCustom(entries, RepositoriesMetadata.TYPE, RepositoriesMetadata::new, RepositoriesMetadata::readDiffFrom);
        registerMetadataCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerMetadataCustom(entries, ScriptMetadata.TYPE, ScriptMetadata::new, ScriptMetadata::readDiffFrom);
        registerMetadataCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        registerMetadataCustom(
            entries,
            PersistentTasksCustomMetadata.TYPE,
            PersistentTasksCustomMetadata::new,
            PersistentTasksCustomMetadata::readDiffFrom
        );
        registerMetadataCustom(
            entries,
            ComponentTemplateMetadata.TYPE,
            ComponentTemplateMetadata::new,
            ComponentTemplateMetadata::readDiffFrom
        );
        registerMetadataCustom(
            entries,
            ComposableIndexTemplateMetadata.TYPE,
            ComposableIndexTemplateMetadata::new,
            ComposableIndexTemplateMetadata::readDiffFrom
        );
        registerMetadataCustom(entries, DataStreamMetadata.TYPE, DataStreamMetadata::new, DataStreamMetadata::readDiffFrom);
        registerMetadataCustom(entries, NodesShutdownMetadata.TYPE, NodesShutdownMetadata::new, NodesShutdownMetadata::readDiffFrom);
        registerMetadataCustom(entries, FeatureMigrationResults.TYPE, FeatureMigrationResults::new, FeatureMigrationResults::readDiffFrom);
        registerMetadataCustom(entries, DesiredNodesMetadata.TYPE, DesiredNodesMetadata::new, DesiredNodesMetadata::readDiffFrom);

        // Task Status (not Diffable)
        entries.add(new Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME, PersistentTasksNodeService.Status::new));
        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(RepositoriesMetadata.TYPE),
                RepositoriesMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IngestMetadata.TYPE), IngestMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ScriptMetadata.TYPE), ScriptMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE), IndexGraveyard::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(PersistentTasksCustomMetadata.TYPE),
                PersistentTasksCustomMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(ComponentTemplateMetadata.TYPE),
                ComponentTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(ComposableIndexTemplateMetadata.TYPE),
                ComposableIndexTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(DataStreamMetadata.TYPE),
                DataStreamMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(NodesShutdownMetadata.TYPE),
                NodesShutdownMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(DesiredNodesMetadata.TYPE),
                DesiredNodesMetadata::fromXContent
            )
        );
        return entries;
    }

    private static <T extends ClusterState.Custom> void registerClusterCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff<?>> diffReader
    ) {
        registerCustom(entries, ClusterState.Custom.class, name, reader, diffReader);
    }

    private static <T extends Metadata.Custom> void registerMetadataCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff<?>> diffReader
    ) {
        registerCustom(entries, Metadata.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(
        List<Entry> entries,
        Class<T> category,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff<?>> diffReader
    ) {
        entries.add(new Entry(category, name, reader));
        entries.add(new Entry(NamedDiff.class, name, diffReader));
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    // TODO: this is public so allocation benchmark can access the default deciders...can we do that in another way?
    /** Return a new {@link AllocationDecider} instance with builtin deciders as well as those from plugins. */
    public static Collection<AllocationDecider> createAllocationDeciders(
        Settings settings,
        ClusterSettings clusterSettings,
        List<ClusterPlugin> clusterPlugins
    ) {
        // collect deciders by class so that we can detect duplicates
        Map<Class<?>, AllocationDecider> deciders = new LinkedHashMap<>();
        addAllocationDecider(deciders, new MaxRetryAllocationDecider());
        addAllocationDecider(deciders, new ResizeAllocationDecider());
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider());
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider());
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeVersionAllocationDecider());
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider());
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider());
        addAllocationDecider(deciders, new NodeShutdownAllocationDecider());
        addAllocationDecider(deciders, new NodeReplacementAllocationDecider());
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
    private static void addAllocationDecider(Map<Class<?>, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    private static ShardsAllocator createShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        List<ClusterPlugin> clusterPlugins
    ) {
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
        return Objects.requireNonNull(allocatorSupplier.get(), "ShardsAllocator factory for [" + allocatorName + "] returned null");
    }

    public AllocationService getAllocationService() {
        return allocationService;
    }

    public MetadataDeleteIndexService getMetadataDeleteIndexService() {
        return metadataDeleteIndexService;
    }

    @Override
    protected void configure() {
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).toInstance(allocationService);
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetadataDeleteIndexService.class).toInstance(metadataDeleteIndexService);
        bind(MetadataIndexStateService.class).asEagerSingleton();
        bind(MetadataMappingService.class).asEagerSingleton();
        bind(MetadataIndexAliasesService.class).asEagerSingleton();
        bind(MetadataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(allocationDeciders);
        bind(ShardsAllocator.class).toInstance(shardsAllocator);
    }

    public void setExistingShardsAllocators(GatewayAllocator gatewayAllocator) {
        final Map<String, ExistingShardsAllocator> existingShardsAllocators = new HashMap<>();
        existingShardsAllocators.put(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator);

        for (ClusterPlugin clusterPlugin : clusterPlugins) {
            for (Map.Entry<String, ExistingShardsAllocator> existingShardsAllocatorEntry : clusterPlugin.getExistingShardsAllocators()
                .entrySet()) {
                final String allocatorName = existingShardsAllocatorEntry.getKey();
                if (existingShardsAllocators.put(allocatorName, existingShardsAllocatorEntry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "ExistingShardsAllocator ["
                            + allocatorName
                            + "] from ["
                            + clusterPlugin.getClass().getName()
                            + "] was already defined"
                    );
                }
            }
        }
        allocationService.setExistingShardsAllocators(existingShardsAllocators);
    }

}
