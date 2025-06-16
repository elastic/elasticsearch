/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
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
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.AllocationService.RerouteStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationStatsService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsAndWeightsCalculator;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator.DesiredBalanceReconcilerAction;
import org.elasticsearch.cluster.routing.allocation.allocator.GlobalBalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.IndexVersionAllocationDecider;
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
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.health.metadata.HealthMetadataService;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.injection.guice.AbstractModule;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksNodeService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.script.ScriptMetadata;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Configures classes and services that affect the entire cluster.
 */
public class ClusterModule extends AbstractModule {

    public static final String BALANCED_ALLOCATOR = "balanced";
    public static final String DESIRED_BALANCE_ALLOCATOR = "desired_balance"; // default
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING = Setting.simpleString(
        "cluster.routing.allocation.type",
        DESIRED_BALANCE_ALLOCATOR,
        Property.NodeScope,
        Property.Deprecated
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
    private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;
    private final AllocationStatsService allocationStatsService;
    private final TelemetryProvider telemetryProvider;

    public ClusterModule(
        Settings settings,
        ClusterService clusterService,
        List<ClusterPlugin> clusterPlugins,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ThreadPool threadPool,
        SystemIndices systemIndices,
        ProjectResolver projectResolver,
        WriteLoadForecaster writeLoadForecaster,
        TelemetryProvider telemetryProvider
    ) {
        this.clusterPlugins = clusterPlugins;
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(deciderList);
        final BalancerSettings balancerSettings = new BalancerSettings(clusterService.getClusterSettings());
        final BalancingWeightsFactory balancingWeightsFactory = getBalancingWeightsFactory(
            clusterPlugins,
            balancerSettings,
            clusterService.getClusterSettings()
        );
        var nodeAllocationStatsAndWeightsCalculator = new NodeAllocationStatsAndWeightsCalculator(
            writeLoadForecaster,
            balancingWeightsFactory
        );
        this.shardsAllocator = createShardsAllocator(
            settings,
            clusterService.getClusterSettings(),
            balancerSettings,
            balancingWeightsFactory,
            threadPool,
            clusterPlugins,
            clusterService,
            this::reconcile,
            writeLoadForecaster,
            telemetryProvider,
            nodeAllocationStatsAndWeightsCalculator
        );
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(threadPool.getThreadContext(), systemIndices, projectResolver);
        this.shardRoutingRoleStrategy = getShardRoutingRoleStrategy(clusterPlugins);
        this.allocationService = new AllocationService(
            allocationDeciders,
            shardsAllocator,
            clusterInfoService,
            snapshotsInfoService,
            shardRoutingRoleStrategy
        );
        this.allocationService.addAllocFailuresResetListenerTo(clusterService);
        this.metadataDeleteIndexService = new MetadataDeleteIndexService(settings, clusterService, allocationService);
        this.allocationStatsService = new AllocationStatsService(
            clusterService,
            clusterInfoService,
            shardsAllocator,
            nodeAllocationStatsAndWeightsCalculator
        );
        this.telemetryProvider = telemetryProvider;
    }

    static ShardRoutingRoleStrategy getShardRoutingRoleStrategy(List<ClusterPlugin> clusterPlugins) {
        final var strategies = clusterPlugins.stream().map(ClusterPlugin::getShardRoutingRoleStrategy).filter(Objects::nonNull).toList();
        return switch (strategies.size()) {
            case 0 -> new ShardRoutingRoleStrategy() {

                // NOTE: this is deliberately an anonymous class to avoid any possibility of using this DEFAULT-only strategy when a plugin
                // has injected a different strategy.

                @Override
                public ShardRouting.Role newReplicaRole() {
                    return ShardRouting.Role.DEFAULT;
                }

                @Override
                public ShardRouting.Role newEmptyRole(int copyIndex) {
                    return ShardRouting.Role.DEFAULT;
                }
            };
            case 1 -> strategies.get(0);
            default -> throw new IllegalArgumentException("multiple plugins define shard role strategies, which is not permitted");
        };
    }

    static BalancingWeightsFactory getBalancingWeightsFactory(
        List<ClusterPlugin> clusterPlugins,
        BalancerSettings balancerSettings,
        ClusterSettings clusterSettings
    ) {
        final var strategies = clusterPlugins.stream()
            .map(pl -> pl.getBalancingWeightsFactory(balancerSettings, clusterSettings))
            .filter(Objects::nonNull)
            .toList();
        return switch (strategies.size()) {
            case 0 -> new GlobalBalancingWeightsFactory(balancerSettings);
            case 1 -> strategies.getFirst();
            default -> throw new IllegalArgumentException("multiple plugins define balancing weights factories, which is not permitted");
        };
    }

    private ClusterState reconcile(ClusterState clusterState, RerouteStrategy rerouteStrategy) {
        return allocationService.executeWithRoutingAllocation(clusterState, "reconcile-desired-balance", rerouteStrategy);
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
        registerProjectCustom(entries, RepositoriesMetadata.TYPE, RepositoriesMetadata::new, RepositoriesMetadata::readDiffFrom);
        registerProjectCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerProjectCustom(entries, ScriptMetadata.TYPE, ScriptMetadata::new, ScriptMetadata::readDiffFrom);
        registerProjectCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        // Project scoped persistent tasks
        registerProjectCustom(
            entries,
            PersistentTasksCustomMetadata.TYPE,
            PersistentTasksCustomMetadata::new,
            PersistentTasksCustomMetadata::readDiffFrom
        );
        // Cluster scoped persistent tasks
        registerMetadataCustom(
            entries,
            ClusterPersistentTasksCustomMetadata.TYPE,
            ClusterPersistentTasksCustomMetadata::new,
            ClusterPersistentTasksCustomMetadata::readDiffFrom
        );
        registerProjectCustom(
            entries,
            ComponentTemplateMetadata.TYPE,
            ComponentTemplateMetadata::new,
            ComponentTemplateMetadata::readDiffFrom
        );
        registerProjectCustom(
            entries,
            ComposableIndexTemplateMetadata.TYPE,
            ComposableIndexTemplateMetadata::new,
            ComposableIndexTemplateMetadata::readDiffFrom
        );
        registerProjectCustom(entries, DataStreamMetadata.TYPE, DataStreamMetadata::new, DataStreamMetadata::readDiffFrom);
        registerMetadataCustom(entries, NodesShutdownMetadata.TYPE, NodesShutdownMetadata::new, NodesShutdownMetadata::readDiffFrom);
        registerMetadataCustom(entries, DesiredNodesMetadata.TYPE, DesiredNodesMetadata::new, DesiredNodesMetadata::readDiffFrom);
        registerProjectCustom(
            entries,
            RegisteredPolicySnapshots.TYPE,
            RegisteredPolicySnapshots::new,
            RegisteredPolicySnapshots.RegisteredSnapshotsDiff::new
        );
        // Secrets
        registerClusterCustom(entries, ClusterSecrets.TYPE, ClusterSecrets::new, ClusterSecrets::readDiffFrom);
        registerProjectCustom(entries, ProjectSecrets.TYPE, ProjectSecrets::new, ProjectSecrets::readDiffFrom);
        // Task Status (not Diffable)
        entries.add(new Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME, PersistentTasksNodeService.Status::new));

        // Health API
        entries.addAll(HealthNodeTaskExecutor.getNamedWriteables());
        entries.addAll(HealthMetadataService.getNamedWriteables());
        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(RepositoriesMetadata.TYPE),
                RepositoriesMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(IngestMetadata.TYPE), IngestMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(ScriptMetadata.TYPE), ScriptMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(IndexGraveyard.TYPE), IndexGraveyard::fromXContent)
        );
        // Project scoped persistent tasks
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(PersistentTasksCustomMetadata.TYPE),
                PersistentTasksCustomMetadata::fromXContent
            )
        );
        // Cluster scoped persistent tasks
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ClusterCustom.class,
                new ParseField(ClusterPersistentTasksCustomMetadata.TYPE),
                ClusterPersistentTasksCustomMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(ComponentTemplateMetadata.TYPE),
                ComponentTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(ComposableIndexTemplateMetadata.TYPE),
                ComposableIndexTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(DataStreamMetadata.TYPE),
                DataStreamMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ClusterCustom.class,
                new ParseField(NodesShutdownMetadata.TYPE),
                NodesShutdownMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.ClusterCustom.class,
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

    private static <T extends Metadata.ClusterCustom> void registerMetadataCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff<?>> diffReader
    ) {
        registerCustom(entries, Metadata.ClusterCustom.class, name, reader, diffReader);
    }

    private static <T extends Metadata.ProjectCustom> void registerProjectCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff<?>> diffReader
    ) {
        registerCustom(entries, Metadata.ProjectCustom.class, name, reader, diffReader);
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
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(clusterSettings));
        addAllocationDecider(deciders, new IndexVersionAllocationDecider());
        addAllocationDecider(deciders, new NodeVersionAllocationDecider());
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider());
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider());
        addAllocationDecider(deciders, new NodeShutdownAllocationDecider());
        addAllocationDecider(deciders, new NodeReplacementAllocationDecider());
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new SameShardAllocationDecider(clusterSettings));
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(clusterSettings));
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(clusterSettings));
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

    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // in v10 there is only one allocator
    private static ShardsAllocator createShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        BalancerSettings balancerSettings,
        BalancingWeightsFactory balancingWeightsFactory,
        ThreadPool threadPool,
        List<ClusterPlugin> clusterPlugins,
        ClusterService clusterService,
        DesiredBalanceReconcilerAction reconciler,
        WriteLoadForecaster writeLoadForecaster,
        TelemetryProvider telemetryProvider,
        NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator
    ) {
        Map<String, Supplier<ShardsAllocator>> allocators = new HashMap<>();
        allocators.put(
            BALANCED_ALLOCATOR,
            () -> new BalancedShardsAllocator(balancerSettings, writeLoadForecaster, balancingWeightsFactory)
        );
        allocators.put(
            DESIRED_BALANCE_ALLOCATOR,
            () -> new DesiredBalanceShardsAllocator(
                clusterSettings,
                new BalancedShardsAllocator(balancerSettings, writeLoadForecaster, balancingWeightsFactory),
                threadPool,
                clusterService,
                reconciler,
                telemetryProvider,
                nodeAllocationStatsAndWeightsCalculator
            )
        );

        for (ClusterPlugin plugin : clusterPlugins) {
            // noinspection removal
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
        bind(ShardRoutingRoleStrategy.class).toInstance(shardRoutingRoleStrategy);
        bind(AllocationStatsService.class).toInstance(allocationStatsService);
        bind(TelemetryProvider.class).toInstance(telemetryProvider);
        bind(MetadataRolloverService.class).asEagerSingleton();
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
