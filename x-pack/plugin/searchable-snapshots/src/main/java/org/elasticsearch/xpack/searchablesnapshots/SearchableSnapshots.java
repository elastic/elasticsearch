/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportRepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportSearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestRepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsStatsAction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, EnginePlugin, ActionPlugin, ClusterPlugin {

    public static final Setting<String> SNAPSHOT_REPOSITORY_SETTING = Setting.simpleString(
        "index.store.snapshot.repository_name",
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_SNAPSHOT_NAME_SETTING = Setting.simpleString(
        "index.store.snapshot.snapshot_name",
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_SNAPSHOT_ID_SETTING = Setting.simpleString(
        "index.store.snapshot.snapshot_uuid",
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_INDEX_ID_SETTING = Setting.simpleString(
        "index.store.snapshot.index_uuid",
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<Boolean> SNAPSHOT_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "index.store.snapshot.cache.enabled",
        true,
        Setting.Property.IndexScope,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<Boolean> SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING = Setting.boolSetting(
        "index.store.snapshot.cache.prewarm.enabled",
        true,
        Setting.Property.IndexScope,
        Setting.Property.NotCopyableOnResize
    );
    // The file extensions that are excluded from the cache
    public static final Setting<List<String>> SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING = Setting.listSetting(
        "index.store.snapshot.cache.excluded_file_types",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.NodeScope,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<ByteSizeValue> SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "index.store.snapshot.uncached_chunk_size",
        new ByteSizeValue(-1, ByteSizeUnit.BYTES),
        Setting.Property.IndexScope,
        Setting.Property.NodeScope,
        Setting.Property.NotCopyableOnResize
    );

    private volatile Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final SetOnce<CacheService> cacheService = new SetOnce<>();
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();
    private final Settings settings;

    public SearchableSnapshots(final Settings settings) {
        this.settings = settings;
    }

    public static void ensureValidLicense(XPackLicenseState licenseState) {
        if (licenseState.isAllowedByLicense(License.OperationMode.PLATINUM) == false) {
            throw LicenseUtils.newComplianceException("searchable-snapshots");
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return List.of(
                SNAPSHOT_REPOSITORY_SETTING,
                SNAPSHOT_SNAPSHOT_NAME_SETTING,
                SNAPSHOT_SNAPSHOT_ID_SETTING,
                SNAPSHOT_INDEX_ID_SETTING,
                SNAPSHOT_CACHE_ENABLED_SETTING,
                SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING,
                SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING,
                SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING,
                CacheService.SNAPSHOT_CACHE_SIZE_SETTING,
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING
            );
        } else {
            return List.of();
        }
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry registry,
        final IndexNameExpressionResolver resolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            final CacheService cacheService = new CacheService(settings);
            this.cacheService.set(cacheService);
            this.repositoriesServiceSupplier = repositoriesServiceSupplier;
            this.threadPool.set(threadPool);
            return List.of(cacheService);
        } else {
            this.repositoriesServiceSupplier = () -> {
                assert false : "searchable snapshots are disabled";
                return null;
            };
            return List.of();
        }
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexModule.getSettings())) {
            indexModule.addIndexEventListener(new SearchableSnapshotIndexEventListener());
        }
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return Map.of(SNAPSHOT_DIRECTORY_FACTORY_KEY, (indexSettings, shardPath) -> {
                final RepositoriesService repositories = repositoriesServiceSupplier.get();
                assert repositories != null;
                final CacheService cache = cacheService.get();
                assert cache != null;
                final ThreadPool threadPool = this.threadPool.get();
                assert threadPool != null;
                return SearchableSnapshotDirectory.create(repositories, cache, indexSettings, shardPath, System::nanoTime, threadPool);
            });
        } else {
            return Map.of();
        }
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexSettings.getSettings())
            && indexSettings.getSettings().getAsBoolean("index.frozen", false) == false) {
            return Optional.of(
                engineConfig -> new ReadOnlyEngine(engineConfig, null, new TranslogStats(), false, Function.identity(), false)
            );
        }
        return Optional.empty();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return List.of(
                new ActionHandler<>(SearchableSnapshotsStatsAction.INSTANCE, TransportSearchableSnapshotsStatsAction.class),
                new ActionHandler<>(ClearSearchableSnapshotsCacheAction.INSTANCE, TransportClearSearchableSnapshotsCacheAction.class),
                new ActionHandler<>(MountSearchableSnapshotAction.INSTANCE, TransportMountSearchableSnapshotAction.class),
                new ActionHandler<>(RepositoryStatsAction.INSTANCE, TransportRepositoryStatsAction.class)
            );
        } else {
            return List.of();
        }
    }

    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return List.of(
                new RestSearchableSnapshotsStatsAction(),
                new RestClearSearchableSnapshotsCacheAction(),
                new RestMountSearchableSnapshotAction(),
                new RestRepositoryStatsAction()
            );
        } else {
            return List.of();
        }
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return Collections.singletonMap(SearchableSnapshotAllocator.ALLOCATOR_NAME, new SearchableSnapshotAllocator());
        } else {
            return Collections.emptyMap();
        }
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return List.of(executorBuilder());
        } else {
            return List.of();
        }
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(
            SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME,
            0,
            32,
            TimeValue.timeValueSeconds(30L),
            "xpack.searchable_snapshots.thread_pool"
        );
    }
}
