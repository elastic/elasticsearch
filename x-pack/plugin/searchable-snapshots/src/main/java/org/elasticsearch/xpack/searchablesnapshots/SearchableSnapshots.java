/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.sourceonly.SourceOnlySnapshotRepository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsInfoTransportAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsUsageTransportAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportSearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.FrozenCacheInfoAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.FrozenCacheInfoNodeAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.allocation.FailShardsOnInvalidLicenseClusterListener;
import org.elasticsearch.xpack.searchablesnapshots.allocation.SearchableSnapshotAllocator;
import org.elasticsearch.xpack.searchablesnapshots.allocation.SearchableSnapshotIndexEventListener;
import org.elasticsearch.xpack.searchablesnapshots.allocation.SearchableSnapshotIndexFoldersDeletionListener;
import org.elasticsearch.xpack.searchablesnapshots.allocation.decider.DedicatedFrozenNodeAllocationDecider;
import org.elasticsearch.xpack.searchablesnapshots.allocation.decider.HasFrozenCacheAllocationDecider;
import org.elasticsearch.xpack.searchablesnapshots.allocation.decider.SearchableSnapshotAllocationDecider;
import org.elasticsearch.xpack.searchablesnapshots.allocation.decider.SearchableSnapshotEnableAllocationDecider;
import org.elasticsearch.xpack.searchablesnapshots.allocation.decider.SearchableSnapshotRepositoryExistsAllocationDecider;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheMaintenanceService;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsNodeCachesStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;
import org.elasticsearch.xpack.searchablesnapshots.upgrade.SearchableSnapshotIndexMetadataUpgrader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, EnginePlugin, ActionPlugin, ClusterPlugin, SystemIndexPlugin {

    public static final Setting<String> SNAPSHOT_REPOSITORY_NAME_SETTING = Setting.simpleString(
        SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_REPOSITORY_UUID_SETTING = Setting.simpleString(
        SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_SNAPSHOT_NAME_SETTING = Setting.simpleString(
        SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_SNAPSHOT_ID_SETTING = Setting.simpleString(
        SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<String> SNAPSHOT_INDEX_NAME_SETTING = Setting.simpleString(
        "index.store.snapshot.index_name",
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
    public static final Setting<List<String>> SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING = Setting.stringListSetting(
        "index.store.snapshot.cache.excluded_file_types",
        Setting.Property.IndexScope,
        Setting.Property.NodeScope,
        Setting.Property.NotCopyableOnResize
    );
    public static final Setting<ByteSizeValue> SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "index.store.snapshot.uncached_chunk_size",
        ByteSizeValue.MINUS_ONE,
        Setting.Property.IndexScope,
        Setting.Property.NodeScope,
        Setting.Property.NotCopyableOnResize
    );

    public static final String SNAPSHOT_BLOB_CACHE_INDEX = ".snapshot-blob-cache";
    public static final String SNAPSHOT_BLOB_CACHE_INDEX_PATTERN = SNAPSHOT_BLOB_CACHE_INDEX + "*";
    public static final String SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH = "index.store.snapshot.blob_cache.metadata_files.max_length";
    public static final Setting<ByteSizeValue> SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH_SETTING = new Setting<>(
        SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH,
        new ByteSizeValue(64L, ByteSizeUnit.KB).getStringRep(),
        s -> Setting.parseByteSize(
            s,
            new ByteSizeValue(1L, ByteSizeUnit.KB),
            ByteSizeValue.ofBytes(Long.MAX_VALUE),
            SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH
        ),
        value -> {
            if (value.getBytes() % BufferedIndexInput.BUFFER_SIZE != 0L) {
                final String message = String.format(
                    Locale.ROOT,
                    "failed to parse value [%s] for setting [%s], must be a multiple of [%s] bytes",
                    value.getStringRep(),
                    SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH,
                    BufferedIndexInput.BUFFER_SIZE
                );
                throw new IllegalArgumentException(message);
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.NotCopyableOnResize
    );

    /**
     * Index setting used to indicate if the snapshot that is mounted as an index should be deleted when the index is deleted. This setting
     * is only set for indices mounted in clusters on or after 8.0.0. Once set this setting cannot be updated.
     */
    public static final Setting<Boolean> DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION = Setting.boolSetting(
        SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION,
        false,
        Setting.Property.Final,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );

    /**
     * Prefer to allocate to the data content tier and then the hot tier.
     * This affects the system searchable snapshot cache index (not the searchable snapshot index itself)
     */
    public static final String DATA_TIERS_CACHE_INDEX_PREFERENCE = String.join(",", DataTier.DATA_CONTENT, DataTier.DATA_HOT);

    private volatile Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final SetOnce<BlobStoreCacheService> blobStoreCacheService = new SetOnce<>();
    private final SetOnce<CacheService> cacheService = new SetOnce<>();
    private final SetOnce<SharedBlobCacheService<CacheKey>> frozenCacheService = new SetOnce<>();
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();
    private final SetOnce<FailShardsOnInvalidLicenseClusterListener> failShardsListener = new SetOnce<>();
    private final SetOnce<SearchableSnapshotAllocator> allocator = new SetOnce<>();
    private final Settings settings;
    private final FrozenCacheInfoService frozenCacheInfoService = new FrozenCacheInfoService();

    public SearchableSnapshots(final Settings settings) {
        this.settings = settings;
    }

    public static void ensureValidLicense(XPackLicenseState licenseState) {
        if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
            throw LicenseUtils.newComplianceException("searchable-snapshots");
        }
    }

    public static BlobStoreRepository getSearchableRepository(Repository repository) {
        if (repository instanceof SourceOnlySnapshotRepository) {
            repository = ((SourceOnlySnapshotRepository) repository).getDelegate();
        }
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
        }
        return (BlobStoreRepository) repository;
    }

    public static Predicate<CacheKey> forceEvictPredicate(ShardId shardId, Settings indexSettings) {
        final String snapshotUUID = SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings);
        final String snapshotIndexName = SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings);
        return k -> shardId.equals(k.shardId()) && snapshotIndexName.equals(k.snapshotIndexName()) && snapshotUUID.equals(k.snapshotUUID());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SNAPSHOT_REPOSITORY_UUID_SETTING,
            SNAPSHOT_REPOSITORY_NAME_SETTING,
            SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SNAPSHOT_SNAPSHOT_ID_SETTING,
            SNAPSHOT_INDEX_NAME_SETTING,
            SNAPSHOT_INDEX_ID_SETTING,
            SNAPSHOT_CACHE_ENABLED_SETTING,
            SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING,
            SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING,
            SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING,
            DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION,
            SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING,
            SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH_SETTING,
            CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING,
            CacheService.SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING,
            CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING,
            CacheService.SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING,
            CacheService.SNAPSHOT_CACHE_SYNC_SHUTDOWN_TIMEOUT,
            SearchableSnapshotEnableAllocationDecider.SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART,
            BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING,
            BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_KEEP_ALIVE_SETTING,
            BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_BATCH_SIZE_SETTING,
            BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD
        );
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
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        final List<Object> components = new ArrayList<>();
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool.set(threadPool);
        this.failShardsListener.set(new FailShardsOnInvalidLicenseClusterListener(getLicenseState(), clusterService.getRerouteService()));
        if (DiscoveryNode.canContainData(settings)) {
            final CacheService cacheService = new CacheService(settings, clusterService, threadPool, new PersistentCache(nodeEnvironment));
            this.cacheService.set(cacheService);
            final SharedBlobCacheService<CacheKey> sharedBlobCacheService = new SharedBlobCacheService<>(
                nodeEnvironment,
                settings,
                threadPool,
                SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME
            );
            this.frozenCacheService.set(sharedBlobCacheService);
            components.add(cacheService);
            final BlobStoreCacheService blobStoreCacheService = new BlobStoreCacheService(
                clusterService,
                client,
                SNAPSHOT_BLOB_CACHE_INDEX
            );
            this.blobStoreCacheService.set(blobStoreCacheService);
            clusterService.addListener(
                new BlobStoreCacheMaintenanceService(settings, clusterService, threadPool, client, SNAPSHOT_BLOB_CACHE_INDEX)
            );
            components.add(blobStoreCacheService);
        } else {
            PersistentCache.cleanUp(settings, nodeEnvironment);
        }

        if (DiscoveryNode.isMasterNode(environment.settings())) {
            // Tracking usage of searchable snapshots is too costly to do on each individually mounted snapshot.
            // Instead, we periodically look through the indices and identify if any are searchable snapshots,
            // then marking the feature as used. We do this on each master node so that if one master fails, the
            // continue reporting usage state.
            var usageTracker = new SearchableSnapshotsUsageTracker(getLicenseState(), clusterService::state);
            threadPool.scheduleWithFixedDelay(usageTracker, TimeValue.timeValueMinutes(15), ThreadPool.Names.GENERIC);
        }

        this.allocator.set(new SearchableSnapshotAllocator(client, clusterService.getRerouteService(), frozenCacheInfoService));
        components.add(new FrozenCacheServiceSupplier(frozenCacheService.get()));
        components.add(new CacheServiceSupplier(cacheService.get()));
        if (DiscoveryNode.isMasterNode(settings)) {
            new SearchableSnapshotIndexMetadataUpgrader(clusterService, threadPool).initialize();
            clusterService.addListener(new RepositoryUuidWatcher(clusterService.getRerouteService()));
        }
        return Collections.unmodifiableList(components);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.indexSettings().getIndexMetadata().isSearchableSnapshot()) {
            indexModule.addIndexEventListener(
                new SearchableSnapshotIndexEventListener(settings, cacheService.get(), frozenCacheService.get())
            );
            indexModule.addIndexEventListener(failShardsListener.get());

            indexModule.addSettingsUpdateConsumer(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, s -> {}, write -> {
                if (write == false) {
                    throw new IllegalArgumentException("Cannot remove write block from searchable snapshot index");
                }
            });
        }
    }

    @Override
    public List<IndexFoldersDeletionListener> getIndexFoldersDeletionListeners() {
        if (DiscoveryNode.canContainData(settings)) {
            return List.of(new SearchableSnapshotIndexFoldersDeletionListener(cacheService::get, frozenCacheService::get));
        }
        return List.of();
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setIndexPattern(SNAPSHOT_BLOB_CACHE_INDEX_PATTERN)
                .setDescription("Contains cached data of blob store repositories")
                .setPrimaryIndex(SNAPSHOT_BLOB_CACHE_INDEX)
                .setMappings(getIndexMappings())
                .setSettings(getIndexSettings())
                .setOrigin(SEARCHABLE_SNAPSHOTS_ORIGIN)
                .setVersionMetaKey("version")
                .build()
        );
    }

    @Override
    public String getFeatureName() {
        return "searchable_snapshots";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages caches and configuration for searchable snapshots";
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(SEARCHABLE_SNAPSHOT_STORE_TYPE, (indexSettings, shardPath) -> {
            final RepositoriesService repositories = repositoriesServiceSupplier.get();
            assert repositories != null;
            final CacheService cache = cacheService.get();
            assert cache != null;
            final ThreadPool threadPool = this.threadPool.get();
            assert threadPool != null;
            final BlobStoreCacheService blobCache = blobStoreCacheService.get();
            assert blobCache != null;
            return SearchableSnapshotDirectory.create(
                repositories,
                cache,
                indexSettings,
                shardPath,
                System::nanoTime,
                threadPool,
                blobCache,
                frozenCacheService.get()
            );
        });
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getIndexMetadata().isSearchableSnapshot()) {
            final Boolean frozen = indexSettings.getSettings().getAsBoolean("index.frozen", null);
            final boolean useFrozenEngine = indexSettings.getIndexMetadata().isPartialSearchableSnapshot()
                && (frozen == null || frozen.equals(Boolean.TRUE));

            if (useFrozenEngine) {
                return Optional.of(
                    engineConfig -> new FrozenEngine(
                        engineConfig,
                        null,
                        new TranslogStats(),
                        false,
                        indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY)
                            ? SourceOnlySnapshotRepository.readerWrapper(engineConfig)
                            : Function.identity(),
                        false,
                        true
                    )
                );
            } else {
                return Optional.of(
                    engineConfig -> new ReadOnlyEngine(
                        engineConfig,
                        null,
                        new TranslogStats(),
                        false,
                        indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY)
                            ? SourceOnlySnapshotRepository.readerWrapper(engineConfig)
                            : Function.identity(),
                        false,
                        true
                    )
                );
            }
        }
        return Optional.empty();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(SearchableSnapshotsStatsAction.INSTANCE, TransportSearchableSnapshotsStatsAction.class),
            new ActionHandler<>(ClearSearchableSnapshotsCacheAction.INSTANCE, TransportClearSearchableSnapshotsCacheAction.class),
            new ActionHandler<>(MountSearchableSnapshotAction.INSTANCE, TransportMountSearchableSnapshotAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.SEARCHABLE_SNAPSHOTS, SearchableSnapshotsUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS, SearchableSnapshotsInfoTransportAction.class),
            new ActionHandler<>(TransportSearchableSnapshotCacheStoresAction.TYPE, TransportSearchableSnapshotCacheStoresAction.class),
            new ActionHandler<>(FrozenCacheInfoAction.INSTANCE, FrozenCacheInfoAction.TransportAction.class),
            new ActionHandler<>(FrozenCacheInfoNodeAction.INSTANCE, FrozenCacheInfoNodeAction.TransportAction.class),
            new ActionHandler<>(
                TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
                TransportSearchableSnapshotsNodeCachesStatsAction.class
            )
        );
    }

    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestSearchableSnapshotsStatsAction(),
            new RestClearSearchableSnapshotsCacheAction(),
            new RestMountSearchableSnapshotAction(),
            new RestSearchableSnapshotsNodeCachesStatsAction()
        );
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Map.of(SearchableSnapshotAllocator.ALLOCATOR_NAME, allocator.get());
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settingsToUse, ClusterSettings clusterSettings) {
        return List.of(
            new SearchableSnapshotAllocationDecider(() -> SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(getLicenseState())),
            new SearchableSnapshotRepositoryExistsAllocationDecider(),
            new SearchableSnapshotEnableAllocationDecider(settingsToUse, clusterSettings),
            new HasFrozenCacheAllocationDecider(frozenCacheInfoService),
            new DedicatedFrozenNodeAllocationDecider()
        );
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(executorBuilders(settings));
    }

    public static final String SNAPSHOT_RECOVERY_STATE_FACTORY_KEY = "snapshot_prewarm";

    @Override
    public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
        return Map.of(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY, SearchableSnapshotRecoveryState::new);
    }

    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_NAME = "searchable_snapshots_cache_fetch_async";
    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_fetch_async_thread_pool";
    public static final String CACHE_PREWARMING_THREAD_POOL_NAME = "searchable_snapshots_cache_prewarming";
    public static final String CACHE_PREWARMING_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_prewarming_thread_pool";

    public static ScalingExecutorBuilder[] executorBuilders(Settings settings) {
        final int processors = EsExecutors.allocatedProcessors(settings);
        // searchable snapshots cache thread pools should always reject tasks once they are shutting down, otherwise some threads might be
        // waiting for some cache file regions to be populated but this will never happen once the thread pool is shutting down. In order to
        // prevent these threads to be blocked the cache thread pools will reject after shutdown.
        final boolean rejectAfterShutdown = true;
        return new ScalingExecutorBuilder[] {
            new ScalingExecutorBuilder(
                CACHE_FETCH_ASYNC_THREAD_POOL_NAME,
                0,
                Math.min(processors * 3, 50),
                TimeValue.timeValueSeconds(30L),
                rejectAfterShutdown,
                CACHE_FETCH_ASYNC_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                CACHE_PREWARMING_THREAD_POOL_NAME,
                0,
                16,
                TimeValue.timeValueSeconds(30L),
                rejectAfterShutdown,
                CACHE_PREWARMING_THREAD_POOL_SETTING
            ) };
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, "900")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .put(DataTier.TIER_PREFERENCE, DATA_TIERS_CACHE_INDEX_PREFERENCE)
            .build();
    }

    private static XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            {
                builder.startObject();
                {
                    builder.startObject(SINGLE_MAPPING_NAME);
                    builder.field("dynamic", "strict");
                    {
                        builder.startObject("_meta");
                        builder.field("version", Version.CURRENT);
                        builder.endObject();
                    }
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("type");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        {
                            builder.startObject("creation_time");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();
                        }
                        {
                            builder.startObject("version");
                            builder.field("type", "integer");
                            builder.endObject();
                        }
                        {
                            builder.startObject("repository");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        {
                            builder.startObject("blob");
                            builder.field("type", "object");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("name");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                    builder.startObject("path");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        {
                            builder.startObject("data");
                            builder.field("type", "object");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("content");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                {
                                    builder.startObject("length");
                                    builder.field("type", "long");
                                    builder.endObject();
                                }
                                {
                                    builder.startObject("from");
                                    builder.field("type", "long");
                                    builder.endObject();
                                }
                                {
                                    builder.startObject("to");
                                    builder.field("type", "long");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + SNAPSHOT_BLOB_CACHE_INDEX + " index mappings", e);
        }
    }

    @Override
    public void close() throws IOException {
        Releasables.close(frozenCacheService.get());
    }

    /**
     * Allows to inject the {@link CacheService} instance to transport actions
     */
    public static final class CacheServiceSupplier implements Supplier<CacheService> {

        @Nullable
        private final CacheService cacheService;

        CacheServiceSupplier(@Nullable CacheService cacheService) {
            this.cacheService = cacheService;
        }

        @Override
        public CacheService get() {
            return cacheService;
        }
    }

    /**
     * Allows to inject the {@link SharedBlobCacheService} instance to transport actions
     */
    public static final class FrozenCacheServiceSupplier implements Supplier<SharedBlobCacheService<CacheKey>> {

        @Nullable
        private final SharedBlobCacheService<CacheKey> sharedBlobCacheService;

        FrozenCacheServiceSupplier(@Nullable SharedBlobCacheService<CacheKey> sharedBlobCacheService) {
            this.sharedBlobCacheService = sharedBlobCacheService;
        }

        @Override
        public SharedBlobCacheService<CacheKey> get() {
            return sharedBlobCacheService;
        }
    }

    private static final class RepositoryUuidWatcher implements ClusterStateListener {

        private final RerouteService rerouteService;
        private final HashSet<String> knownUuids = new HashSet<>();

        RepositoryUuidWatcher(RerouteService rerouteService) {
            this.rerouteService = rerouteService;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            final RepositoriesMetadata repositoriesMetadata = event.state().metadata().custom(RepositoriesMetadata.TYPE);
            if (repositoriesMetadata == null) {
                knownUuids.clear();
                return;
            }

            final Set<String> newUuids = repositoriesMetadata.repositories()
                .stream()
                .map(RepositoryMetadata::uuid)
                .filter(s -> s.equals(RepositoryData.MISSING_UUID) == false)
                .collect(Collectors.toSet());
            if (knownUuids.addAll(newUuids)) {
                rerouteService.reroute("repository UUIDs changed", Priority.NORMAL, ActionListener.noop());
            }
            knownUuids.retainAll(newUuids);
            assert knownUuids.equals(newUuids) : knownUuids + " vs " + newUuids;
        }
    }

}
