/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.allocation.StatelessAllocationDecider;
import co.elastic.elasticsearch.stateless.allocation.StatelessExistingShardsAllocator;
import co.elastic.elasticsearch.stateless.allocation.StatelessIndexSettingProvider;
import co.elastic.elasticsearch.stateless.allocation.StatelessShardRoutingRoleStrategy;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadPublisher;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.PublishNodeIngestLoadAction;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.TransportPublishNodeIngestLoadMetric;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.PublishShardSizesAction;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizesCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizesPublisher;
import co.elastic.elasticsearch.stateless.autoscaling.search.TransportPublishShardSizes;
import co.elastic.elasticsearch.stateless.cache.ClearBlobCacheRestHandler;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheAction;
import co.elastic.elasticsearch.stateless.cache.action.TransportClearBlobCacheAction;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessHeartbeatStore;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessPersistedClusterStateService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottlingService;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsReader;
import co.elastic.elasticsearch.stateless.metering.GetBlobStoreStatsRestHandler;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsAction;
import co.elastic.elasticsearch.stateless.metering.action.TransportGetBlobStoreStatsAction;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;
import co.elastic.elasticsearch.stateless.upgrade.StatelessUpgrader;
import co.elastic.elasticsearch.stateless.xpack.DummySearchableSnapshotsInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummySearchableSnapshotsUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyVotingOnlyInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyVotingOnlyUsageTransportAction;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.shard.StoreRecovery.bootstrap;

public class Stateless extends Plugin implements EnginePlugin, ActionPlugin, ClusterPlugin, ClusterCoordinationPlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(Stateless.class);

    public static final String NAME = "stateless";

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    private final SetOnce<StatelessCommitService> commitService = new SetOnce<>();
    private final SetOnce<ObjectStoreService> objectStoreService = new SetOnce<>();
    private final SetOnce<SharedBlobCacheService<FileCacheKey>> sharedBlobCacheService = new SetOnce<>();
    private final SetOnce<TranslogReplicator> translogReplicator = new SetOnce<>();
    private final SetOnce<StatelessElectionStrategy> electionStrategy = new SetOnce<>();
    private final SetOnce<StoreHeartbeatService> storeHeartbeatService = new SetOnce<>();
    private final SetOnce<RefreshThrottlingService> refreshThrottlingService = new SetOnce<>();
    private final SetOnce<ShardSizesCollector> shardSizesCollector = new SetOnce<>();

    private final boolean sharedCachedSettingExplicitlySet;

    private final boolean sharedCacheMmapExplicitlySet;

    private final boolean hasSearchRole;
    private final boolean hasIndexRole;

    private ObjectStoreService getObjectStoreService() {
        return Objects.requireNonNull(this.objectStoreService.get());
    }

    private StatelessCommitService getCommitService() {
        return Objects.requireNonNull(this.commitService.get());
    }

    public Stateless(Settings settings) {
        validateSettings(settings);
        // It is dangerous to retain these settings because they will be further modified after this ctor due
        // to the call to #additionalSettings. We only parse out the components that has already been set.
        sharedCachedSettingExplicitlySet = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings);
        sharedCacheMmapExplicitlySet = SharedBlobCacheService.SHARED_CACHE_MMAP.exists(settings);
        hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(TransportNewCommitNotificationAction.TYPE, TransportNewCommitNotificationAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.VOTING_ONLY, DummyVotingOnlyInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.VOTING_ONLY, DummyVotingOnlyUsageTransportAction.class),
            new ActionHandler<>(PublishNodeIngestLoadAction.INSTANCE, TransportPublishNodeIngestLoadMetric.class),
            new ActionHandler<>(ClearBlobCacheAction.INSTANCE, TransportClearBlobCacheAction.class),
            new ActionHandler<>(PublishShardSizesAction.INSTANCE, TransportPublishShardSizes.class),
            new ActionHandler<>(StatelessPrimaryRelocationAction.INSTANCE, TransportStatelessPrimaryRelocationAction.class),
            new ActionHandler<>(GetBlobStoreStatsAction.INSTANCE, TransportGetBlobStoreStatsAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new ClearBlobCacheRestHandler(), new GetBlobStoreStatsRestHandler());
    }

    @Override
    public Settings additionalSettings() {
        var settings = Settings.builder().put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), StatelessElectionStrategy.NAME);
        if (sharedCachedSettingExplicitlySet == false) {
            if (hasSearchRole) {
                settings.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "75%")
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(), "250GB");
            }
            if (hasIndexRole) {
                settings.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "50%")
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(), "-1");
            }
        }
        if (sharedCacheMmapExplicitlySet == false) {
            settings.put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true);
        }
        // always override counting reads, stateless does not expose this number so the overhead for tracking it is wasted in any case
        settings.put(SharedBlobCacheService.SHARED_CACHE_COUNT_READS.getKey(), false);

        String nodeMemoryAttrName = "node.attr." + RefreshThrottlingService.MEMORY_NODE_ATTR;
        if (settings.get(nodeMemoryAttrName) == null) {
            settings.put(nodeMemoryAttrName, Long.toString(OsProbe.getInstance().osStats().getMem().getAdjustedTotal().getBytes()));
        } else {
            throw new IllegalArgumentException("Directly setting [" + nodeMemoryAttrName + "] is not permitted - it is reserved.");
        }

        return settings.build();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        // use the settings that include additional settings.
        Settings settings = environment.settings();
        var objectStoreService = setAndGet(
            this.objectStoreService,
            new ObjectStoreService(settings, repositoriesServiceSupplier, threadPool, clusterService)
        );
        setAndGet(this.commitService, createStatelessCommitService(objectStoreService, clusterService, client));
        var sharedBlobCacheServiceSupplier = new SharedBlobCacheServiceSupplier(
            // TODO: figure out a better/correct threadpool for this
            setAndGet(
                this.sharedBlobCacheService,
                new SharedBlobCacheService<>(nodeEnvironment, settings, threadPool, ThreadPool.Names.GENERIC)
            )
        );

        var statelessElectionStrategy = setAndGet(
            this.electionStrategy,
            new StatelessElectionStrategy(objectStoreService::getTermLeaseBlobContainer, threadPool)
        );
        setAndGet(
            this.storeHeartbeatService,
            StoreHeartbeatService.create(
                new StatelessHeartbeatStore(objectStoreService::getLeaderHeartbeatContainer, threadPool),
                threadPool,
                environment.settings(),
                statelessElectionStrategy::getCurrentLeaseTerm
            )
        );
        var consistencyService = new StatelessClusterConsistencyService(clusterService, statelessElectionStrategy);
        var translogReplicator = setAndGet(this.translogReplicator, new TranslogReplicator(threadPool, settings, objectStoreService));
        var refreshThrottlingService = setAndGet(this.refreshThrottlingService, new RefreshThrottlingService(settings, clusterService));

        // autoscaling
        // memory
        var memoryMetricsService = new MemoryMetricsService();
        // ingest
        var ingestLoadPublisher = new IngestLoadPublisher(client, threadPool);
        var writeLoadSampler = AverageWriteLoadSampler.create(threadPool, settings, clusterService.getClusterSettings());
        var ingestLoadProbe = new IngestLoadProbe(clusterService.getClusterSettings(), writeLoadSampler::getExecutorStats);
        var ingestLoadSampler = IngestLoadSampler.create(
            threadPool,
            writeLoadSampler,
            ingestLoadPublisher,
            ingestLoadProbe::getIngestionLoad,
            hasIndexRole,
            settings,
            clusterService
        );
        var ingestMetricService = new IngestMetricsService(
            clusterService.getClusterSettings(),
            threadPool::relativeTimeInNanos,
            memoryMetricsService
        );
        clusterService.addListener(ingestMetricService);
        // search
        var shardSizeStatsReader = new ShardSizeStatsReader(threadPool, indicesService);
        var shardSizesPublisher = new ShardSizesPublisher(client, threadPool);
        var shardSizesCollector = setAndGet(
            this.shardSizesCollector,
            ShardSizesCollector.create(
                clusterService.getClusterSettings(),
                threadPool,
                clusterService,
                shardSizeStatsReader,
                shardSizesPublisher,
                hasSearchRole
            )
        );
        var searchMetricsService = SearchMetricsService.create(
            clusterService.getClusterSettings(),
            threadPool,
            clusterService,
            memoryMetricsService
        );

        StatelessUpgrader statelessUpgrader = new StatelessUpgrader(client, clusterService);
        clusterService.addListener(statelessUpgrader);

        return List.of(
            objectStoreService,
            translogReplicator,
            sharedBlobCacheServiceSupplier,
            refreshThrottlingService,
            // autoscaling
            memoryMetricsService,
            ingestLoadSampler,
            ingestMetricService,
            shardSizesCollector,
            searchMetricsService,
            consistencyService
        );
    }

    /**
     * This class wraps the {@code sharedBlobCacheService} for use in dependency injection, as the sharedBlobCacheService's parameterized
     * type of {@code SharedBlobCacheService<FileCacheKey>} is erased.
     */
    public static final class SharedBlobCacheServiceSupplier implements Supplier<SharedBlobCacheService<FileCacheKey>> {

        private final SharedBlobCacheService<FileCacheKey> sharedBlobCacheService;

        SharedBlobCacheServiceSupplier(SharedBlobCacheService<FileCacheKey> sharedBlobCacheService) {
            this.sharedBlobCacheService = Objects.requireNonNull(sharedBlobCacheService);
        }

        @Override
        public SharedBlobCacheService<FileCacheKey> get() {
            return sharedBlobCacheService;
        }
    }

    protected StatelessCommitService createStatelessCommitService(
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client
    ) {
        return new StatelessCommitService(objectStoreService, clusterService, client);
    }

    private static <T> T setAndGet(SetOnce<T> ref, T service) {
        ref.set(service);
        return service;
    }

    @Override
    public void close() throws IOException {
        Releasables.close(sharedBlobCacheService.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            STATELESS_ENABLED,
            ObjectStoreService.TYPE_SETTING,
            ObjectStoreService.BUCKET_SETTING,
            ObjectStoreService.CLIENT_SETTING,
            ObjectStoreService.BASE_PATH_SETTING,
            ObjectStoreService.OBJECT_STORE_SHUTDOWN_TIMEOUT,
            TranslogReplicator.FLUSH_RETRY_INITIAL_DELAY_SETTING,
            TranslogReplicator.FLUSH_INTERVAL_SETTING,
            TranslogReplicator.FLUSH_SIZE_SETTING,
            StoreHeartbeatService.HEARTBEAT_FREQUENCY,
            StoreHeartbeatService.MAX_MISSED_HEARTBEATS,
            IngestLoadSampler.SAMPLING_FREQUENCY_SETTING,
            IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING,
            IngestLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
            IngestMetricsService.ACCURATE_LOAD_WINDOW,
            IngestMetricsService.STALE_LOAD_WINDOW,
            IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
            ShardSizesCollector.BOOST_WINDOW_SETTING,
            ShardSizesCollector.PUSH_INTERVAL_SETTING,
            ShardSizesCollector.PUSH_DELTA_THRESHOLD_SETTING,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        var statelessCommitService = commitService.get();
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (hasIndexRole) {
            var localTranslogReplicator = translogReplicator.get();
            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    statelessCommitService.register(indexShard.shardId());
                    localTranslogReplicator.register(indexShard.shardId());
                    // We are pruning the archive for a given generation, only once we know all search shards are
                    // aware of that generation.
                    // TODO: In the context of real-time GET, this might be an overkill and in case of misbehaving
                    // search shards, this might lead to higher memory consumption on the indexing shards. Depending on
                    // how we respond to get requests that are not in the live version map (what generation we send back
                    // for the search shard to wait for), it could be safe to trigger the pruning earlier, e.g., once the
                    // commit upload is successful.
                    statelessCommitService.registerNewCommitSuccessListener(indexShard.shardId(), (gen) -> {
                        var engine = (IndexEngine) indexShard.getEngineOrNull();
                        if (engine != null) {
                            engine.commitSuccess(gen);
                        }
                    });
                }

                @Override
                public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                    if (indexShard != null) {
                        statelessCommitService.unregisterNewCommitSuccessListener(shardId);
                    }
                }

                @Override
                public void onStoreClosed(ShardId shardId) {
                    statelessCommitService.unregister(shardId);
                    localTranslogReplicator.unregister(shardId);
                }
            });
            indexModule.setIndexCommitListener(createIndexCommitListener());
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isPromotableToPrimary()) {
                    Lucene.cleanLuceneIndex(in);
                    return new IndexDirectory(in, sharedBlobCacheService.get(), shardRouting.shardId());
                } else {
                    return in;
                }
            });
        }
        if (hasSearchRole) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void afterIndexShardStarted(IndexShard indexShard) {
                    shardSizesCollector.get().detectShardSize(indexShard.shardId());
                }
            });
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isSearchable()) {
                    in.close();
                    return new SearchDirectory(sharedBlobCacheService.get(), shardRouting.shardId());
                } else {
                    return in;
                }
            });
        }
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                ActionListener.completeWith(listener, () -> {
                    final Store store = indexShard.store();
                    store.incRef();
                    try {
                        final var blobStore = objectStoreService.get().getObjectStore();
                        final var objectStore = blobStore.blobStore();
                        var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
                        final ShardId shardId = indexShard.shardId();
                        final var basePath = blobStore.basePath()
                            .add("indices")
                            .add(shardId.getIndex().getUUID())
                            .add(String.valueOf(shardId.id()));
                        final LongFunction<BlobContainer> containerSupplier = primaryTerm -> objectStore.blobContainer(
                            basePath.add(String.valueOf(primaryTerm))
                        );
                        searchDirectory.setBlobContainer(containerSupplier);
                        StatelessCompoundCommit latestCommit = null;
                        final long primaryTerm = indexShard.getOperationPrimaryTerm();
                        if (indexShard.recoveryState().getRecoverySource() == RecoverySource.EmptyStoreRecoverySource.INSTANCE) {
                            logger.debug("Skipping checking for existing commit for empty store recovery of [{}]", shardId);
                        } else {
                            logger.debug("Checking for usable commit for [{}] starting at term [{}]", shardId, primaryTerm);
                            for (long term = primaryTerm; term >= 0; term--) {
                                latestCommit = ObjectStoreService.findSearchShardFiles(containerSupplier.apply(term));
                                if (latestCommit != null) {
                                    logger.debug("Found usable commit [{}] at term [{}]", latestCommit, term);
                                    break;
                                }
                            }
                        }
                        final StatelessCompoundCommit commit = latestCommit;
                        logger.debug(() -> {
                            var segments = commit == null
                                ? Optional.empty()
                                : commit.commitFiles().keySet().stream().filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findFirst();
                            if (segments.isPresent()) {
                                return format("[%s] bootstrapping shard from object store using commit [%s]", shardId, segments.get());
                            } else {
                                return format("[%s] bootstrapping shard from object store using empty commit", shardId);
                            }
                        });

                        if (indexShard.routingEntry().isSearchable() && commit != null) {
                            searchDirectory.updateCommit(commit);
                        }

                        if (indexShard.routingEntry().isPromotableToPrimary()) {
                            final var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
                            if (commit != null) {
                                indexDirectory.updateCommit(commit);
                            }
                            final var segmentInfos = SegmentInfos.readLatestCommit(indexDirectory);
                            final var translogUUID = segmentInfos.userData.get(Translog.TRANSLOG_UUID_KEY);
                            final var checkPoint = segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
                            if (translogUUID != null) {
                                Translog.createEmptyTranslog(
                                    indexShard.shardPath().resolveTranslog(),
                                    indexShard.shardId(),
                                    checkPoint == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : Long.parseLong(checkPoint),
                                    indexShard.getPendingPrimaryTerm(),
                                    translogUUID,
                                    null
                                );
                            } else {
                                bootstrap(indexShard, store);
                            }
                            if (commit != null) {
                                statelessCommitService.markRecoveredCommit(shardId, commit);
                            }
                            statelessCommitService.addConsumerForNewUploadedCommit(shardId, indexDirectory::updateCommit);
                        }
                    } finally {
                        store.decRef();
                    }
                    return null;
                });
            }

            @Override
            public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                ActionListener.run(listener, l -> {
                    if (indexShard.routingEntry().isPromotableToPrimary()) {
                        Engine engineOrNull = indexShard.getEngineOrNull();
                        if (engineOrNull instanceof IndexEngine engine) {
                            long currentGeneration = engine.getCurrentGeneration();
                            if (currentGeneration > statelessCommitService.getRecoveredGeneration(indexShard.shardId())) {
                                ShardId shardId = indexShard.shardId();
                                statelessCommitService.addListenerForUploadedGeneration(shardId, engine.getCurrentGeneration(), l);
                            } else {
                                engine.flush(true, true, l.map(f -> null));
                            }
                        } else if (engineOrNull == null) {
                            throw new IllegalStateException("Engine is null");
                        } else {
                            assert engineOrNull instanceof NoOpEngine;
                            l.onResponse(null);
                        }
                    } else {
                        l.onResponse(null);
                    }
                });
            }
        });
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.of(config -> {
            TranslogReplicator replicator = translogReplicator.get();
            TranslogConfig translogConfig = config.getTranslogConfig();
            replicator.setBigArrays(translogConfig.getBigArrays());
            if (config.isPromotableToPrimary()) {
                TranslogConfig newTranslogConfig = new TranslogConfig(
                    translogConfig.getShardId(),
                    translogConfig.getTranslogPath(),
                    translogConfig.getIndexSettings(),
                    translogConfig.getBigArrays(),
                    translogConfig.getBufferSize(),
                    translogConfig.getDiskIoBufferPool(),
                    (data, seqNo, location) -> replicator.add(translogConfig.getShardId(), data, seqNo, location)
                );
                EngineConfig newConfig = new EngineConfig(
                    config.getShardId(),
                    config.getThreadPool(),
                    config.getIndexSettings(),
                    config.getWarmer(),
                    config.getStore(),
                    config.getMergePolicy(),
                    config.getAnalyzer(),
                    config.getSimilarity(),
                    config.getCodecService(),
                    config.getEventListener(),
                    config.getQueryCache(),
                    config.getQueryCachingPolicy(),
                    newTranslogConfig,
                    config.getFlushMergesAfter(),
                    config.getExternalRefreshListener(),
                    config.getInternalRefreshListener(),
                    config.getIndexSort(),
                    config.getCircuitBreakerService(),
                    config.getGlobalCheckpointSupplier(),
                    config.retentionLeasesSupplier(),
                    config.getPrimaryTermSupplier(),
                    config.getSnapshotCommitSupplier(),
                    config.getLeafSorter(),
                    config.getRelativeTimeInNanosSupplier(),
                    config.getIndexCommitListener(),
                    config.isPromotableToPrimary()
                );
                return new IndexEngine(
                    newConfig,
                    translogReplicator.get(),
                    getObjectStoreService()::getTranslogBlobContainer,
                    getCommitService(),
                    refreshThrottlingService.get().createRefreshThrottlerFactory(indexSettings)
                );
            } else {
                return new SearchEngine(config);
            }
        });
    }

    @Override
    public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
        return new StatelessShardRoutingRoleStrategy();
    }

    /**
     * Creates an {@link Engine.IndexCommitListener} that notifies the {@link ObjectStoreService} of all commit points created by Lucene.
     * This method is protected and overridable in tests.
     *
     * @return a {@link Engine.IndexCommitListener}
     */
    protected Engine.IndexCommitListener createIndexCommitListener() {
        final StatelessCommitService statelessCommitService = commitService.get();
        return new Engine.IndexCommitListener() {
            @Override
            public void onNewCommit(
                ShardId shardId,
                Store store,
                long primaryTerm,
                Engine.IndexCommitRef indexCommitRef,
                Set<String> additionalFiles
            ) {
                final long translogRecoveryStartFile;
                translogRecoveryStartFile = getTranslogRecoveryStartFile(indexCommitRef);

                statelessCommitService.onCommitCreation(
                    new StatelessCommitRef(
                        shardId,
                        indexCommitRef,
                        getIndexCommitFileNames(indexCommitRef.getIndexCommit()),
                        additionalFiles,
                        primaryTerm,
                        translogRecoveryStartFile
                    )
                );
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                statelessCommitService.markCommitDeleted(shardId, getIndexCommitFileNames(deletedCommit));
            }
        };
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new StatelessAllocationDecider());
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Map.of(NAME, new StatelessExistingShardsAllocator());
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(new StatelessIndexSettingProvider());
    }

    @Override
    public Optional<PersistedStateFactory> getPersistedStateFactory() {
        return Optional.of((settings, transportService, persistedClusterStateService) -> {
            assert persistedClusterStateService instanceof StatelessPersistedClusterStateService;
            return ((StatelessPersistedClusterStateService) persistedClusterStateService).createPersistedState(
                settings,
                transportService.getLocalNode()
            );
        });
    }

    @Override
    public Optional<PersistedClusterStateServiceFactory> getPersistedClusterStateServiceFactory() {
        return Optional.of(
            (nodeEnvironment, xContentRegistry, clusterSettings, threadPool) -> new StatelessPersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry,
                clusterSettings,
                threadPool::relativeTimeInMillis,
                electionStrategy::get,
                objectStoreService::get,
                threadPool
            )
        );
    }

    @Override
    public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
        return Optional.of(SingleNodeReconfigurator::new);
    }

    @Override
    public Optional<PreVoteCollector.Factory> getPreVoteCollectorFactory() {
        return Optional.of(
            (
                transportService,
                startElection,
                updateMaxTermSeen,
                electionStrategy,
                nodeHealthService,
                leaderHeartbeatService) -> new AtomicRegisterPreVoteCollector((StoreHeartbeatService) leaderHeartbeatService, startElection)
        );
    }

    @Override
    public Optional<LeaderHeartbeatService> getLeaderHeartbeatService(Settings settings) {
        return Optional.of(Objects.requireNonNull(storeHeartbeatService.get()));
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        return Map.of(StatelessElectionStrategy.NAME, Objects.requireNonNull(electionStrategy.get()));
    }

    /**
     * Validates that stateless can work with the given node settings.
     */
    private static void validateSettings(final Settings settings) {
        if (STATELESS_ENABLED.get(settings) == false) {
            throw new IllegalArgumentException(NAME + " is not enabled");
        }
        var nonStatelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
            .stream()
            .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toSet());
        if (nonStatelessDataNodeRoles.isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " does not support roles " + nonStatelessDataNodeRoles);
        }
        logger.info("{} is enabled", NAME);
        if (Objects.equals(SHARDS_ALLOCATOR_TYPE_SETTING.get(settings), DESIRED_BALANCE_ALLOCATOR) == false) {
            throw new IllegalArgumentException(
                NAME + " can only be used with " + SHARDS_ALLOCATOR_TYPE_SETTING.getKey() + "=" + DESIRED_BALANCE_ALLOCATOR
            );
        }
    }

    private long getTranslogRecoveryStartFile(Engine.IndexCommitRef indexCommitRef) {
        final long translogRecoveryStartFile;
        try {
            Map<String, String> userData = indexCommitRef.getIndexCommit().getUserData();
            String startFile = userData.get(IndexEngine.TRANSLOG_RECOVERY_START_FILE);
            if (startFile == null) {
                // If we don't have the TRANSLOG_RECOVERY_START_FILE in the user data, then this is the first commit after a recovery and no
                // operations have been processed on this node.
                translogRecoveryStartFile = translogReplicator.get().getMaxUploadedFile() + 1;
            } else {
                translogRecoveryStartFile = Long.parseLong(startFile);
            }
        } catch (IOException e) {
            assert false : e; // should never happen, none of the Lucene implementations throw this.
            throw new UncheckedIOException(e);
        }
        return translogRecoveryStartFile;
    }

    private static Collection<String> getIndexCommitFileNames(IndexCommit commit) {
        try {
            return commit.getFileNames();
        } catch (IOException e) {
            assert false : e; // should never happen, none of the Lucene implementations throw this.
            throw new UncheckedIOException(e);
        }
    }
}
