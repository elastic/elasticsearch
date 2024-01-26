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
import co.elastic.elasticsearch.stateless.autoscaling.indexing.TransportPublishNodeIngestLoadMetric;
import co.elastic.elasticsearch.stateless.autoscaling.memory.IndicesMappingSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.memory.IndicesMappingSizePublisher;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishHeapMemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchShardSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizesPublisher;
import co.elastic.elasticsearch.stateless.autoscaling.search.TransportPublishShardSizes;
import co.elastic.elasticsearch.stateless.cache.ClearBlobCacheRestHandler;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesResponse;
import co.elastic.elasticsearch.stateless.cache.action.TransportClearBlobCacheAction;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterStateCleanupService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessHeartbeatStore;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessPersistedClusterStateService;
import co.elastic.elasticsearch.stateless.cluster.coordination.TransportConsistentClusterStateReadAction;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottlingService;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.lucene.stats.GetAllShardSizesAction;
import co.elastic.elasticsearch.stateless.lucene.stats.GetShardSizeAction;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsClient;
import co.elastic.elasticsearch.stateless.metering.GetBlobStoreStatsRestHandler;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesResponse;
import co.elastic.elasticsearch.stateless.metering.action.TransportGetBlobStoreStatsAction;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTask;
import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTaskExecutor;
import co.elastic.elasticsearch.stateless.recovery.RecoveryCommitRegistrationHandler;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;
import co.elastic.elasticsearch.stateless.recovery.TransportSendRecoveryCommitRegistrationAction;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;
import co.elastic.elasticsearch.stateless.xpack.DummyESQLInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyEsqlUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyILMInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyILMUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyMonitoringInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyMonitoringUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyRollupInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyRollupUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummySearchableSnapshotsInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummySearchableSnapshotsUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyTransportGetRollupIndexCapsAction;
import co.elastic.elasticsearch.stateless.xpack.DummyVotingOnlyInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyVotingOnlyUsageTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyWatcherInfoTransportAction;
import co.elastic.elasticsearch.stateless.xpack.DummyWatcherUsageTransportAction;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.blobcache.BlobCacheMetrics;
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
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;

public class Stateless extends Plugin
    implements
        EnginePlugin,
        ActionPlugin,
        ClusterPlugin,
        ClusterCoordinationPlugin,
        ExtensiblePlugin,
        HealthPlugin,
        PersistentTaskPlugin {

    private static final Logger logger = LogManager.getLogger(Stateless.class);

    public static final String NAME = "stateless";

    public static final ActionType<GetBlobStoreStatsNodesResponse> GET_BLOB_STORE_STATS_ACTION = new ActionType<>(
        "cluster:monitor/" + NAME + "/blob_store/stats/get"
    );

    public static final ActionType<ClearBlobCacheNodesResponse> CLEAR_BLOB_CACHE_ACTION = new ActionType<>(
        "cluster:admin/" + NAME + "/blob_cache/clear"
    );

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public static final String SHARD_READ_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_READ_THREAD_NAME;
    public static final String SHARD_READ_THREAD_POOL_SETTING = "stateless." + SHARD_READ_THREAD_POOL + "_thread_pool";
    public static final String TRANSLOG_THREAD_POOL = BlobStoreRepository.STATELESS_TRANSLOG_THREAD_NAME;
    public static final String TRANSLOG_THREAD_POOL_SETTING = "stateless." + TRANSLOG_THREAD_POOL + "_thread_pool";
    public static final String SHARD_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_WRITE_THREAD_NAME;
    public static final String SHARD_WRITE_THREAD_POOL_SETTING = "stateless." + SHARD_WRITE_THREAD_POOL + "_thread_pool";

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    private final SetOnce<StatelessCommitService> commitService = new SetOnce<>();
    private final SetOnce<ObjectStoreService> objectStoreService = new SetOnce<>();
    private final SetOnce<SharedBlobCacheService<FileCacheKey>> sharedBlobCacheService = new SetOnce<>();
    private final SetOnce<BlobStoreHealthIndicator> blobStoreHealthIndicator = new SetOnce<>();
    private final SetOnce<TranslogReplicator> translogReplicator = new SetOnce<>();
    private final SetOnce<StatelessElectionStrategy> electionStrategy = new SetOnce<>();
    private final SetOnce<StoreHeartbeatService> storeHeartbeatService = new SetOnce<>();
    private final SetOnce<RefreshThrottlingService> refreshThrottlingService = new SetOnce<>();
    private final SetOnce<ShardSizeCollector> shardSizeCollector = new SetOnce<>();
    private final SetOnce<IndicesMappingSizeCollector> indicesMappingSizeCollector = new SetOnce<>();
    private final SetOnce<RecoveryCommitRegistrationHandler> recoveryCommitRegistrationHandler = new SetOnce<>();

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
            new ActionHandler<>(XPackInfoFeatureAction.ESQL, DummyESQLInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.ESQL, DummyEsqlUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.INDEX_LIFECYCLE, DummyILMInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.INDEX_LIFECYCLE, DummyILMUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.MONITORING, DummyMonitoringInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.MONITORING, DummyMonitoringUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ROLLUP, DummyRollupInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.ROLLUP, DummyRollupUsageTransportAction.class),
            new ActionHandler<>(GetRollupIndexCapsAction.INSTANCE, DummyTransportGetRollupIndexCapsAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.WATCHER, DummyWatcherInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.WATCHER, DummyWatcherUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.VOTING_ONLY, DummyVotingOnlyInfoTransportAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.VOTING_ONLY, DummyVotingOnlyUsageTransportAction.class),

            // autoscaling
            new ActionHandler<>(TransportPublishNodeIngestLoadMetric.INSTANCE, TransportPublishNodeIngestLoadMetric.class),
            new ActionHandler<>(TransportPublishShardSizes.INSTANCE, TransportPublishShardSizes.class),
            new ActionHandler<>(TransportPublishHeapMemoryMetrics.INSTANCE, TransportPublishHeapMemoryMetrics.class),
            new ActionHandler<>(GetAllShardSizesAction.INSTANCE, GetAllShardSizesAction.TransportGetAllShardSizes.class),
            new ActionHandler<>(GetShardSizeAction.INSTANCE, GetShardSizeAction.TransportGetShardSize.class),

            new ActionHandler<>(CLEAR_BLOB_CACHE_ACTION, TransportClearBlobCacheAction.class),
            new ActionHandler<>(GET_BLOB_STORE_STATS_ACTION, TransportGetBlobStoreStatsAction.class),
            new ActionHandler<>(TransportNewCommitNotificationAction.TYPE, TransportNewCommitNotificationAction.class),
            new ActionHandler<>(StatelessPrimaryRelocationAction.TYPE, TransportStatelessPrimaryRelocationAction.class),
            new ActionHandler<>(TransportRegisterCommitForRecoveryAction.TYPE, TransportRegisterCommitForRecoveryAction.class),
            new ActionHandler<>(TransportSendRecoveryCommitRegistrationAction.TYPE, TransportSendRecoveryCommitRegistrationAction.class),
            new ActionHandler<>(TransportConsistentClusterStateReadAction.TYPE, TransportConsistentClusterStateReadAction.class)
        );
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
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
        var settings = Settings.builder()
            .put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), StatelessElectionStrategy.NAME)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0);
        if (sharedCachedSettingExplicitlySet == false) {
            if (hasSearchRole) {
                settings.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "90%")
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
        settings.put(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {
        Client client = services.client();
        ClusterService clusterService = services.clusterService();
        ThreadPool threadPool = services.threadPool();
        Environment environment = services.environment();
        NodeEnvironment nodeEnvironment = services.nodeEnvironment();
        IndicesService indicesService = services.indicesService();

        final Collection<Object> components = new ArrayList<>();
        // use the settings that include additional settings.
        Settings settings = environment.settings();
        var objectStoreService = setAndGet(
            this.objectStoreService,
            createObjectStoreService(settings, services.repositoriesServiceSupplier(), threadPool, clusterService)
        );
        components.add(objectStoreService);
        var sharedBlobCacheServiceSupplier = new SharedBlobCacheServiceSupplier(
            setAndGet(this.sharedBlobCacheService, createSharedBlobCacheService(services, nodeEnvironment, settings, threadPool))
        );
        components.add(sharedBlobCacheServiceSupplier);
        var statelessElectionStrategy = setAndGet(
            this.electionStrategy,
            new StatelessElectionStrategy(objectStoreService::getClusterStateBlobContainer, threadPool)
        );
        setAndGet(
            this.storeHeartbeatService,
            StoreHeartbeatService.create(
                new StatelessHeartbeatStore(objectStoreService::getClusterStateHeartbeatContainer, threadPool),
                threadPool,
                environment.settings(),
                statelessElectionStrategy::getCurrentLeaseTerm
            )
        );
        var consistencyService = new StatelessClusterConsistencyService(clusterService, statelessElectionStrategy);
        components.add(consistencyService);
        var commitCleaner = new StatelessCommitCleaner(consistencyService, threadPool, objectStoreService);
        components.add(commitCleaner);
        var commitService = new StatelessCommitService(settings, objectStoreService, clusterService, client, commitCleaner);
        components.add(commitService);
        var clusterStateCleanupService = new StatelessClusterStateCleanupService(threadPool, objectStoreService, clusterService);
        clusterService.addListener(clusterStateCleanupService);
        // Allow wrapping non-Guiced version for testing
        commitService = wrapStatelessCommitService(commitService);
        clusterService.addListener(commitService);
        setAndGet(this.commitService, commitService);
        var translogReplicator = setAndGet(
            this.translogReplicator,
            new TranslogReplicator(threadPool, settings, objectStoreService, consistencyService, indicesService)
        );
        components.add(translogReplicator);
        var refreshThrottlingService = setAndGet(this.refreshThrottlingService, new RefreshThrottlingService(settings, clusterService));
        components.add(refreshThrottlingService);

        // autoscaling
        // memory
        var indexMappingSizePublisher = new IndicesMappingSizePublisher(client);
        var indicesMappingSizeCollector = setAndGet(
            this.indicesMappingSizeCollector,
            IndicesMappingSizeCollector.create(
                hasIndexRole,
                clusterService,
                indicesService,
                indexMappingSizePublisher,
                threadPool,
                settings
            )
        );
        components.add(indicesMappingSizeCollector);

        var memoryMetricsService = new MemoryMetricsService(threadPool::relativeTimeInNanos, clusterService.getClusterSettings());
        clusterService.addListener(memoryMetricsService);
        components.add(memoryMetricsService);

        if (hasIndexRole) {
            var ingestLoadPublisher = new IngestLoadPublisher(client, threadPool);
            var writeLoadSampler = AverageWriteLoadSampler.create(threadPool, settings, clusterService.getClusterSettings());
            var ingestLoadProbe = new IngestLoadProbe(clusterService.getClusterSettings(), writeLoadSampler::getExecutorStats);
            var ingestLoadSampler = new IngestLoadSampler(
                threadPool,
                writeLoadSampler,
                ingestLoadPublisher,
                ingestLoadProbe::getIngestionLoad,
                EsExecutors.nodeProcessors(settings).count(),
                clusterService.getClusterSettings()
            );
            clusterService.addListener(ingestLoadSampler);
            components.add(ingestLoadSampler);
        }
        var ingestMetricService = new IngestMetricsService(
            clusterService.getClusterSettings(),
            threadPool::relativeTimeInNanos,
            memoryMetricsService
        );
        clusterService.addListener(ingestMetricService);
        components.add(ingestMetricService);

        final ShardSizeCollector shardSizeCollector;
        if (hasSearchRole) {
            var searchShardSizeCollector = new SearchShardSizeCollector(
                clusterService.getClusterSettings(),
                threadPool,
                new ShardSizeStatsClient(client),
                new ShardSizesPublisher(client)
            );
            clusterService.addListener(searchShardSizeCollector);
            shardSizeCollector = searchShardSizeCollector;
        } else {
            shardSizeCollector = ShardSizeCollector.NOOP;
        }
        components.add(new PluginComponentBinding<>(ShardSizeCollector.class, setAndGet(this.shardSizeCollector, shardSizeCollector)));
        var searchMetricsService = SearchMetricsService.create(
            clusterService.getClusterSettings(),
            threadPool,
            clusterService,
            memoryMetricsService
        );
        components.add(searchMetricsService);

        recoveryCommitRegistrationHandler.set(new RecoveryCommitRegistrationHandler(client, clusterService));

        if (hasIndexRole) {
            components.add(new IndexingDiskController(nodeEnvironment, settings, threadPool, indicesService, commitService));
        }
        components.add(
            setAndGet(
                blobStoreHealthIndicator,
                new BlobStoreHealthIndicator(settings, clusterService, electionStrategy.get(), threadPool::relativeTimeInMillis).init()
            )
        );
        return components;
    }

    protected ObjectStoreService createObjectStoreService(
        Settings settings,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        return new ObjectStoreService(settings, repositoriesServiceSupplier, threadPool, clusterService);
    }

    protected SharedBlobCacheService<FileCacheKey> createSharedBlobCacheService(
        PluginServices services,
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool
    ) {
        return new SharedBlobCacheService<>(
            nodeEnvironment,
            settings,
            threadPool,
            SHARD_READ_THREAD_POOL,
            new BlobCacheMetrics(services.telemetryProvider().getMeterRegistry())
        );
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(blobStoreHealthIndicator.get());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(statelessExecutorBuilders(settings, hasIndexRole));
    }

    public static ExecutorBuilder<?>[] statelessExecutorBuilders(Settings settings, boolean hasIndexRole) {
        // TODO: Consider modifying these pool counts if we change the object store client connections based on node size.
        // Right now we have 10 threads for snapshots, 1 or 8 threads for translog and 20 or 28 threads for shard thread pools. This is to
        // attempt to keep the threads below the default client connections limit of 50. This assumption is currently broken by the snapshot
        // metadata pool having 50 threads. But we will continue to iterate on this numbers and limits.

        final int processors = EsExecutors.allocatedProcessors(settings);
        final int shardReadMaxThreads;
        final int translogCoreThreads;
        final int translogMaxThreads;
        final int shardWriteCoreThreads;
        final int shardWriteMaxThreads;

        if (hasIndexRole) {
            shardReadMaxThreads = Math.min(processors * 4, 10);
            translogCoreThreads = 2;
            translogMaxThreads = Math.min(processors * 2, 8);
            shardWriteCoreThreads = 2;
            shardWriteMaxThreads = Math.min(processors * 4, 10);
        } else {
            shardReadMaxThreads = Math.min(processors * 4, 28);
            translogCoreThreads = 0;
            translogMaxThreads = 1;
            shardWriteCoreThreads = 0;
            shardWriteMaxThreads = 1;
        }

        return new ExecutorBuilder<?>[] {
            new ScalingExecutorBuilder(
                SHARD_READ_THREAD_POOL,
                4,
                shardReadMaxThreads,
                TimeValue.timeValueSeconds(30L),
                true,
                SHARD_READ_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                TRANSLOG_THREAD_POOL,
                translogCoreThreads,
                translogMaxThreads,
                TimeValue.timeValueSeconds(30L),
                true,
                TRANSLOG_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                SHARD_WRITE_THREAD_POOL,
                shardWriteCoreThreads,
                shardWriteMaxThreads,
                TimeValue.timeValueSeconds(30L),
                true,
                SHARD_WRITE_THREAD_POOL_SETTING
            ) };
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

    protected StatelessCommitService wrapStatelessCommitService(StatelessCommitService instance) {
        return instance;
    }

    private static <T> T setAndGet(SetOnce<T> ref, T service) {
        ref.set(service);
        return service;
    }

    @Override
    public void close() throws IOException {
        Releasables.close(sharedBlobCacheService.get());
        try {
            IOUtils.close(blobStoreHealthIndicator.get());
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close the blob store health indicator service", e);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            STATELESS_ENABLED,
            ObjectStoreService.TYPE_SETTING,
            ObjectStoreService.BUCKET_SETTING,
            ObjectStoreService.CLIENT_SETTING,
            ObjectStoreService.BASE_PATH_SETTING,
            ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY,
            ObjectStoreService.OBJECT_STORE_SHUTDOWN_TIMEOUT,
            TranslogReplicator.FLUSH_RETRY_INITIAL_DELAY_SETTING,
            TranslogReplicator.FLUSH_INTERVAL_SETTING,
            TranslogReplicator.FLUSH_SIZE_SETTING,
            StoreHeartbeatService.HEARTBEAT_FREQUENCY,
            StoreHeartbeatService.MAX_MISSED_HEARTBEATS,
            IngestLoadSampler.SAMPLING_FREQUENCY_SETTING,
            IndicesMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING,
            IndicesMappingSizeCollector.CUT_OFF_TIMEOUT_SETTING,
            IndicesMappingSizeCollector.RETRY_INITIAL_DELAY_SETTING,
            MemoryMetricsService.STALE_METRICS_CHECK_DURATION_SETTING,
            MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING,
            IngestLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
            IngestMetricsService.ACCURATE_LOAD_WINDOW,
            IngestMetricsService.STALE_LOAD_WINDOW,
            IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
            SearchShardSizeCollector.PUSH_INTERVAL_SETTING,
            SearchShardSizeCollector.PUSH_DELTA_THRESHOLD_SETTING,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING,
            StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING,
            IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING,
            IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING,
            BlobStoreHealthIndicator.POLL_INTERVAL_SETTING,
            BlobStoreHealthIndicator.CHECK_TIMEOUT_SETTING,
            StatelessClusterStateCleanupService.CLUSTER_STATE_CLEANUP_DELAY_SETTING,
            StatelessClusterStateCleanupService.RETRY_TIMEOUT_SETTING,
            StatelessClusterStateCleanupService.RETRY_INITIAL_DELAY_SETTING,
            StatelessClusterStateCleanupService.RETRY_MAX_DELAY_SETTING,
            ObjectStoreGCTask.STALE_INDICES_GC_ENABLED_SETTING,
            ObjectStoreGCTask.STALE_TRANSLOGS_GC_ENABLED_SETTING,
            ObjectStoreGCTask.STALE_TRANSLOGS_GC_FILES_LIMIT_SETTING,
            ObjectStoreGCTask.GC_INTERVAL_SETTING
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        var statelessCommitService = commitService.get();
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (hasIndexRole) {

            indexModule.addIndexEventListener(indicesMappingSizeCollector.get());

            var localTranslogReplicator = translogReplicator.get();
            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    statelessCommitService.register(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
                    localTranslogReplicator.register(indexShard.shardId(), indexShard.getOperationPrimaryTerm(), seqNo -> {
                        var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
                        if (indexEngine != null) {
                            indexEngine.objectStorePersistedSeqNoConsumer().accept(seqNo);
                        }
                    });
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
                public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                    statelessCommitService.delete(shardId);
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
                    return new IndexDirectory(in, createSearchDirectory(sharedBlobCacheService.get(), shardRouting.shardId()));
                } else {
                    return in;
                }
            });
        }
        if (hasSearchRole) {
            final var collector = shardSizeCollector.get();
            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void afterIndexShardStarted(IndexShard indexShard) {
                    collector.collectShardSize(indexShard.shardId());
                }
            });
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isSearchable()) {
                    in.close();
                    return createSearchDirectory(sharedBlobCacheService.get(), shardRouting.shardId());
                } else {
                    return in;
                }
            });
        }
        indexModule.addIndexEventListener(
            new StatelessIndexEventListener(
                statelessCommitService,
                objectStoreService::get,
                translogReplicator::get,
                recoveryCommitRegistrationHandler::get
            )
        );
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
                    refreshThrottlingService.get().createRefreshThrottlerFactory(indexSettings),
                    commitService.get().closedLocalReadersForGeneration(newConfig.getShardId())
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
                statelessCommitService.markCommitDeleted(shardId, deletedCommit.getGeneration());
            }
        };
    }

    // protected to allow tests to override
    protected SearchDirectory createSearchDirectory(SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        return new SearchDirectory(cacheService, shardId);
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
            (
                nodeEnvironment,
                xContentRegistry,
                clusterSettings,
                threadPool,
                compatibilityVersions) -> new StatelessPersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry,
                    clusterSettings,
                    threadPool::relativeTimeInMillis,
                    electionStrategy::get,
                    objectStoreService::get,
                    threadPool,
                    compatibilityVersions
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

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(
            ObjectStoreGCTaskExecutor.create(clusterService, threadPool, client, objectStoreService::get, settingsModule.getSettings())
        );
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                ObjectStoreGCTask.TASK_NAME,
                ObjectStoreGCTaskExecutor.ObjectStoreGCTaskParams::new
            )
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(ObjectStoreGCTask.TASK_NAME),
                ObjectStoreGCTaskExecutor.ObjectStoreGCTaskParams::fromXContent
            )
        );
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
        if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.exists(settings)) {
            if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings)) {
                throw new IllegalArgumentException(
                    CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() + " cannot be enabled"
                );
            }
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

    protected Clock getClock() {
        return Clock.systemUTC();
    }
}
