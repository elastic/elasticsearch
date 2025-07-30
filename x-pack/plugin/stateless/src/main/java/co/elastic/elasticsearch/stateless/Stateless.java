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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.action.TransportEnsureDocsSearchableAction;
import co.elastic.elasticsearch.stateless.action.TransportFetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.allocation.EstimatedHeapUsageAllocationDecider;
import co.elastic.elasticsearch.stateless.allocation.StatelessAllocationDecider;
import co.elastic.elasticsearch.stateless.allocation.StatelessBalancingWeightsFactory;
import co.elastic.elasticsearch.stateless.allocation.StatelessExistingShardsAllocator;
import co.elastic.elasticsearch.stateless.allocation.StatelessIndexSettingProvider;
import co.elastic.elasticsearch.stateless.allocation.StatelessShardRoutingRoleStrategy;
import co.elastic.elasticsearch.stateless.api.DocValuesFormatFactory;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsProvider;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadProbe;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadPublisher;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.TransportPublishNodeIngestLoadMetric;
import co.elastic.elasticsearch.stateless.autoscaling.memory.HeapMemoryUsagePublisher;
import co.elastic.elasticsearch.stateless.autoscaling.memory.IndexingOperationsMemoryRequirementsSampler;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimatePublisher;
import co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishHeapMemoryMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishIndexingOperationsHeapMemoryRequirements;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishMergeMemoryEstimate;
import co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchShardSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizesPublisher;
import co.elastic.elasticsearch.stateless.autoscaling.search.TransportPublishShardSizes;
import co.elastic.elasticsearch.stateless.autoscaling.search.TransportUpdateReplicasAction;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.AverageSearchLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadProbe;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.TransportPublishSearchLoads;
import co.elastic.elasticsearch.stateless.cache.ClearBlobCacheRestHandler;
import co.elastic.elasticsearch.stateless.cache.SearchCommitPrefetcher;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessOnlinePrewarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesResponse;
import co.elastic.elasticsearch.stateless.cache.action.TransportClearBlobCacheAction;
import co.elastic.elasticsearch.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterStateCleanupService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessHeartbeatStore;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessPersistedClusterStateService;
import co.elastic.elasticsearch.stateless.cluster.coordination.TransportConsistentClusterStateReadAction;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure;
import co.elastic.elasticsearch.stateless.commits.HollowIndexEngineDeletionPolicy;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.IndexEngineDeletionPolicy;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.HollowShardsMetrics;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottlingService;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.lucene.stats.GetAllShardSizesAction;
import co.elastic.elasticsearch.stateless.lucene.stats.GetShardSizeAction;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsClient;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsReaderImpl;
import co.elastic.elasticsearch.stateless.metering.GetBlobStoreStatsRestHandler;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesResponse;
import co.elastic.elasticsearch.stateless.metering.action.TransportGetBlobStoreStatsAction;
import co.elastic.elasticsearch.stateless.multiproject.ProjectLifeCycleService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTask;
import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTaskExecutor;
import co.elastic.elasticsearch.stateless.recovery.RecoveryCommitRegistrationHandler;
import co.elastic.elasticsearch.stateless.recovery.RemedialAllocationSettingService;
import co.elastic.elasticsearch.stateless.recovery.RemoveRefreshClusterBlockService;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;
import co.elastic.elasticsearch.stateless.recovery.TransportSendRecoveryCommitRegistrationAction;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessUnpromotableRelocationAction;
import co.elastic.elasticsearch.stateless.recovery.metering.RecoveryMetricsCollector;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexService;
import co.elastic.elasticsearch.stateless.reshard.SplitSourceService;
import co.elastic.elasticsearch.stateless.reshard.SplitTargetService;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardSplitAction;
import co.elastic.elasticsearch.stateless.reshard.TransportUpdateSplitSourceStateAction;
import co.elastic.elasticsearch.stateless.reshard.TransportUpdateSplitStateAction;
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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.termvectors.EnsureDocsSearchableAction;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeightsFactory;
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
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecProvider;
import org.elasticsearch.index.engine.CombinedDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
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
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
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

    /** Temporary feature flag setting for creating indices with a refresh block. Defaults to false. **/
    public static final Setting<Boolean> USE_INDEX_REFRESH_BLOCK_SETTING = Setting.boolSetting(
        MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    // Thread pool names are defined in the BlobStoreRepository because we need to verify there that no requests are running on other pools.
    public static final String SHARD_READ_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_READ_THREAD_NAME;
    public static final String SHARD_READ_THREAD_POOL_SETTING = "stateless." + SHARD_READ_THREAD_POOL + "_thread_pool";
    public static final String TRANSLOG_THREAD_POOL = BlobStoreRepository.STATELESS_TRANSLOG_THREAD_NAME;
    public static final String TRANSLOG_THREAD_POOL_SETTING = "stateless." + TRANSLOG_THREAD_POOL + "_thread_pool";
    public static final String SHARD_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_WRITE_THREAD_NAME;
    public static final String SHARD_WRITE_THREAD_POOL_SETTING = "stateless." + SHARD_WRITE_THREAD_POOL + "_thread_pool";
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_CLUSTER_STATE_READ_WRITE_THREAD_NAME;
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING = "stateless."
        + CLUSTER_STATE_READ_WRITE_THREAD_POOL
        + "_thread_pool";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL = "stateless_get_vbcc_chunk";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL = "stateless_fill_vbcc_cache";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL
        + "_thread_pool";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        + "_thread_pool";
    public static final String PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_PREWARMING_THREAD_NAME;
    public static final String PREWARM_THREAD_POOL_SETTING = "stateless." + PREWARM_THREAD_POOL + "_thread_pool";
    public static final String UPLOAD_PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME;
    public static final String UPLOAD_PREWARM_THREAD_POOL_SETTING = "stateless." + UPLOAD_PREWARM_THREAD_POOL + "_thread_pool";

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);
    private final SetOnce<SplitTargetService> splitTargetService = new SetOnce<>();
    private final SetOnce<SplitSourceService> splitSourceService = new SetOnce<>();
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();
    private final SetOnce<Executor> commitSuccessExecutor = new SetOnce<>();
    private final SetOnce<StatelessCommitService> commitService = new SetOnce<>();
    private final SetOnce<ClosedShardService> closedShardService = new SetOnce<>();
    private final SetOnce<ObjectStoreService> objectStoreService = new SetOnce<>();
    private final SetOnce<StatelessSharedBlobCacheService> sharedBlobCacheService = new SetOnce<>();
    private final SetOnce<CacheBlobReaderService> cacheBlobReaderService = new SetOnce<>();
    private final SetOnce<SharedBlobCacheWarmingService> sharedBlobCacheWarmingService = new SetOnce<>();
    private final SetOnce<BlobStoreHealthIndicator> blobStoreHealthIndicator = new SetOnce<>();
    private final SetOnce<TranslogReplicator> translogReplicator = new SetOnce<>();
    private final SetOnce<TranslogRecoveryMetrics> translogReplicatorMetrics = new SetOnce<>();
    private final SetOnce<HollowShardsMetrics> hollowShardMetrics = new SetOnce<>();
    private final SetOnce<StatelessElectionStrategy> electionStrategy = new SetOnce<>();
    private final SetOnce<StoreHeartbeatService> storeHeartbeatService = new SetOnce<>();
    private final SetOnce<RefreshThrottlingService> refreshThrottlingService = new SetOnce<>();
    private final SetOnce<HollowShardsService> hollowShardsService = new SetOnce<>();
    private final SetOnce<ShardSizeCollector> shardSizeCollector = new SetOnce<>();
    private final SetOnce<ShardsMappingSizeCollector> shardsMappingSizeCollector = new SetOnce<>();
    private final SetOnce<RecoveryCommitRegistrationHandler> recoveryCommitRegistrationHandler = new SetOnce<>();
    private final SetOnce<RecoveryMetricsCollector> recoveryMetricsCollector = new SetOnce<>();
    private final SetOnce<DocumentParsingProvider> documentParsingProvider = new SetOnce<>();
    private final SetOnce<BlobCacheMetrics> blobCacheMetrics = new SetOnce<>();
    private final SetOnce<IndicesService> indicesService = new SetOnce<>();
    private final SetOnce<Predicate<ShardId>> skipMerges = new SetOnce<>();
    private final SetOnce<ProjectResolver> projectResolver = new SetOnce<>();
    private final SetOnce<ReshardIndexService> reshardIndexService = new SetOnce<>();
    private final SetOnce<MemoryMetricsService> memoryMetricsService = new SetOnce<>();
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();
    private final SetOnce<SearchCommitPrefetcher.PrefetchExecutor> prefetchExecutor = new SetOnce<>();

    private final boolean sharedCachedSettingExplicitlySet;
    private final boolean sharedCacheMmapExplicitlySet;
    private final boolean useRealMemoryCircuitBreakerExplicitlySet;
    private final boolean pageCacheReyclerLimitExplicitlySet;

    private final boolean hasSearchRole;
    private final boolean hasIndexRole;
    private final boolean hasMasterRole;
    private final ProjectType projectType;
    private final StatelessIndexSettingProvider statelessIndexSettingProvider;
    private final boolean hollowShardsEnabled;

    // visible for testing
    protected Function<Codec, Codec> codecWrapper = Function.identity();

    private ObjectStoreService getObjectStoreService() {
        return Objects.requireNonNull(this.objectStoreService.get());
    }

    private StatelessCommitService getCommitService() {
        return Objects.requireNonNull(this.commitService.get());
    }

    public Stateless(Settings settings) {
        validateSettings(settings);
        logSettings(settings);
        // It is dangerous to retain these settings because they will be further modified after this ctor due
        // to the call to #additionalSettings. We only parse out the components that has already been set.
        sharedCachedSettingExplicitlySet = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings);
        sharedCacheMmapExplicitlySet = SharedBlobCacheService.SHARED_CACHE_MMAP.exists(settings);
        useRealMemoryCircuitBreakerExplicitlySet = HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.exists(settings);
        pageCacheReyclerLimitExplicitlySet = PageCacheRecycler.LIMIT_HEAP_SETTING.exists(settings);
        hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
        hasMasterRole = DiscoveryNode.isMasterNode(settings);
        projectType = PROJECT_TYPE.get(settings);
        this.statelessIndexSettingProvider = new StatelessIndexSettingProvider(projectType);
        hollowShardsEnabled = STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(XPackInfoFeatureAction.INDEX_LIFECYCLE, DummyILMInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.INDEX_LIFECYCLE, DummyILMUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.MONITORING, DummyMonitoringInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.MONITORING, DummyMonitoringUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.ROLLUP, DummyRollupInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.ROLLUP, DummyRollupUsageTransportAction.class),
            new ActionHandler(GetRollupIndexCapsAction.INSTANCE, DummyTransportGetRollupIndexCapsAction.class),
            new ActionHandler(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.SEARCHABLE_SNAPSHOTS, DummySearchableSnapshotsUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.WATCHER, DummyWatcherInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.WATCHER, DummyWatcherUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.VOTING_ONLY, DummyVotingOnlyInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.VOTING_ONLY, DummyVotingOnlyUsageTransportAction.class),

            // autoscaling
            new ActionHandler(TransportPublishNodeIngestLoadMetric.INSTANCE, TransportPublishNodeIngestLoadMetric.class),
            new ActionHandler(TransportPublishShardSizes.INSTANCE, TransportPublishShardSizes.class),
            new ActionHandler(TransportPublishHeapMemoryMetrics.INSTANCE, TransportPublishHeapMemoryMetrics.class),
            new ActionHandler(GetAllShardSizesAction.INSTANCE, GetAllShardSizesAction.TransportGetAllShardSizes.class),
            new ActionHandler(GetShardSizeAction.INSTANCE, GetShardSizeAction.TransportGetShardSize.class),
            new ActionHandler(TransportPublishSearchLoads.INSTANCE, TransportPublishSearchLoads.class),
            new ActionHandler(
                TransportPublishIndexingOperationsHeapMemoryRequirements.INSTANCE,
                TransportPublishIndexingOperationsHeapMemoryRequirements.class
            ),
            new ActionHandler(TransportPublishMergeMemoryEstimate.INSTANCE, TransportPublishMergeMemoryEstimate.class),

            new ActionHandler(CLEAR_BLOB_CACHE_ACTION, TransportClearBlobCacheAction.class),
            new ActionHandler(GET_BLOB_STORE_STATS_ACTION, TransportGetBlobStoreStatsAction.class),
            new ActionHandler(TransportNewCommitNotificationAction.TYPE, TransportNewCommitNotificationAction.class),
            new ActionHandler(TransportFetchShardCommitsInUseAction.TYPE, TransportFetchShardCommitsInUseAction.class),
            new ActionHandler(
                TransportGetVirtualBatchedCompoundCommitChunkAction.TYPE,
                TransportGetVirtualBatchedCompoundCommitChunkAction.class
            ),

            new ActionHandler(EnsureDocsSearchableAction.TYPE, TransportEnsureDocsSearchableAction.class),
            new ActionHandler(StatelessPrimaryRelocationAction.TYPE, TransportStatelessPrimaryRelocationAction.class),
            new ActionHandler(TransportRegisterCommitForRecoveryAction.TYPE, TransportRegisterCommitForRecoveryAction.class),
            new ActionHandler(TransportSendRecoveryCommitRegistrationAction.TYPE, TransportSendRecoveryCommitRegistrationAction.class),
            new ActionHandler(TransportConsistentClusterStateReadAction.TYPE, TransportConsistentClusterStateReadAction.class),
            new ActionHandler(TransportUpdateReplicasAction.TYPE, TransportUpdateReplicasAction.class),
            new ActionHandler(TransportUpdateSplitStateAction.TYPE, TransportUpdateSplitStateAction.class),
            new ActionHandler(TransportUpdateSplitSourceStateAction.TYPE, TransportUpdateSplitSourceStateAction.class),
            new ActionHandler(TransportReshardSplitAction.TYPE, TransportReshardSplitAction.class),
            new ActionHandler(TransportReshardAction.TYPE, TransportReshardAction.class),
            new ActionHandler(StatelessUnpromotableRelocationAction.TYPE, TransportStatelessUnpromotableRelocationAction.class)
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
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
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
        if (useRealMemoryCircuitBreakerExplicitlySet == false) {
            if (hasIndexRole) {
                settings.put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false);
            }
        }
        if (pageCacheReyclerLimitExplicitlySet == false) {
            if (hasIndexRole) {
                settings.put(PageCacheRecycler.LIMIT_HEAP_SETTING.getKey(), "3%");
            }
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
        // Explicitly disable the recovery source, as it is not needed in stateless mode.
        settings.put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {

        this.projectResolver.set(services.projectResolver());
        Client client = services.client();
        ClusterService clusterService = services.clusterService();
        this.clusterService.set(clusterService);
        ShardRoutingRoleStrategy shardRoutingRoleStrategy = services.allocationService().getShardRoutingRoleStrategy();
        RerouteService rerouteService = services.rerouteService();
        ThreadPool threadPool = setAndGet(this.threadPool, services.threadPool());
        setAndGet(this.commitSuccessExecutor, threadPool.generic());
        Environment environment = services.environment();
        // use the settings that include additional settings.
        Settings settings = environment.settings();
        NodeEnvironment nodeEnvironment = services.nodeEnvironment();
        IndicesService indicesService = setAndGet(this.indicesService, services.indicesService());
        final var blobCacheMetrics = setAndGet(
            this.blobCacheMetrics,
            new BlobCacheMetrics(services.telemetryProvider().getMeterRegistry())
        );

        final Collection<Object> components = new ArrayList<>();
        var objectStoreService = setAndGet(
            this.objectStoreService,
            createObjectStoreService(settings, services.repositoriesService(), threadPool, clusterService, projectResolver.get())
        );
        if (projectResolver.get().supportsMultipleProjects()) {
            clusterService.addStateApplier(objectStoreService);
        }
        var cacheService = createSharedBlobCacheService(nodeEnvironment, settings, threadPool, blobCacheMetrics);
        var sharedBlobCacheServiceSupplier = new SharedBlobCacheServiceSupplier(setAndGet(this.sharedBlobCacheService, cacheService));
        components.add(sharedBlobCacheServiceSupplier);
        var cacheBlobReaderService = setAndGet(
            this.cacheBlobReaderService,
            new CacheBlobReaderService(settings, cacheService, client, threadPool)
        );
        components.add(cacheBlobReaderService);
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
        var consistencyService = new StatelessClusterConsistencyService(clusterService, statelessElectionStrategy, threadPool, settings);
        components.add(consistencyService);
        var commitCleaner = new StatelessCommitCleaner(consistencyService, threadPool, objectStoreService);
        components.add(commitCleaner);
        var cacheWarmingService = createSharedBlobCacheWarmingService(cacheService, threadPool, services.telemetryProvider(), settings);
        setAndGet(this.sharedBlobCacheWarmingService, cacheWarmingService);
        var commitService = createStatelessCommitService(
            settings,
            objectStoreService,
            clusterService,
            indicesService,
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            services.telemetryProvider()
        );
        components.add(commitService);
        var clusterStateCleanupService = new StatelessClusterStateCleanupService(threadPool, objectStoreService, clusterService);
        clusterService.addListener(clusterStateCleanupService);
        // Allow wrapping non-Guiced version for testing
        commitService = wrapStatelessCommitService(commitService);
        clusterService.addListener(commitService);
        setAndGet(this.commitService, commitService);
        var closedShardService = new ClosedShardService();
        components.add(closedShardService);
        setAndGet(this.closedShardService, closedShardService);
        var translogReplicator = setAndGet(
            this.translogReplicator,
            new TranslogReplicator(threadPool, settings, objectStoreService, consistencyService)
        );
        setAndGet(this.translogReplicatorMetrics, new TranslogRecoveryMetrics(services.telemetryProvider().getMeterRegistry()));
        setAndGet(
            hollowShardMetrics,
            HollowShardsMetrics.from(services.telemetryProvider().getMeterRegistry(), this::amountOfHollowableShards)
        );
        components.add(hollowShardMetrics.get());
        components.add(new StatelessComponents(translogReplicator, objectStoreService));

        var indexShardCacheWarmer = new IndexShardCacheWarmer(
            objectStoreService,
            cacheWarmingService,
            threadPool,
            commitService.useReplicatedRanges()
        );
        components.add(indexShardCacheWarmer);

        var refreshThrottlingService = setAndGet(this.refreshThrottlingService, new RefreshThrottlingService(settings, clusterService));
        components.add(refreshThrottlingService);

        // We need to inject HollowShardsService into TransportStatelessPrimaryRelocationAction via DI, so it has to be
        // available on all nodes despite being useful only on indexing nodes
        var hollowShardsService = setAndGet(
            this.hollowShardsService,
            new HollowShardsService(settings, clusterService, indicesService, indexShardCacheWarmer, threadPool, hollowShardMetrics.get())
        );
        components.add(hollowShardsService);

        // autoscaling
        // memory
        var heapMemoryUsagePublisher = new HeapMemoryUsagePublisher(client);
        var shardsMappingSizeCollector = setAndGet(
            this.shardsMappingSizeCollector,
            ShardsMappingSizeCollector.create(hasIndexRole, clusterService, indicesService, heapMemoryUsagePublisher, threadPool, settings)
        );
        components.add(shardsMappingSizeCollector);

        var vbccChunksPressure = createVirtualBatchedCompoundCommitChunksPressure(
            settings,
            services.telemetryProvider().getMeterRegistry()
        );
        components.add(vbccChunksPressure);

        ProjectType projectType = ServerlessSharedSettings.PROJECT_TYPE.get(settings);
        this.memoryMetricsService.set(
            new MemoryMetricsService(
                threadPool::relativeTimeInNanos,
                clusterService.getClusterSettings(),
                projectType,
                services.telemetryProvider().getMeterRegistry()
            )
        );

        clusterService.addListener(memoryMetricsService.get());
        components.add(memoryMetricsService.get());

        if (hasIndexRole) {
            var ingestLoadPublisher = new IngestLoadPublisher(client, threadPool);
            var writeLoadSampler = AverageWriteLoadSampler.create(threadPool, settings, clusterService.getClusterSettings());
            var ingestLoadProbe = new IngestLoadProbe(clusterService.getClusterSettings(), writeLoadSampler::getExecutorStats);
            var ingestLoadSampler = new IngestLoadSampler(
                threadPool,
                writeLoadSampler,
                ingestLoadPublisher,
                ingestLoadProbe::getIngestionLoad,
                ingestLoadProbe::getExecutorIngestionLoad,
                EsExecutors.nodeProcessors(settings).count(),
                clusterService.getClusterSettings(),
                services.telemetryProvider().getMeterRegistry()
            );
            clusterService.addListener(ingestLoadSampler);
            components.add(ingestLoadSampler);
            var indexingPressureMonitor = services.indexingPressure();
            var indexingMemoryRequirementsSampler = new IndexingOperationsMemoryRequirementsSampler(
                settings,
                threadPool,
                client,
                () -> clusterService.state().getMinTransportVersion(),
                indexingPressureMonitor.getMaxAllowedOperationSizeInBytes()
            );
            indexingPressureMonitor.addListener(indexingMemoryRequirementsSampler);
            components.add(indexingMemoryRequirementsSampler);

            var mergeExecutorService = indicesService.getThreadPoolMergeExecutorService();
            if (mergeExecutorService != null) {
                var mergeMemoryEstimatePublisher = new MergeMemoryEstimatePublisher(threadPool, client);
                var mergeMemoryEstimateCollector = new MergeMemoryEstimateCollector(
                    clusterService.getClusterSettings(),
                    () -> clusterService.state().getMinTransportVersion(),
                    () -> clusterService.localNode().getEphemeralId(),
                    mergeMemoryEstimatePublisher::publish
                );
                mergeExecutorService.registerMergeEventListener(mergeMemoryEstimateCollector);
                clusterService.addListener(mergeMemoryEstimateCollector);
                components.add(mergeMemoryEstimateCollector);
                components.add(mergeMemoryEstimatePublisher);
            }
            if (projectResolver.get().supportsMultipleProjects()) {
                var projectLifeCycleService = new ProjectLifeCycleService(clusterService, objectStoreService, threadPool);
                components.add(projectLifeCycleService);
                clusterService.addListener(projectLifeCycleService);
            }
        }
        var ingestMetricService = new IngestMetricsService(
            clusterService.getClusterSettings(),
            threadPool::relativeTimeInNanos,
            memoryMetricsService.get(),
            services.telemetryProvider().getMeterRegistry()
        );
        clusterService.addListener(ingestMetricService);
        components.add(ingestMetricService);

        components.add(
            new PluginComponentBinding<>(ShardSizeStatsReader.class, new ShardSizeStatsReaderImpl(clusterService, indicesService))
        );
        final ShardSizeCollector shardSizeCollector;
        if (hasSearchRole) {
            var averageSearchLoadSampler = AverageSearchLoadSampler.create(threadPool, settings, clusterService.getClusterSettings());
            var searchLoadProbe = new SearchLoadProbe(clusterService.getClusterSettings(), averageSearchLoadSampler::getExecutorLoadStats);
            var searchLoadSampler = SearchLoadSampler.create(
                client,
                averageSearchLoadSampler,
                searchLoadProbe::getSearchLoad,
                searchLoadProbe::getSearchLoadQuality,
                clusterService,
                settings,
                threadPool
            );
            components.add(searchLoadSampler);
            var searchShardSizeCollector = createSearchShardSizeCollector(clusterService.getClusterSettings(), threadPool, client);
            clusterService.addListener(searchShardSizeCollector);
            components.add(new PluginComponentBinding<>(ShardSizeStatsProvider.class, searchShardSizeCollector));
            shardSizeCollector = searchShardSizeCollector;
        } else {
            components.add(new PluginComponentBinding<>(ShardSizeStatsProvider.class, ShardSizeStatsProvider.NOOP));
            shardSizeCollector = ShardSizeCollector.NOOP;
        }
        components.add(new PluginComponentBinding<>(ShardSizeCollector.class, setAndGet(this.shardSizeCollector, shardSizeCollector)));
        var searchMetricsService = SearchMetricsService.create(
            clusterService.getClusterSettings(),
            threadPool,
            clusterService,
            memoryMetricsService.get(),
            services.telemetryProvider().getMeterRegistry()
        );
        components.add(searchMetricsService);
        var replicationUpdaterService = new ReplicasUpdaterService(threadPool, clusterService, (NodeClient) client, searchMetricsService);
        components.add(replicationUpdaterService);

        recoveryCommitRegistrationHandler.set(new RecoveryCommitRegistrationHandler(client, clusterService));

        statelessIndexSettingProvider.initialize(clusterService, services.indexNameExpressionResolver());

        if (hasIndexRole) {
            components.add(new IndexingDiskController(nodeEnvironment, settings, threadPool, indicesService, commitService));
        }
        components.add(
            setAndGet(
                blobStoreHealthIndicator,
                new BlobStoreHealthIndicator(settings, clusterService, electionStrategy.get(), threadPool::relativeTimeInMillis).init()
            )
        );
        components.add(setAndGet(recoveryMetricsCollector, new RecoveryMetricsCollector(services.telemetryProvider())));
        documentParsingProvider.set(services.documentParsingProvider());
        skipMerges.set(new ShouldSkipMerges(indicesService));
        if (hasMasterRole && USE_INDEX_REFRESH_BLOCK_SETTING.get(settings)) {
            components.add(new RemoveRefreshClusterBlockService(settings, clusterService, threadPool));
        }

        // Resharding
        var reshardIndexService = setAndGet(
            this.reshardIndexService,
            createMetadataReshardIndexService(clusterService, shardRoutingRoleStrategy, rerouteService, indicesService, threadPool)
        );
        components.add(reshardIndexService);
        splitTargetService.set(new SplitTargetService(settings, client, clusterService, reshardIndexService));
        var splitSourceService = setAndGet(
            this.splitSourceService,
            new SplitSourceService(client, clusterService, indicesService, commitService, objectStoreService, reshardIndexService)
        );
        components.add(splitSourceService);

        if (hasSearchRole) {
            setAndGet(this.prefetchExecutor, new SearchCommitPrefetcher.PrefetchExecutor(threadPool));
        }

        if (hasMasterRole) {
            var remedialSettingService = new RemedialAllocationSettingService(clusterService);
            clusterService.addListener(remedialSettingService);
        }

        return components;
    }

    protected SearchShardSizeCollector createSearchShardSizeCollector(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        Client client
    ) {
        return new SearchShardSizeCollector(clusterSettings, threadPool, new ShardSizeStatsClient(client), new ShardSizesPublisher(client));
    }

    protected ObjectStoreService createObjectStoreService(
        Settings settings,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        return new ObjectStoreService(settings, repositoriesService, threadPool, clusterService, projectResolver);
    }

    protected StatelessSharedBlobCacheService createSharedBlobCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics
    ) {
        StatelessSharedBlobCacheService statelessSharedBlobCacheService = new StatelessSharedBlobCacheService(
            nodeEnvironment,
            settings,
            threadPool,
            blobCacheMetrics
        );
        statelessSharedBlobCacheService.assertInvariants();
        return statelessSharedBlobCacheService;
    }

    public SharedBlobCacheWarmingService getSharedBlobCacheWarmingService() {
        return Objects.requireNonNull(sharedBlobCacheWarmingService.get());
    }

    public StatelessSharedBlobCacheService getStatelessSharedBlobCacheService() {
        return Objects.requireNonNull(sharedBlobCacheService.get());
    }

    // Can be overridden by tests
    protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        Settings settings
    ) {
        return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings);
    }

    protected ReshardIndexService createMetadataReshardIndexService(
        ClusterService clusterService,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy,
        RerouteService rerouteService,
        IndicesService indicesService,
        ThreadPool threadPool
    ) {
        return new ReshardIndexService(clusterService, shardRoutingRoleStrategy, rerouteService, indicesService, threadPool);
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
        final int clusterStateReadWriteCoreThreads;
        final int clusterStateReadWriteMaxThreads;
        final int getVirtualBatchedCompoundCommitChunkCoreThreads;
        final int getVirtualBatchedCompoundCommitChunkMaxThreads;
        final int fillVirtualBatchedCompoundCommitCacheCoreThreads;
        final int fillVirtualBatchedCompoundCommitCacheMaxThreads;
        final int prewarmMaxThreads = Math.min(processors * 4, 32);
        final int uploadPrewarmCoreThreads;
        final int uploadPrewarmMaxThreads;
        final int mergeCoreThreads;
        final int mergeMaxThreads;

        if (hasIndexRole) {
            shardReadMaxThreads = Math.min(processors * 4, 10);
            translogCoreThreads = 2;
            translogMaxThreads = Math.min(processors * 2, 8);
            shardWriteCoreThreads = 2;
            shardWriteMaxThreads = Math.min(processors * 4, 10);
            clusterStateReadWriteCoreThreads = 2;
            clusterStateReadWriteMaxThreads = 4;
            getVirtualBatchedCompoundCommitChunkCoreThreads = 1;
            getVirtualBatchedCompoundCommitChunkMaxThreads = Math.min(processors, 4);
            fillVirtualBatchedCompoundCommitCacheCoreThreads = 0;
            fillVirtualBatchedCompoundCommitCacheMaxThreads = 1;
            // These threads are used for prewarming the shared blob cache on upload, and are separate from the prewarm thread pool
            // in order to avoid any deadlocks between the two (e.g., when two fillgaps compete). Since they are used to prewarm on upload,
            // we use the same amount of max threads as the shard write pool.
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            uploadPrewarmMaxThreads = Math.min(processors * 4, 10);
            uploadPrewarmCoreThreads = uploadPrewarmMaxThreads / 2;
            mergeCoreThreads = 1;
            mergeMaxThreads = processors;
        } else {
            shardReadMaxThreads = Math.min(processors * 4, 28);
            translogCoreThreads = 0;
            translogMaxThreads = 1;
            shardWriteCoreThreads = 0;
            shardWriteMaxThreads = 1;
            clusterStateReadWriteCoreThreads = 0;
            clusterStateReadWriteMaxThreads = 1;
            getVirtualBatchedCompoundCommitChunkCoreThreads = 0;
            getVirtualBatchedCompoundCommitChunkMaxThreads = 1;
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            fillVirtualBatchedCompoundCommitCacheCoreThreads = Math.max(processors / 2, 2);
            fillVirtualBatchedCompoundCommitCacheMaxThreads = Math.max(processors, 2);
            uploadPrewarmCoreThreads = 0;
            uploadPrewarmMaxThreads = 1;
            mergeCoreThreads = 0;
            mergeMaxThreads = 1;
        }

        return new ExecutorBuilder<?>[] {
            new ScalingExecutorBuilder(
                SHARD_READ_THREAD_POOL,
                4,
                shardReadMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_READ_THREAD_POOL_SETTING,
                EsExecutors.TaskTrackingConfig.builder().trackOngoingTasks().trackExecutionTime(0.3).build()
            ),
            new ScalingExecutorBuilder(
                TRANSLOG_THREAD_POOL,
                translogCoreThreads,
                translogMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                TRANSLOG_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                SHARD_WRITE_THREAD_POOL,
                shardWriteCoreThreads,
                shardWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                CLUSTER_STATE_READ_WRITE_THREAD_POOL,
                clusterStateReadWriteCoreThreads,
                clusterStateReadWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
                getVirtualBatchedCompoundCommitChunkCoreThreads,
                getVirtualBatchedCompoundCommitChunkMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL,
                fillVirtualBatchedCompoundCommitCacheCoreThreads,
                fillVirtualBatchedCompoundCommitCacheMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                PREWARM_THREAD_POOL,
                // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
                // threads around to reduce churn and re-use the existing buffers more
                prewarmMaxThreads / 2,
                prewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                PREWARM_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                UPLOAD_PREWARM_THREAD_POOL,
                uploadPrewarmCoreThreads,
                uploadPrewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                UPLOAD_PREWARM_THREAD_POOL_SETTING
            ) };
    }

    public MemoryMetricsService getMemoryMetricsService() {
        return memoryMetricsService.get();
    }

    public ClusterService getClusterService() {
        return clusterService.get();
    }

    /**
     * This class wraps the {@code sharedBlobCacheService} for use in dependency injection, as the sharedBlobCacheService's parameterized
     * type of {@code StatelessSharedBlobCacheService} is erased.
     */
    public static final class SharedBlobCacheServiceSupplier implements Supplier<StatelessSharedBlobCacheService> {

        private final StatelessSharedBlobCacheService sharedBlobCacheService;

        SharedBlobCacheServiceSupplier(StatelessSharedBlobCacheService sharedBlobCacheService) {
            this.sharedBlobCacheService = Objects.requireNonNull(sharedBlobCacheService);
        }

        @Override
        public StatelessSharedBlobCacheService get() {
            return sharedBlobCacheService;
        }
    }

    protected StatelessCommitService createStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        return new StatelessCommitService(
            settings,
            objectStoreService,
            clusterService,
            indicesService,
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            telemetryProvider
        );
    }

    protected GetVirtualBatchedCompoundCommitChunksPressure createVirtualBatchedCompoundCommitChunksPressure(
        Settings settings,
        MeterRegistry meterRegistry
    ) {
        return new GetVirtualBatchedCompoundCommitChunksPressure(settings, meterRegistry);
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
        // We should close the shared blob cache only after we made sure that all shards have been closed.
        try {
            if (indicesService.get().awaitClose(1, TimeUnit.MINUTES) == false) {
                logger.warn("Closing the Stateless services while some shards are still open");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
            ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS,
            TranslogReplicator.FLUSH_RETRY_INITIAL_DELAY_SETTING,
            TranslogReplicator.FLUSH_INTERVAL_SETTING,
            TranslogReplicator.FLUSH_SIZE_SETTING,
            IndexEngine.MERGE_PREWARM,
            IndexEngine.MERGE_FORCE_REFRESH_SIZE,
            StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING,
            StoreHeartbeatService.HEARTBEAT_FREQUENCY,
            StoreHeartbeatService.MAX_MISSED_HEARTBEATS,
            IngestLoadSampler.SAMPLING_FREQUENCY_SETTING,
            ShardsMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING,
            ShardsMappingSizeCollector.CUT_OFF_TIMEOUT_SETTING,
            ShardsMappingSizeCollector.RETRY_INITIAL_DELAY_SETTING,
            MergeMemoryEstimateCollector.MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO,
            MemoryMetricsService.STALE_METRICS_CHECK_DURATION_SETTING,
            MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
            MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
            MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
            MemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
            MemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
            IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING,
            IngestLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
            IngestMetricsService.ACCURATE_LOAD_WINDOW,
            IngestMetricsService.STALE_LOAD_WINDOW,
            IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW,
            IngestMetricsService.MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN,
            IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR,
            AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
            AverageWriteLoadSampler.QUEUE_SIZE_SAMPLER_EWMA_ALPHA_SETTING,
            SearchShardSizeCollector.PUSH_INTERVAL_SETTING,
            SearchShardSizeCollector.PUSH_DELTA_THRESHOLD_SETTING,
            SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
            SearchLoadSampler.SAMPLING_FREQUENCY_SETTING,
            SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING,
            AverageSearchLoadSampler.USE_VCPU_REQUEST,
            AverageSearchLoadSampler.SEARCH_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
            AverageSearchLoadSampler.SHARD_READ_SAMPLER_EWMA_ALPHA_SETTING,
            SearchMetricsService.ACCURATE_METRICS_WINDOW_SETTING,
            SearchMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL,
            ReplicasUpdaterService.REPLICA_UPDATER_SCALEDOWN_REPETITIONS,
            SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            SearchLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR,
            SearchLoadProbe.SHARD_READ_LOAD_THRESHOLD_SETTING,
            StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING,
            StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING,
            StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE,
            StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL,
            StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS,
            StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE,
            StatelessCommitService.STATELESS_UPLOAD_MAX_IO_ERROR_RETRIES,
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
            ObjectStoreGCTask.GC_INTERVAL_SETTING,
            StatelessIndexSettingProvider.DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING,
            TransportStatelessPrimaryRelocationAction.SLOW_SECONDARY_FLUSH_THRESHOLD_SETTING,
            TransportStatelessPrimaryRelocationAction.SLOW_HANDOFF_WARMING_THRESHOLD_SETTING,
            TransportStatelessPrimaryRelocationAction.SLOW_RELOCATION_THRESHOLD_SETTING,
            GetVirtualBatchedCompoundCommitChunksPressure.CHUNKS_BYTES_LIMIT,
            CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING,
            SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP,
            RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING,
            StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT,
            HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED,
            HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL,
            HollowShardsService.SETTING_HOLLOW_INGESTION_TTL,
            USE_INDEX_REFRESH_BLOCK_SETTING,
            RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING,
            SplitTargetService.RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT,
            IndexingOperationsMemoryRequirementsSampler.SAMPLE_VALIDITY_SETTING,
            StatelessBalancingWeightsFactory.SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            TransportStatelessUnpromotableRelocationAction.START_HANDOFF_CLUSTER_STATE_CONVERGENCE_TIMEOUT_SETTING,
            TransportStatelessUnpromotableRelocationAction.START_HANDOFF_REQUEST_TIMEOUT_SETTING,
            StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED,
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            EstimatedHeapUsageAllocationDecider.MINIMUM_LOGGING_INTERVAL,
            SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING,
            SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING,
            SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING,
            SearchCommitPrefetcher.PREFETCH_SEARCH_IDLE_TIME_SETTING,
            SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING,
            SearchCommitPrefetcher.FORCE_PREFETCH_SETTING
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        var statelessCommitService = commitService.get();
        var localTranslogReplicator = translogReplicator.get();
        var localSplitTargetService = splitTargetService.get();
        var localSplitSourceService = splitSourceService.get();
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (hasIndexRole) {

            indexModule.addIndexOperationListener(new StatelessIndexingOperationListener(hollowShardsService.get()));
            indexModule.addIndexEventListener(shardsMappingSizeCollector.get());

            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    statelessCommitService.register(
                        indexShard.shardId(),
                        indexShard.getOperationPrimaryTerm(),
                        () -> isInitializingNoSearchShards(indexShard),
                        indexShard::addGlobalCheckpointListener,
                        () -> {
                            Engine engineOrNull = indexShard.getEngineOrNull();
                            if (engineOrNull instanceof IndexEngine engine) {
                                engine.syncTranslogReplicator(ActionListener.noop());
                            } else if (engineOrNull == null) {
                                throw new AlreadyClosedException("engine is closed");
                            } else {
                                assert false : "Engine is " + engineOrNull;
                                throw new IllegalStateException("Engine is " + engineOrNull);
                            }
                        }
                    );
                    localTranslogReplicator.register(indexShard.shardId(), indexShard.getOperationPrimaryTerm(), seqNo -> {
                        var engine = indexShard.getEngineOrNull();
                        if (engine != null && engine instanceof IndexEngine indexEngine) {
                            indexEngine.objectStorePersistedSeqNoConsumer().accept(seqNo);
                            // The local checkpoint is updated as part of the post-replication actions of ReplicationOperation. However, if
                            // a bulk request has a refresh included, the post-replication actions happen after the refresh. And the refresh
                            // may need to wait for the checkpoint to progress in order to send out a new VBCC commit notification. To
                            // break this stalemate, we update the checkpoint as early as here, when the translog has persisted a seqno.
                            // We exclude the initializing state since the replication tracker may not yet be in primary mode and the local
                            // checkpoint is updated as part of recovery. We ignore errors since this is best effort.
                            try {
                                if (indexShard.routingEntry().state() != ShardRoutingState.INITIALIZING) {
                                    indexShard.updateLocalCheckpointForShard(
                                        indexShard.routingEntry().allocationId().getId(),
                                        indexEngine.getPersistedLocalCheckpoint()
                                    );
                                }
                            } catch (Exception e) {
                                logger.debug(() -> "Failed to update local checkpoint", e);
                            }
                        }
                    });
                    // We are pruning the archive for a given generation, only once we know all search shards are
                    // aware of that generation.
                    // TODO: In the context of real-time GET, this might be an overkill and in case of misbehaving
                    // search shards, this might lead to higher memory consumption on the indexing shards. Depending on
                    // how we respond to get requests that are not in the live version map (what generation we send back
                    // for the search shard to wait for), it could be safe to trigger the pruning earlier, e.g., once the
                    // commit upload is successful.
                    statelessCommitService.registerCommitNotificationSuccessListener(indexShard.shardId(), (gen) -> {
                        // We dispatch to a generic thread to avoid a transport worker being blocked to get the engine while it's reset
                        commitSuccessExecutor.get().execute(new AbstractRunnable() {
                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(
                                    () -> "["
                                        + indexShard.shardId()
                                        + "] failed to notify success of commit notification with generation"
                                        + gen,
                                    e
                                );
                            }

                            @Override
                            protected void doRun() throws Exception {
                                indexShard.withEngineOrNull(engine -> {
                                    if (engine != null && engine instanceof IndexEngine e) {
                                        e.commitSuccess(gen);
                                    }
                                    return null;
                                });
                            }

                            @Override
                            public String toString() {
                                return "commitSuccess[" + indexShard.shardId() + "]";
                            }
                        });
                    });
                }

                @Override
                public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                    if (indexShard != null) {
                        statelessCommitService.unregisterCommitNotificationSuccessListener(shardId);
                        statelessCommitService.closeShard(shardId);
                        localSplitTargetService.cancelSplits(indexShard);
                        localSplitSourceService.cancelSplits(indexShard);
                        hollowShardsService.get().removeHollowShard(indexShard, "index shard closed");
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
            if (hollowShardsEnabled == false) {
                indexModule.setIndexCommitListener(createIndexCommitListener());
            }
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isPromotableToPrimary()) {
                    Lucene.cleanLuceneIndex(in);
                    var indexCacheDirectory = createIndexBlobStoreCacheDirectory(sharedBlobCacheService.get(), shardRouting.shardId());
                    return new IndexDirectory(in, indexCacheDirectory, statelessCommitService::onGenerationalFileDeletion);
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

                @Override
                public void onStoreClosed(ShardId shardId) {
                    getClosedShardService().onStoreClose(shardId);
                }
            });
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isSearchable()) {
                    in.close();
                    return createSearchDirectory(
                        sharedBlobCacheService.get(),
                        cacheBlobReaderService.get(),
                        new AtomicMutableObjectStoreUploadTracker(),
                        shardRouting.shardId()
                    );
                } else {
                    return in;
                }
            });
        }
        indexModule.addIndexEventListener(
            new StatelessIndexEventListener(
                threadPool.get(),
                statelessCommitService,
                objectStoreService.get(),
                localTranslogReplicator,
                recoveryCommitRegistrationHandler.get(),
                sharedBlobCacheWarmingService.get(),
                hollowShardsService.get(),
                splitTargetService.get(),
                splitSourceService.get(),
                projectResolver.get()
            )
        );
        indexModule.addIndexEventListener(recoveryMetricsCollector.get());
    }

    protected IndexEngine newIndexEngine(
        EngineConfig engineConfig,
        TranslogReplicator translogReplicator,
        Function<String, BlobContainer> translogBlobContainer,
        StatelessCommitService statelessCommitService,
        HollowShardsService hollowShardsService,
        SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
        RefreshThrottler.Factory refreshThrottlerFactory,
        DocumentParsingProvider documentParsingProvider,
        IndexEngine.EngineMetrics engineMetrics
    ) {
        return new IndexEngine(
            engineConfig,
            translogReplicator,
            translogBlobContainer,
            statelessCommitService,
            hollowShardsService,
            sharedBlobCacheWarmingService,
            refreshThrottlerFactory,
            statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
            documentParsingProvider,
            engineMetrics,
            skipMerges.get(),
            statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
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
                    (data, seqNo, location) -> replicator.add(translogConfig.getShardId(), data, seqNo, location),
                    false // translog is replicated to the object store, no need fsync that
                );
                var collectorRefreshListener = refreshListenerForShardMappingSizeCollector(config.getShardId());
                var internalRefreshListeners = config.getInternalRefreshListener();
                if (internalRefreshListeners == null) {
                    internalRefreshListeners = List.of(collectorRefreshListener);
                } else {
                    internalRefreshListeners = CollectionUtils.appendToCopy(internalRefreshListeners, collectorRefreshListener);
                }

                final var shardLocalCommitsTracker = getCommitService().getShardLocalCommitsTracker(config.getShardId());
                assert shardLocalCommitsTracker != null : config.getShardId();

                final IndexEngineDeletionPolicy.CommitsListener indexEngineDeletionPolicyCommitsListener;
                if (hollowShardsEnabled) {
                    final var commitsListener = createIndexCommitListener();
                    indexEngineDeletionPolicyCommitsListener = new IndexEngineDeletionPolicy.CommitsListener() {
                        @Override
                        public void onNewCommit(Engine.IndexCommitRef indexCommitRef, Set<String> additionalFiles) {
                            var store = config.getStore();
                            store.incRef();
                            commitsListener.onNewCommit(
                                config.getShardId(),
                                config.getStore(),
                                config.getPrimaryTermSupplier().getAsLong(),
                                new Engine.IndexCommitRef(
                                    indexCommitRef.getIndexCommit(),
                                    () -> IOUtils.close(indexCommitRef, store::decRef)
                                ),
                                additionalFiles
                            );
                        }

                        @Override
                        public void onCommitDeleted(IndexCommit indexCommit) {
                            commitsListener.onIndexCommitDelete(config.getShardId(), indexCommit);
                        }
                    };
                } else {
                    indexEngineDeletionPolicyCommitsListener = null;
                }

                EngineConfig newConfig = new EngineConfig(
                    config.getShardId(),
                    config.getThreadPool(),
                    config.getThreadPoolMergeExecutorService(),
                    config.getIndexSettings(),
                    config.getWarmer(),
                    config.getStore(),
                    getMergePolicy(config),
                    config.getAnalyzer(),
                    config.getSimilarity(),
                    getCodecProvider(config),
                    config.getEventListener(),
                    config.getQueryCache(),
                    config.getQueryCachingPolicy(),
                    newTranslogConfig,
                    config.getFlushMergesAfter(),
                    config.getExternalRefreshListener(),
                    internalRefreshListeners,
                    config.getIndexSort(),
                    config.getCircuitBreakerService(),
                    config.getGlobalCheckpointSupplier(),
                    config.retentionLeasesSupplier(),
                    config.getPrimaryTermSupplier(),
                    config.getSnapshotCommitSupplier(),
                    config.getLeafSorter(),
                    config.getRelativeTimeInNanosSupplier(),
                    config.getIndexCommitListener(),
                    config.isPromotableToPrimary(),
                    config.getMapperService(),
                    config.getEngineResetLock(),
                    config.getMergeMetrics(),
                    // Here we pass an index deletion policy wrapper to the engine. This is the only way we have to pass the
                    // LocalCommitsRefs and the listener to the IndexWriter's policy, because the IndexWriter is created during
                    // InternalEngine construction, before IndexEngine class attributes are set.
                    policy -> {
                        if (hollowShardsEnabled) {
                            // If there is no default policy, we assume it is an hollow index engine
                            if (policy instanceof CombinedDeletionPolicy combinedDeletionPolicy) {
                                return new IndexEngineDeletionPolicy(
                                    shardLocalCommitsTracker.shardLocalCommitsRefs(),
                                    combinedDeletionPolicy,
                                    indexEngineDeletionPolicyCommitsListener
                                );
                            } else {
                                assert policy == null : "Only expect CombinedDeletionPolicy or null policy";
                                return new HollowIndexEngineDeletionPolicy(shardLocalCommitsTracker.shardLocalCommitsRefs());
                            }
                        } else {
                            return policy;
                        }
                    }
                );
                SegmentInfos segmentCommitInfos;
                try {
                    segmentCommitInfos = config.getStore().readLastCommittedSegmentsInfo();
                } catch (IOException e) {
                    throw new EngineCreationFailureException(config.getShardId(), "failed to read last segments info", e);
                }

                // When recovering a hollow commit, or when relocation hollows a hollowable shard, the shard is not yet marked hollow
                // in the HollowShardService, and we want to use (or reset the engine to) the HollowIndexEngine. When unhollowing
                // a hollow shard, the shard is marked hollow in the HollowShardService and we would like to reset the engine to
                // the IndexEngine in order to flush a new blob and complete unhollowing.
                if (hollowShardsService.get().isFeatureEnabled()
                    && hollowShardsService.get().isHollowShard(config.getShardId()) == false
                    && IndexEngine.isLastCommitHollow(segmentCommitInfos)) {
                    logger.debug(
                        () -> config.getShardId()
                            + " using hollow engine for shard [generation: "
                            + segmentCommitInfos.getGeneration()
                            + "]"
                    );
                    return new HollowIndexEngine(newConfig, getCommitService(), hollowShardsService.get(), newConfig.getMapperService());
                }
                return newIndexEngine(
                    newConfig,
                    translogReplicator.get(),
                    getObjectStoreService()::getTranslogBlobContainer,
                    getCommitService(),
                    hollowShardsService.get(),
                    sharedBlobCacheWarmingService.get(),
                    refreshThrottlingService.get().createRefreshThrottlerFactory(indexSettings),
                    documentParsingProvider.get(),
                    new IndexEngine.EngineMetrics(translogReplicatorMetrics.get(), newConfig.getMergeMetrics(), hollowShardMetrics.get())
                );
            } else {
                assert prefetchExecutor.get() != null : "Prefetch executor should be instantiated in search nodes";
                return new SearchEngine(
                    config,
                    getClosedShardService(),
                    sharedBlobCacheService.get(),
                    clusterService.get().getClusterSettings(),
                    prefetchExecutor.get()
                );
            }
        });
    }

    protected CodecProvider getCodecProvider(EngineConfig engineConfig) {
        final var innerCodecProvider = engineConfig.getCodecProvider();
        final var availableCodecs = innerCodecProvider.availableCodecs();
        // Wrap all availableCodecs, so we create just one CodedProvider and one Codec per codec name.
        final var codecs = Arrays.stream(availableCodecs)
            .collect(
                Collectors.toUnmodifiableMap(Function.identity(), codecName -> codecWrapper.apply(innerCodecProvider.codec(codecName)))
            );
        return new CodecProvider() {
            @Override
            public Codec codec(String name) {
                var codec = codecs.get(name);
                assert codec != null;
                return codec;
            }

            @Override
            public String[] availableCodecs() {
                return availableCodecs;
            }
        };
    }

    protected org.apache.lucene.index.MergePolicy getMergePolicy(EngineConfig engineConfig) {
        return engineConfig.getMergePolicy();
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        var formatFactories = loader.loadExtensions(DocValuesFormatFactory.class);

        if (formatFactories.size() > 1) {
            throw new IllegalStateException(DocValuesFormatFactory.class + " may not have multiple implementations");
        } else if (formatFactories.size() == 1) {
            var formatFactory = formatFactories.get(0);
            this.codecWrapper = createCodecWrapper(formatFactory);
        }
    }

    // visible for testing
    protected Function<Codec, Codec> createCodecWrapper(DocValuesFormatFactory formatFactory) {
        return (parentCodec) -> {
            var parentCodecDocValuesFormat = parentCodec.docValuesFormat();
            var extensionDocValuesFormat = formatFactory.createDocValueFormat(parentCodecDocValuesFormat);
            return new FilterCodec(parentCodec.getName(), parentCodec) {
                private final DocValuesFormat rawStorageDocValuesFormat = extensionDocValuesFormat;

                @Override
                public DocValuesFormat docValuesFormat() {
                    return rawStorageDocValuesFormat;
                }
            };
        };
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
                final long translogReleaseEndFile;
                try {
                    Map<String, String> userData = indexCommitRef.getIndexCommit().getUserData();
                    String startFile = userData.get(IndexEngine.TRANSLOG_RECOVERY_START_FILE);
                    String releaseFile = userData.get(IndexEngine.TRANSLOG_RELEASE_END_FILE);
                    if (startFile == null) {
                        // If we don't have it in the user data, then this is the first commit and no operations have been processed on
                        // this node. So we set the translog start file to the next possible translog file, and the translog release end
                        // file to the ineffective value of -1.
                        assert releaseFile == null : "release file is set to " + releaseFile + " but start file is not set";
                        translogRecoveryStartFile = translogReplicator.get().getMaxUploadedFile() + 1;
                        translogReleaseEndFile = -1;
                    } else {
                        translogRecoveryStartFile = Long.parseLong(startFile);
                        if (releaseFile == null) {
                            // Only hollow commits contain a release file in the commit user data. If the release file is not set, then
                            // this is a regular non-hollow commit, and we can re-use the start file as the release file.
                            translogReleaseEndFile = translogRecoveryStartFile;
                        } else {
                            assert translogRecoveryStartFile == HOLLOW_TRANSLOG_RECOVERY_START_FILE
                                : "start file is set to " + startFile + " and release file is set to " + releaseFile;
                            translogReleaseEndFile = Long.parseLong(releaseFile);
                        }
                    }
                } catch (IOException e) {
                    assert false : e; // should never happen, none of the Lucene implementations throw this.
                    throw new UncheckedIOException(e);
                }

                statelessCommitService.onCommitCreation(
                    new StatelessCommitRef(
                        shardId,
                        indexCommitRef,
                        additionalFiles,
                        primaryTerm,
                        translogRecoveryStartFile,
                        translogReleaseEndFile
                    )
                );
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                statelessCommitService.markCommitDeleted(shardId, deletedCommit.getGeneration());
            }
        };
    }

    /**
     * Creates a refresh listener for an indexing shard. This listener notifies the
     * {@link ShardsMappingSizeCollector} whenever the underlying segments change,
     * allowing it to publish the updated estimated heap memory usage to the master node.
     */
    protected ReferenceManager.RefreshListener refreshListenerForShardMappingSizeCollector(ShardId shardId) {
        return new ReferenceManager.RefreshListener() {
            final ShardsMappingSizeCollector collector = shardsMappingSizeCollector.get();
            boolean first = true;

            @Override
            public void beforeRefresh() {

            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                if (first || didRefresh) {
                    collector.updateMappingMetricsForShard(shardId);
                    first = false;
                }
            }
        };
    }

    // protected to allow tests to override
    protected SearchDirectory createSearchDirectory(
        StatelessSharedBlobCacheService cacheService,
        CacheBlobReaderService cacheBlobReaderService,
        MutableObjectStoreUploadTracker objectStoreUploadTracker,
        ShardId shardId
    ) {
        return new SearchDirectory(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
    }

    protected IndexBlobStoreCacheDirectory createIndexBlobStoreCacheDirectory(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId
    ) {
        return new IndexBlobStoreCacheDirectory(cacheService, shardId);
    }

    private ClosedShardService getClosedShardService() {
        return Objects.requireNonNull(this.closedShardService.get());
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new StatelessAllocationDecider(), new EstimatedHeapUsageAllocationDecider(clusterSettings));
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Map.of(NAME, new StatelessExistingShardsAllocator());
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(statelessIndexSettingProvider);
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
                    compatibilityVersions,
                    () -> projectResolver.get().supportsMultipleProjects()
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

    @Override
    public BalancingWeightsFactory getBalancingWeightsFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
        if (clusterSettings.get(StatelessBalancingWeightsFactory.SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING)) {
            return new StatelessBalancingWeightsFactory(balancerSettings, clusterSettings);
        }
        // Returning null means we use the default (global) BalancingWeightsFactory
        return null;
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

    private static void logSettings(final Settings settings) {
        // TODO: Move the logging back to StatelessCommitService#new once ES-8507 is resolved
        final var bccMaxAmountOfCommits = StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.get(settings);
        final var bccUploadMaxSize = StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.get(settings);
        final var virtualBccUploadMaxAge = StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.get(settings);
        logger.info(
            "delayed upload with [max_commits={}], [max_size={}], [max_age={}]",
            bccMaxAmountOfCommits,
            bccUploadMaxSize.getStringRep(),
            virtualBccUploadMaxAge.getStringRep()
        );
    }

    private boolean isInitializingNoSearchShards(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        return shardRouting.initializing() && shardRouting.recoverySource().getType() != RecoverySource.Type.PEER;
    }

    private record ShouldSkipMerges(IndicesService indicesService) implements Predicate<ShardId> {

        @Override
        public boolean test(ShardId shardId) {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            return indexShard == null || indexShard.routingEntry().relocating();
        }
    }

    private long amountOfHollowableShards() {
        var hollowShardsService = this.hollowShardsService.get();
        if (hollowShardsService == null) {
            return 0;
        }
        long amountOfHollowableShards = 0;
        for (IndexService indexShards : indicesService.get()) {
            for (IndexShard indexShard : indexShards) {
                if (hollowShardsService.isHollowableIndexShard(indexShard)) {
                    amountOfHollowableShards++;
                }
            }
        }
        return amountOfHollowableShards;
    }
}
