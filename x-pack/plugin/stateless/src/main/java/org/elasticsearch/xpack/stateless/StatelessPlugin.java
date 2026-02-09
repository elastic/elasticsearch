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

package org.elasticsearch.xpack.stateless;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.api.DocValuesFormatFactory;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsProvider;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;

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
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.termvectors.EnsureDocsSearchableAction;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
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
import org.elasticsearch.cluster.routing.allocation.allocator.ShardRelocationOrder;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobStore;
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
import org.elasticsearch.index.IndexVersions;
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
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
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
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.stateless.action.TransportEnsureDocsSearchableAction;
import org.elasticsearch.xpack.stateless.action.TransportFetchShardCommitsInUseAction;
import org.elasticsearch.xpack.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider;
import org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageMonitor;
import org.elasticsearch.xpack.stateless.allocation.StatelessAllocationDecider;
import org.elasticsearch.xpack.stateless.allocation.StatelessBalancingWeightsFactory;
import org.elasticsearch.xpack.stateless.allocation.StatelessExistingShardsAllocator;
import org.elasticsearch.xpack.stateless.allocation.StatelessIndexSettingProvider;
import org.elasticsearch.xpack.stateless.allocation.StatelessShardRelocationOrder;
import org.elasticsearch.xpack.stateless.allocation.StatelessShardRoutingRoleStrategy;
import org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.AverageWriteLoadSampler;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadPublisher;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadSampler;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestMetricsService;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.NodeIngestionLoadTracker;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.TransportPublishNodeIngestLoadMetric;
import org.elasticsearch.xpack.stateless.autoscaling.memory.HeapMemoryUsagePublisher;
import org.elasticsearch.xpack.stateless.autoscaling.memory.IndexingOperationsMemoryRequirementsSampler;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetricsService;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MergeMemoryEstimateCollector;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MergeMemoryEstimatePublisher;
import org.elasticsearch.xpack.stateless.autoscaling.memory.ShardsMappingSizeCollector;
import org.elasticsearch.xpack.stateless.autoscaling.memory.TransportPublishHeapMemoryMetrics;
import org.elasticsearch.xpack.stateless.autoscaling.memory.TransportPublishIndexingOperationsHeapMemoryRequirements;
import org.elasticsearch.xpack.stateless.autoscaling.memory.TransportPublishMergeMemoryEstimate;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasScalerCacheBudget;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService;
import org.elasticsearch.xpack.stateless.autoscaling.search.SearchMetricsService;
import org.elasticsearch.xpack.stateless.autoscaling.search.SearchShardSizeCollector;
import org.elasticsearch.xpack.stateless.autoscaling.search.ShardSizeCollector;
import org.elasticsearch.xpack.stateless.autoscaling.search.ShardSizesPublisher;
import org.elasticsearch.xpack.stateless.autoscaling.search.TransportPublishShardSizes;
import org.elasticsearch.xpack.stateless.autoscaling.search.TransportUpdateReplicasAction;
import org.elasticsearch.xpack.stateless.autoscaling.search.load.AverageSearchLoadSampler;
import org.elasticsearch.xpack.stateless.autoscaling.search.load.SearchLoadProbe;
import org.elasticsearch.xpack.stateless.autoscaling.search.load.SearchLoadSampler;
import org.elasticsearch.xpack.stateless.autoscaling.search.load.TransportPublishSearchLoads;
import org.elasticsearch.xpack.stateless.cache.ClearBlobCacheRestHandler;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.action.ClearBlobCacheNodesResponse;
import org.elasticsearch.xpack.stateless.cache.action.TransportClearBlobCacheAction;
import org.elasticsearch.xpack.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterStateCleanupService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessElectionStrategy;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessHeartbeatStore;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessPersistedClusterStateService;
import org.elasticsearch.xpack.stateless.cluster.coordination.TransportConsistentClusterStateReadAction;
import org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor;
import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure;
import org.elasticsearch.xpack.stateless.commits.HollowIndexEngineDeletionPolicy;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.IndexEngineDeletionPolicy;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.HollowShardsMetrics;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshThrottler;
import org.elasticsearch.xpack.stateless.engine.RefreshThrottlingService;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogRecoveryMetrics;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.lucene.stats.GetAllShardSizesAction;
import org.elasticsearch.xpack.stateless.lucene.stats.GetShardSizeAction;
import org.elasticsearch.xpack.stateless.lucene.stats.ShardSizeStatsClient;
import org.elasticsearch.xpack.stateless.lucene.stats.ShardSizeStatsReaderImpl;
import org.elasticsearch.xpack.stateless.metering.GetBlobStoreStatsRestHandler;
import org.elasticsearch.xpack.stateless.metering.action.GetBlobStoreStatsNodesResponse;
import org.elasticsearch.xpack.stateless.metering.action.TransportGetBlobStoreStatsAction;
import org.elasticsearch.xpack.stateless.multiproject.ProjectLifeCycleService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.objectstore.gc.ObjectStoreGCTask;
import org.elasticsearch.xpack.stateless.objectstore.gc.ObjectStoreGCTaskExecutor;
import org.elasticsearch.xpack.stateless.recovery.PITRelocationService;
import org.elasticsearch.xpack.stateless.recovery.RecoveryCommitRegistrationHandler;
import org.elasticsearch.xpack.stateless.recovery.RemedialAllocationSettingService;
import org.elasticsearch.xpack.stateless.recovery.RemoveRefreshClusterBlockService;
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;
import org.elasticsearch.xpack.stateless.recovery.TransportSendRecoveryCommitRegistrationAction;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction;
import org.elasticsearch.xpack.stateless.recovery.metering.RecoveryMetricsCollector;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.SearchShardInformationIndexListener;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.SearchShardInformationMetricsCollector;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.elasticsearch.xpack.stateless.reshard.RestReshardSplitAction;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.SplitTargetService;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardSplitAction;
import org.elasticsearch.xpack.stateless.reshard.TransportUpdateSplitSourceShardStateAction;
import org.elasticsearch.xpack.stateless.reshard.TransportUpdateSplitTargetShardStateAction;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory;
import org.elasticsearch.xpack.stateless.xpack.DummyILMInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyILMUsageTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyMonitoringInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyMonitoringUsageTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyRollupInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyRollupUsageTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummySearchableSnapshotsInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummySearchableSnapshotsUsageTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyTransportGetRollupIndexCapsAction;
import org.elasticsearch.xpack.stateless.xpack.DummyVotingOnlyInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyVotingOnlyUsageTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyWatcherInfoTransportAction;
import org.elasticsearch.xpack.stateless.xpack.DummyWatcherUsageTransportAction;

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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;
import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;

public class StatelessPlugin extends Plugin
    implements
        EnginePlugin,
        ActionPlugin,
        ClusterPlugin,
        ClusterCoordinationPlugin,
        ExtensiblePlugin,
        HealthPlugin,
        PersistentTaskPlugin {

    private static final Logger logger = LogManager.getLogger(StatelessPlugin.class);

    public static final LicensedFeature.Persistent STATELESS_FEATURE = LicensedFeature.persistent(
        null,
        "stateless",
        License.OperationMode.ENTERPRISE
    );

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = boolSetting(
        DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME,
        true,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> FAILURE_STORE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME,
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    public static final String NAME = "stateless";

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
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL
        + "_thread_pool";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL = "stateless_fill_vbcc_cache";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        + "_thread_pool";
    public static final String PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_PREWARMING_THREAD_NAME;
    public static final String PREWARM_THREAD_POOL_SETTING = "stateless." + PREWARM_THREAD_POOL + "_thread_pool";
    public static final String UPLOAD_PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME;
    public static final String UPLOAD_PREWARM_THREAD_POOL_SETTING = "stateless." + UPLOAD_PREWARM_THREAD_POOL + "_thread_pool";

    /**
     * The set of {@link ShardRouting.Role}s that we expect to see in a stateless deployment
     */
    public static final Set<ShardRouting.Role> STATELESS_SHARD_ROLES = Set.of(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.SEARCH_ONLY);

    public static final ActionType<GetBlobStoreStatsNodesResponse> GET_BLOB_STORE_STATS_ACTION = new ActionType<>(
        "cluster:monitor/stateless/blob_store/stats/get"
    );

    public static final ActionType<ClearBlobCacheNodesResponse> CLEAR_BLOB_CACHE_ACTION = new ActionType<>(
        "cluster:admin/stateless/blob_cache/clear"
    );

    /** Temporary feature flag setting for creating indices with a refresh block. Defaults to false. **/
    public static final Setting<Boolean> USE_INDEX_REFRESH_BLOCK_SETTING = Setting.boolSetting(
        MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

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
        final int prewarmMaxThreads;
        final int uploadPrewarmCoreThreads;
        final int uploadPrewarmMaxThreads;

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
            prewarmMaxThreads = Math.min(processors * 2, 32);
            // These threads are used for prewarming the shared blob cache on upload, and are separate from the prewarm thread pool
            // in order to avoid any deadlocks between the two (e.g., when two fillgaps compete). Since they are used to prewarm on upload,
            // we use the same amount of max threads as the shard write pool.
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            uploadPrewarmMaxThreads = Math.min(processors * 4, 10);
            uploadPrewarmCoreThreads = uploadPrewarmMaxThreads / 2;
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
            prewarmMaxThreads = Math.min(processors * 4, 32);
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            fillVirtualBatchedCompoundCommitCacheCoreThreads = Math.max(processors / 2, 2);
            fillVirtualBatchedCompoundCommitCacheMaxThreads = Math.max(processors, 2);
            uploadPrewarmCoreThreads = 0;
            uploadPrewarmMaxThreads = 1;
        }

        return new ExecutorBuilder<?>[] {
            new ScalingExecutorBuilder(
                SHARD_READ_THREAD_POOL,
                4,
                shardReadMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_READ_THREAD_POOL_SETTING,
                EsExecutors.TaskTrackingConfig.builder().trackOngoingTasks().trackExecutionTime(0.3).build(),
                EsExecutors.HotThreadsOnLargeQueueConfig.DISABLED
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
    private final SetOnce<BCCHeaderReadExecutor> bccHeaderReadExecutor = new SetOnce<>();
    private final SetOnce<SearchCommitPrefetcherDynamicSettings> prefetchingDynamicSettings = new SetOnce<>();
    private final SetOnce<IngestLoadProbe> ingestLoadProbe = new SetOnce<>();
    private final SetOnce<DesiredTopologyContext> desiredTopologyContext = new SetOnce<>();
    private final SetOnce<SearchShardInformationIndexListener> searchShardInformationIndexListener = new SetOnce<>();
    private final SetOnce<PITRelocationService> pitRelocationService = new SetOnce<>();

    private final boolean sharedCachedSettingExplicitlySet;
    private final boolean sharedCacheMmapExplicitlySet;
    private final boolean useRealMemoryCircuitBreakerExplicitlySet;
    private final boolean pageCacheReyclerLimitExplicitlySet;

    private final boolean enabled;
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

    public StatelessCommitService getCommitService() {
        return Objects.requireNonNull(this.commitService.get());
    }

    public StatelessPlugin(Settings settings) {
        enabled = STATELESS_ENABLED.get(settings);
        if (enabled) {
            var nonStatelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (nonStatelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(NAME + " does not support node roles " + nonStatelessDataNodeRoles);
            }
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(STATELESS_ROLES::contains)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.size() > 1) {
                throw new IllegalArgumentException(NAME + " does not support a node with more than 1 role of " + statelessDataNodeRoles);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.exists(settings)) {
                if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings)) {
                    throw new IllegalArgumentException(
                        NAME + " does not support " + CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()
                    );
                }
            }
            if (DATA_STREAMS_LIFECYCLE_ONLY_MODE.exists(settings)) {
                if (DATA_STREAMS_LIFECYCLE_ONLY_MODE.get(settings) == false) {
                    throw new IllegalArgumentException(
                        NAME + " does not support setting " + DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey() + " to false"
                    );
                }
            }
            if (Objects.equals(SHARDS_ALLOCATOR_TYPE_SETTING.get(settings), DESIRED_BALANCE_ALLOCATOR) == false) {
                throw new IllegalArgumentException(
                    NAME + " can only be used with " + SHARDS_ALLOCATOR_TYPE_SETTING.getKey() + "=" + DESIRED_BALANCE_ALLOCATOR
                );
            }

            logger.info("[{}] is enabled", NAME);
        } else {
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r))
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(
                    NAME + " is not enabled, but stateless-only node roles are configured: " + statelessDataNodeRoles
                );
            }
        }
        hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);

        logSettings(settings);
        // It is dangerous to retain these settings because they will be further modified after this ctor due
        // to the call to #additionalSettings. We only parse out the components that has already been set.
        sharedCachedSettingExplicitlySet = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings);
        sharedCacheMmapExplicitlySet = SharedBlobCacheService.SHARED_CACHE_MMAP.exists(settings);
        useRealMemoryCircuitBreakerExplicitlySet = HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.exists(settings);
        pageCacheReyclerLimitExplicitlySet = PageCacheRecycler.LIMIT_HEAP_SETTING.exists(settings);
        hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        hasMasterRole = DiscoveryNode.isMasterNode(settings);
        projectType = PROJECT_TYPE.get(settings);
        this.statelessIndexSettingProvider = new StatelessIndexSettingProvider(projectType);
        hollowShardsEnabled = STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (enabled) {
            return List.of(statelessExecutorBuilders(settings, hasIndexRole));
        } else {
            return super.getExecutorBuilders(settings);
        }
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
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
            new ActionHandler(TransportUpdateSplitTargetShardStateAction.TYPE, TransportUpdateSplitTargetShardStateAction.class),
            new ActionHandler(TransportUpdateSplitSourceShardStateAction.TYPE, TransportUpdateSplitSourceShardStateAction.class),
            new ActionHandler(TransportReshardSplitAction.TYPE, TransportReshardSplitAction.class),
            new ActionHandler(TransportReshardAction.TYPE, TransportReshardAction.class),
            new ActionHandler(StatelessUnpromotableRelocationAction.TYPE, TransportStatelessUnpromotableRelocationAction.class),
            new ActionHandler(TransportFetchSearchShardInformationAction.TYPE, TransportFetchSearchShardInformationAction.class)
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
        return List.of(new ClearBlobCacheRestHandler(), new GetBlobStoreStatsRestHandler(), new RestReshardSplitAction(settings));
    }

    @Override
    public Settings additionalSettings() {
        if (enabled) {
            var settings = Settings.builder()
                .put(super.additionalSettings())
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                .put(DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), true)
                .put(FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30));
            settings.put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), StatelessElectionStrategy.NAME)
                .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0)
                .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), false);
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
            // Explicitly disable the recovery source, as it is not needed in stateless mode.
            settings.put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
            return settings.build();
        } else {
            return super.additionalSettings();
        }
    }

    void checkLicense() {
        if (enabled) {
            final var licenseState = getLicenseState();
            if (STATELESS_FEATURE.checkAndStartTracking(licenseState, NAME) == false) {
                throw new IllegalStateException(
                    NAME
                        + " cannot be enabled with a ["
                        + licenseState.getOperationMode()
                        + "] license. It is only allowed with an Enterprise license."
                );
            }
        }
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {
        checkLicense();
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
        var cacheWarmingService = createSharedBlobCacheWarmingService(
            cacheService,
            threadPool,
            services.telemetryProvider(),
            clusterService.getClusterSettings()
        );
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
        setAndGet(this.bccHeaderReadExecutor, new BCCHeaderReadExecutor(threadPool));

        var indexShardCacheWarmer = new IndexShardCacheWarmer(
            objectStoreService,
            cacheWarmingService,
            threadPool,
            commitService.useReplicatedRanges(),
            bccHeaderReadExecutor.get()
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
            ShardsMappingSizeCollector.create(
                hasIndexRole,
                clusterService,
                indicesService,
                heapMemoryUsagePublisher,
                threadPool,
                hollowShardsService
            )
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

        services.allocationService()
            .getClusterInfoService()
            .addListener(
                new EstimatedHeapUsageMonitor(clusterService.getClusterSettings(), clusterService::state, rerouteService)::onNewInfo
            );

        if (hasIndexRole) {
            var ingestLoadPublisher = new IngestLoadPublisher(client, threadPool);
            var writeLoadSampler = AverageWriteLoadSampler.create(threadPool, settings, clusterService.getClusterSettings());
            var ingestLoadProbe = new IngestLoadProbe(clusterService.getClusterSettings(), writeLoadSampler::getExecutorStats, threadPool);
            this.ingestLoadProbe.set(ingestLoadProbe);
            var ingestLoadSampler = new IngestLoadSampler(
                threadPool,
                writeLoadSampler,
                ingestLoadPublisher,
                ingestLoadProbe::getNodeIngestionLoad,
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
                var projectLifeCycleService = new ProjectLifeCycleService(clusterService, objectStoreService, threadPool, client);
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
            var searchLoadProbe = new SearchLoadProbe(
                clusterService.getClusterSettings(),
                averageSearchLoadSampler::getExecutorLoadStats,
                services.telemetryProvider().getMeterRegistry()
            );
            var searchLoadSampler = SearchLoadSampler.create(
                client,
                averageSearchLoadSampler,
                searchLoadProbe::getSearchLoad,
                searchLoadProbe::getSearchLoadQuality,
                clusterService,
                settings,
                threadPool,
                services.telemetryProvider().getMeterRegistry()
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
        var desiredTopologyContext = setAndGet(this.desiredTopologyContext, new DesiredTopologyContext(clusterService));
        components.add(desiredTopologyContext);
        desiredTopologyContext.init();

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
            createMetadataReshardIndexService(clusterService, shardRoutingRoleStrategy, rerouteService, indicesService)
        );
        components.add(reshardIndexService);
        var splitTargetService = setAndGet(
            this.splitTargetService,
            new SplitTargetService(settings, client, clusterService, reshardIndexService)
        );
        components.add(splitTargetService);
        var splitSourceService = setAndGet(
            this.splitSourceService,
            new SplitSourceService(
                client,
                clusterService,
                indicesService,
                commitService,
                objectStoreService,
                reshardIndexService,
                services.taskManager()
            )
        );
        components.add(splitSourceService);
        // PIT relocation
        var pitRelocationService = setAndGet(this.pitRelocationService, new PITRelocationService());
        components.add(pitRelocationService);

        if (hasSearchRole) {
            setAndGet(this.prefetchExecutor, new SearchCommitPrefetcher.PrefetchExecutor(threadPool));
            // it is imperative that we do not listen for dynamic settings updates within the prefetcher itself because the prefetcher is
            // created whenever we create the search engine and it might miss the settings that were updated before the node was started.
            // also note that we create one search engine per index so we could end up with many instances of settings listeners.
            setAndGet(this.prefetchingDynamicSettings, new SearchCommitPrefetcherDynamicSettings(clusterService.getClusterSettings()));
            SearchShardInformationMetricsCollector shardInformationMetricsCollector = new SearchShardInformationMetricsCollector(
                services.telemetryProvider()
            );
            setAndGet(
                this.searchShardInformationIndexListener,
                new SearchShardInformationIndexListener(
                    client,
                    shardInformationMetricsCollector,
                    clusterService.getClusterSettings(),
                    threadPool.relativeTimeInMillisSupplier()
                )
            );
        }

        if (hasMasterRole) {
            var remedialSettingService = new RemedialAllocationSettingService(clusterService);
            clusterService.addListener(remedialSettingService);
            var replicationUpdaterService = new ReplicasUpdaterService(
                threadPool,
                clusterService,
                (NodeClient) client,
                searchMetricsService,
                new ReplicasLoadBalancingScaler(clusterService, client),
                desiredTopologyContext,
                services.telemetryProvider().getMeterRegistry()
            );
            replicationUpdaterService.init();
            components.add(replicationUpdaterService);
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
        ClusterSettings clusterSettings
    ) {
        return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, clusterSettings);
    }

    protected ReshardIndexService createMetadataReshardIndexService(
        ClusterService clusterService,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy,
        RerouteService rerouteService,
        IndicesService indicesService
    ) {
        return new ReshardIndexService(clusterService, shardRoutingRoleStrategy, rerouteService, indicesService);
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(blobStoreHealthIndicator.get());
    }

    public MemoryMetricsService getMemoryMetricsService() {
        return memoryMetricsService.get();
    }

    public ClusterService getClusterService() {
        return clusterService.get();
    }

    public IndicesService getIndicesService() {
        return indicesService.get();
    }

    public BiFunction<ShardId, Long, BlobContainer> shardBlobContainerFunc() {
        final var objectStoreService = this.objectStoreService.get();
        return (shardId, primaryTerm) -> {
            final BlobStore blobStore = objectStoreService.getProjectBlobStore(shardId);
            final var shardBasePath = objectStoreService.shardBasePath(shardId);
            return blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm)));
        };
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
        super.close();
        if (enabled) {
            STATELESS_FEATURE.stopTracking(getLicenseState(), NAME);
        }

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
            DATA_STREAMS_LIFECYCLE_ONLY_MODE,
            FAILURE_STORE_REFRESH_INTERVAL_SETTING,
            ObjectStoreService.TYPE_SETTING,
            ObjectStoreService.BUCKET_SETTING,
            ObjectStoreService.CLIENT_SETTING,
            ObjectStoreService.BASE_PATH_SETTING,
            ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY,
            ObjectStoreService.OBJECT_STORE_SHUTDOWN_TIMEOUT,
            ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS,
            ObjectStoreService.CACHE_SEARCH_RECOVERY_BCC_ENABLED_SETTING,
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
            ShardsMappingSizeCollector.FIXED_HOLLOW_SHARD_MEMORY_OVERHEAD_SETTING,
            ShardsMappingSizeCollector.HOLLOW_SHARD_SEGMENT_MEMORY_OVERHEAD_SETTING,
            MergeMemoryEstimateCollector.MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO,
            MemoryMetricsService.STALE_METRICS_CHECK_DURATION_SETTING,
            MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
            MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
            MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
            MemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
            MemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
            MemoryMetricsService.ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING,
            MemoryMetricsService.SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING,
            IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING,
            IngestLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
            NodeIngestionLoadTracker.ACCURATE_LOAD_WINDOW,
            NodeIngestionLoadTracker.STALE_LOAD_WINDOW,
            IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING,
            IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW,
            IngestMetricsService.MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN,
            IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR,
            IngestLoadProbe.INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED,
            IngestLoadProbe.INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION,
            IngestLoadProbe.MAX_MANAGEABLE_QUEUED_WORK,
            IngestLoadProbe.INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE,
            IngestLoadProbe.WRITE_COORDINATION_MAX_CONTRIBUTION_CAP_RATIO,
            IngestLoadProbe.WRITE_COORDINATION_MAX_CONTRIBUTION_CAPPED_LOG_INTERVAL,
            IngestLoadProbe.WRITE_COORDINATION_UNCAPPING_THRESHOLD,
            AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING,
            AverageWriteLoadSampler.QUEUE_SIZE_SAMPLER_EWMA_ALPHA_SETTING,
            NodeIngestionLoadTracker.USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING,
            NodeIngestionLoadTracker.INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE,
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
            ReplicasUpdaterService.AUTO_EXPAND_REPLICA_INDICES,
            ReplicasScalerCacheBudget.REPLICA_CACHE_BUDGET_RATIO,
            ReplicasLoadBalancingScaler.MAX_REPLICA_RELATIVE_SEARCH_LOAD,
            SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
            SearchLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR,
            SearchLoadProbe.SHARD_READ_LOAD_THRESHOLD_SETTING,
            SearchLoadProbe.SEARCH_LOAD_SCALING_FACTOR,
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
            TransportStatelessPrimaryRelocationAction.SLOW_RELOCATION_THRESHOLD_SETTING,
            GetVirtualBatchedCompoundCommitChunksPressure.CHUNKS_BYTES_LIMIT,
            CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING,
            SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP,
            RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING,
            StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT,
            SearchEngine.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT,
            HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED,
            HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL,
            HollowShardsService.SETTING_HOLLOW_INGESTION_TTL,
            USE_INDEX_REFRESH_BLOCK_SETTING,
            RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING,
            SplitTargetService.RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT,
            SplitTargetService.RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT,
            IndexingOperationsMemoryRequirementsSampler.SAMPLE_VALIDITY_SETTING,
            StatelessBalancingWeightsFactory.SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING,
            StatelessBalancingWeightsFactory.INDEXING_TIER_BALANCING_THRESHOLD_SETTING,
            StatelessBalancingWeightsFactory.SEARCH_TIER_BALANCING_THRESHOLD_SETTING,
            TransportStatelessUnpromotableRelocationAction.START_HANDOFF_CLUSTER_STATE_CONVERGENCE_TIMEOUT_SETTING,
            TransportStatelessUnpromotableRelocationAction.START_HANDOFF_REQUEST_TIMEOUT_SETTING,
            StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED,
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            EstimatedHeapUsageAllocationDecider.MINIMUM_LOGGING_INTERVAL,
            EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT,
            SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING,
            SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING,
            SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING,
            SearchCommitPrefetcherDynamicSettings.PREFETCH_SEARCH_IDLE_TIME_SETTING,
            SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING,
            SearchCommitPrefetcher.FORCE_PREFETCH_SETTING,
            StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING,
            StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB,
            SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING,
            SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
            RestReshardSplitAction.RESHARD_ALLOWED,
            AutoCreateAction.AUTO_CREATE_INDEX_PRIORITY_SETTING,
            AutoCreateAction.AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING,
            MetadataCreateIndexService.CREATE_INDEX_PRIORITY_SETTING,
            MetadataCreateIndexService.CREATE_INDEX_MAX_TIMEOUT_SETTING,
            MetadataCreateIndexService.CLUSTER_MAX_INDICES_PER_PROJECT_SETTING,
            MetadataCreateIndexService.CLUSTER_MAX_INDICES_PER_PROJECT_ENABLED_SETTING,
            MetadataMappingService.PUT_MAPPING_PRIORITY_SETTING,
            MetadataMappingService.PUT_MAPPING_MAX_TIMEOUT_SETTING,
            ShardStateAction.SHARD_STARTED_REROUTE_SOME_UNASSIGNED_PRIORITY,
            ShardStateAction.SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY,
            ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING,
            ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING,
            ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_INTERVAL_SETTING,
            SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING,
            StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING,
            StatelessShardRelocationOrder.PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING
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
            indexModule.addIndexEventListener(ingestLoadProbe.get());
            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    statelessCommitService.register(
                        indexShard.shardId(),
                        indexShard.getOperationPrimaryTerm(),
                        () -> isInitializingNoSearchShards(indexShard),
                        () -> indexShard.mapperService().mappingLookup(),
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
            final var idxVersion = indexModule.indexSettings().getIndexVersionCreated();
            final var readSiFromMemoryIfPossible = idxVersion.onOrAfter(IndexVersions.READ_SI_FILES_FROM_MEMORY_FOR_HOLLOW_COMMITS);
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isPromotableToPrimary()) {
                    Lucene.cleanLuceneIndex(in);
                    var indexCacheDirectory = createIndexBlobStoreCacheDirectory(sharedBlobCacheService.get(), shardRouting.shardId());
                    return new IndexDirectory(
                        in,
                        indexCacheDirectory,
                        statelessCommitService::onGenerationalFileDeletion,
                        readSiFromMemoryIfPossible
                    );
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

            indexModule.addIndexEventListener(this.searchShardInformationIndexListener.get());
            indexModule.addIndexEventListener(this.pitRelocationService.get());

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
                projectResolver.get(),
                bccHeaderReadExecutor.get(),
                clusterService.get().getClusterSettings(),
                getStatelessSharedBlobCacheService()
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
        ReshardIndexService reshardIndexService,
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
            reshardIndexService,
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
                    (operation, seqNo, location) -> replicator.add(translogConfig.getShardId(), operation, seqNo, location),
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
                final Engine engine = newHollowOrIndexEngine(indexSettings, newConfig);

                IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(newConfig.getStore().directory());
                assert indexDirectory != null : "expected an IndexDirectory";
                indexDirectory.setLastCommittedSegmentInfosSupplier(engine::getLastCommittedSegmentInfos);

                return engine;
            } else {
                assert prefetchExecutor.get() != null : "Prefetch executor should be instantiated in search nodes";
                assert prefetchingDynamicSettings.get() != null : "Prefetching dynamic settings should be instantiated in search nodes";
                return new SearchEngine(
                    config,
                    getClosedShardService(),
                    sharedBlobCacheService.get(),
                    clusterService.get().getClusterSettings(),
                    prefetchExecutor.get(),
                    prefetchingDynamicSettings.get(),
                    bccHeaderReadExecutor.get()
                );
            }
        });
    }

    private Engine newHollowOrIndexEngine(IndexSettings indexSettings, EngineConfig newConfig) {

        SegmentInfos segmentCommitInfos = lastCommittedSegmentsInfo(newConfig);

        // When recovering a hollow commit, or when relocation hollows a hollowable shard, the shard is not yet marked hollow
        // in the HollowShardService, and we want to use (or reset the engine to) the HollowIndexEngine. When unhollowing
        // a hollow shard, the shard is marked hollow in the HollowShardService and we would like to reset the engine to
        // the IndexEngine in order to flush a new blob and complete unhollowing.
        // If the shard does not have an existing blob container, e.g., when restoring a snapshot, we need to unhollow by
        // using the IndexEngine that flushes and uploads a new commit.
        var shardId = newConfig.getShardId();
        boolean existingBlobContainer = commitService.get().getLatestUploadedBcc(shardId) != null;
        if (hollowShardsService.get().isFeatureEnabled()
            && hollowShardsService.get().isHollowShard(shardId) == false
            && IndexEngine.isLastCommitHollow(segmentCommitInfos)
            && existingBlobContainer) {
            logger.debug(() -> shardId + " using hollow engine for shard [generation: " + segmentCommitInfos.getGeneration() + "]");
            return new HollowIndexEngine(newConfig, getCommitService(), hollowShardsService.get(), segmentCommitInfos);
        }
        return newIndexEngine(
            newConfig,
            translogReplicator.get(),
            getObjectStoreService()::getTranslogBlobContainer,
            getCommitService(),
            hollowShardsService.get(),
            sharedBlobCacheWarmingService.get(),
            refreshThrottlingService.get().createRefreshThrottlerFactory(indexSettings),
            reshardIndexService.get(),
            documentParsingProvider.get(),
            new IndexEngine.EngineMetrics(translogReplicatorMetrics.get(), newConfig.getMergeMetrics(), hollowShardMetrics.get())
        );
    }

    private SegmentInfos lastCommittedSegmentsInfo(EngineConfig config) {
        try {
            return config.getStore().readLastCommittedSegmentsInfo();
        } catch (IOException e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to read last segments info", e);
        }
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
                    String startFile = userData.get(StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE);
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
        return List.of(
            new StatelessAllocationDecider(),
            new EstimatedHeapUsageAllocationDecider(clusterSettings),
            new StatelessThrottlingConcurrentRecoveriesAllocationDecider(clusterSettings)
        );
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Map.of(StatelessPlugin.NAME, new StatelessExistingShardsAllocator());
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

    @Override
    public ShardRelocationOrder getShardRelocationOrder(ClusterSettings clusterSettings) {
        return new StatelessShardRelocationOrder(clusterSettings);
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
