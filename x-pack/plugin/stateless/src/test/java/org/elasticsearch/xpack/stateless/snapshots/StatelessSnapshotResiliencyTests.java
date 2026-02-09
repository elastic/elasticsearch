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

package org.elasticsearch.xpack.stateless.snapshots;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettingsExtension;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.MockPluginsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.snapshots.SnapshotResiliencyTestHelper.TestClusterNodes;
import org.elasticsearch.snapshots.SnapshotResiliencyTestHelper.TestClusterNodes.TransportInterceptorFactory;
import org.elasticsearch.snapshots.SnapshotResiliencyTests;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.IndexShardCacheWarmer;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.allocation.StatelessAllocationDecider;
import org.elasticsearch.xpack.stateless.allocation.StatelessExistingShardsAllocator;
import org.elasticsearch.xpack.stateless.allocation.StatelessIndexSettingProvider;
import org.elasticsearch.xpack.stateless.allocation.StatelessShardRoutingRoleStrategy;
import org.elasticsearch.xpack.stateless.autoscaling.search.ShardSizeCollector;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessElectionStrategy;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessHeartbeatStore;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessPersistedClusterStateService;
import org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.HollowShardsMetrics;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshThrottler;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogRecoveryMetrics;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.PITRelocationService;
import org.elasticsearch.xpack.stateless.recovery.RecoveryCommitRegistrationHandler;
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;
import org.elasticsearch.xpack.stateless.recovery.TransportSendRecoveryCommitRegistrationAction;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.SplitTargetService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.ESAllocationTestCase.createShardsAllocator;
import static org.elasticsearch.cluster.ESAllocationTestCase.randomAllocationDeciders;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // TranslogReplicatorReader does not allow random extra files
public class StatelessSnapshotResiliencyTests extends SnapshotResiliencyTests {

    private static final TimeValue SHORT_TRANSLOG_FLUSH_INTERVAL = TimeValue.timeValueMillis(10);

    private static class StatelessDeterministicTaskQueue extends DeterministicTaskQueue {

        private static final Set<String> EXECUTOR_NAMES_TO_USE_DIRECT_EXECUTOR = Set.of(
            StatelessPlugin.CLUSTER_STATE_READ_WRITE_THREAD_POOL,
            StatelessPlugin.SHARD_READ_THREAD_POOL,
            StatelessPlugin.SHARD_WRITE_THREAD_POOL,
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL,
            StatelessPlugin.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
            ThreadPool.Names.REFRESH
        );

        @Override
        public ThreadPool getThreadPool(Function<Runnable, Runnable> runnableWrapper) {
            return new StatelessDeterministicThreadPool(runnableWrapper);
        }

        private class StatelessDeterministicThreadPool extends DeterministicThreadPool {

            protected StatelessDeterministicThreadPool(Function<Runnable, Runnable> runnableWrapper) {
                super(runnableWrapper);
            }

            @Override
            public ExecutorService executor(String name) {
                // There are places in stateless code where a thread blocks waiting for a future to complete, where the future
                // is submitted to a different executor. This does not work with the single thread execution model of
                // DeterministicTaskQueue. So instead of submitting the task to a different thread pool, we execute it directly
                // inline so that the future is ready when the waiting thread checks for its completion.
                if (EXECUTOR_NAMES_TO_USE_DIRECT_EXECUTOR.contains(name)) {
                    logger.debug("--> using direct executor for [{}] executor", name);
                    return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                } else {
                    return forkingExecutor;
                }
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                // Schedule a task immediately if the delay is within 10ms. A current known case for this is to schedule
                // translog upload right away so that indexing operations, which wait for translog upload, can complete
                // with `runAllRunnableTasks` without advancing time.
                final var actualDelay = delay.compareTo(SHORT_TRANSLOG_FLUSH_INTERVAL) <= 0 ? TimeValue.ZERO : delay;
                return super.schedule(command, actualDelay, executor);
            }
        }
    }

    @Override
    protected DeterministicTaskQueue createDeterministicTaskQueue() {
        return new StatelessDeterministicTaskQueue();
    }

    @Override
    protected void disconnectOrRestartMasterNode() {
        // Disconnect current master does not lead to master failover in stateless, so we always restart
        // The restart will wait for the master to failover. Otherwise, it could reclaim the master and leave other nodes masterless
        testClusterNodes.randomMasterNode().ifPresent(masterNode -> {
            masterNode.restart(() -> {
                final var currentMasterNodeIds = testClusterNodes.nodes()
                    .values()
                    .stream()
                    .map(n -> n.clusterService().state().nodes().getMasterNodeId())
                    .collect(Collectors.toSet());
                return currentMasterNodeIds.contains(null) == false
                    && currentMasterNodeIds.size() == 1
                    && currentMasterNodeIds.iterator().next().equals(masterNode.node().getName()) == false;
            });
        });
    }

    @Override
    protected void setupTestCluster(int masterNodes, int dataNodes, TransportInterceptorFactory transportInterceptorFactory) {
        testClusterNodes = new StatelessNodes(
            masterNodes,
            dataNodes,
            tempDir,
            deterministicTaskQueue,
            transportInterceptorFactory,
            this::assertCriticalWarnings
        );
        startCluster();
    }

    @Override
    protected VotingConfiguration createVotingConfiguration() {
        return VotingConfiguration.of(
            testClusterNodes.nodes()
                .values()
                .stream()
                .map(TestClusterNodes.TestClusterNode::node)
                .filter(DiscoveryNode::isMasterNode)
                .findFirst()
                .orElseThrow()
        );
    }

    @Override
    protected Settings defaultIndexSettings(int shards) {
        return Settings.builder()
            .put(super.defaultIndexSettings(shards))
            .put("index.number_of_replicas", 1) // always need a replica for search
            .put("index.refresh_interval", -1) // disable refreshes to avoid background operations during tests
            .build();
    }

    static class StatelessNodes extends TestClusterNodes {

        StatelessNodes(
            int masterNodes,
            int dataNodes,
            Path tempDir,
            DeterministicTaskQueue deterministicTaskQueue,
            TransportInterceptorFactory transportInterceptorFactory,
            Consumer<String[]> warningConsumer
        ) {
            super(masterNodes, dataNodes, tempDir, deterministicTaskQueue, transportInterceptorFactory, warningConsumer);
            if (dataNodes > 0) {
                // Always start one search node if there is any index node
                nodes.computeIfAbsent("search-node-0", nodeName -> {
                    try {
                        return newNode(nodeName, DiscoveryNodeRole.SEARCH_ROLE, transportInterceptorFactory);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
        }

        @Override
        protected DiscoveryNodeRole dataNodeRole() {
            return DiscoveryNodeRole.INDEX_ROLE;
        }

        @Override
        protected StatelessNode newNode(String nodeName, DiscoveryNodeRole role, TransportInterceptorFactory transportInterceptorFactory)
            throws IOException {
            final StatelessNode statelessNode = newNode(
                DiscoveryNodeUtils.builder(ESTestCase.randomAlphaOfLength(10)).name(nodeName).roles(Collections.singleton(role)).build(),
                transportInterceptorFactory
            );
            statelessNode.init();
            return statelessNode;
        }

        @Override
        protected StatelessNode newNode(DiscoveryNode node, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return new StatelessNode(node, transportInterceptorFactory);
        }

        @Override
        protected Settings nodeSettings(DiscoveryNode node) {
            final Settings.Builder builder = Settings.builder();
            builder.put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), "1s")
                .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), SHORT_TRANSLOG_FLUSH_INTERVAL)
                .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false)
                .put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), StatelessElectionStrategy.NAME)
                .put(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
                .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
                .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
                .put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket")
                .put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "object_store")
                .put("node.roles", node.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining(",")));

            if (node.canContainData()) {
                // cache only uses up to 0.1% disk to be friendly with default region size
                builder.put(SHARED_CACHE_SIZE_SETTING.getKey(), new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString());
            }
            return builder.build();
        }

        @Override
        protected Set<Setting<?>> clusterSettings() {
            final var res = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            res.addAll(new ServerlessSharedSettingsExtension().getSettings());
            res.add(StatelessIndexSettingProvider.DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING);
            res.add(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING);
            res.add(SearchCommitPrefetcherDynamicSettings.PREFETCH_SEARCH_IDLE_TIME_SETTING);
            res.add(SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING);
            res.add(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING);
            res.add(SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING);
            res.add(SearchCommitPrefetcher.FORCE_PREFETCH_SETTING);
            res.add(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP);
            res.add(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING);
            res.add(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING);
            res.add(TransportStatelessPrimaryRelocationAction.SLOW_RELOCATION_THRESHOLD_SETTING);
            res.add(SearchEngine.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT);
            res.add(StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING);
            return Set.copyOf(res);
        }

        @Override
        protected PluginsService createPluginsService(Settings settings, Environment environment) {
            return new MockPluginsService(settings, environment, List.of(TestStatelessPlugin.class));
        }

        class StatelessNode extends TestClusterNodes.TestClusterNode {

            private TestStatelessPlugin testStatelessPlugin;

            StatelessNode(DiscoveryNode node, TransportInterceptorFactory transportInterceptorFactory) {
                super(node, transportInterceptorFactory);
            }

            @Override
            protected void doInit(Map<ActionType<?>, TransportAction<?, ?>> actions, ActionFilters actionFilters) {
                // Make sure bootstrapping removes global cluster block
                clusterService().addListener(
                    new GatewayService(settings, rerouteService, clusterService(), new StatelessShardRoutingRoleStrategy(), threadPool)
                );
                final var pluginServices = mock(Plugin.PluginServices.class);
                when(pluginServices.client()).thenReturn(client);
                when(pluginServices.nodeEnvironment()).thenReturn(nodeEnv);
                when(pluginServices.repositoriesService()).thenReturn(repositoriesService);
                when(pluginServices.threadPool()).thenReturn(threadPool);
                when(pluginServices.clusterService()).thenReturn(clusterService());
                when(pluginServices.indicesService()).thenReturn(indicesService);
                when(pluginServices.projectResolver()).thenReturn(projectResolver);
                testStatelessPlugin = pluginsService.filterPlugins(TestStatelessPlugin.class).findFirst().orElseThrow();
                testStatelessPlugin.createComponents(pluginServices);
                actions.putAll(getActions(actionFilters));
            }

            @Override
            protected SnapshotShardContextFactory createSnapshotShardContextFactory() {
                final var plugin = mock(StatelessPlugin.class);
                when(plugin.getClusterService()).thenReturn(clusterService());
                when(plugin.getIndicesService()).thenReturn(indicesService);
                when(plugin.getCommitService()).thenReturn(testStatelessPlugin.statelessCommitService);
                when(plugin.shardBlobContainerFunc()).thenReturn((shardId, primaryTerm) -> {
                    final BlobStore blobStore = testStatelessPlugin.objectStoreService.getProjectBlobStore(shardId);
                    final var shardBasePath = testStatelessPlugin.objectStoreService.shardBasePath(shardId);
                    return blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm)));
                });
                return new StatelessSnapshotShardContextFactory(plugin);
            }

            @Override
            protected boolean maybeExecuteTransportRunnable(Runnable runnable) {
                // Inline execution for search node fetching data from index node
                if (runnable.toString().contains("[" + TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]]")) {
                    runnable.run();
                    return true;
                }
                return false;
            }

            @Override
            public void start(ClusterState initialState) {
                super.start(initialState);
                testStatelessPlugin.objectStoreService.start();
                testStatelessPlugin.statelessCommitService.start();
                testStatelessPlugin.translogReplicator.start();
                testStatelessPlugin.consistencyService.start();
            }

            @Override
            public void stop() {
                testStatelessPlugin.consistencyService.stop();
                testStatelessPlugin.translogReplicator.stop();
                testStatelessPlugin.statelessCommitService.stop();
                testStatelessPlugin.storeHeartbeatService.stop();
                testStatelessPlugin.objectStoreService.stop();
                testStatelessPlugin.cacheService.close();
                super.stop();
            }

            @Override
            protected Set<IndexSettingProvider> getIndexSettingProviders() {
                final var statelessIndexSettingProvider = new StatelessIndexSettingProvider(PROJECT_TYPE.get(settings));
                statelessIndexSettingProvider.initialize(clusterService(), indexNameExpressionResolver);
                return Set.of(statelessIndexSettingProvider);
            }

            private Map<ActionType<?>, TransportAction<?, ?>> getActions(ActionFilters actionFilters) {
                return Map.of(
                    TransportNewCommitNotificationAction.TYPE,
                    new TransportNewCommitNotificationAction(
                        clusterService(),
                        transportService(),
                        shardStateAction,
                        actionFilters,
                        indicesService,
                        mock(ShardSizeCollector.class)
                    ),
                    TransportSendRecoveryCommitRegistrationAction.TYPE,
                    new TransportSendRecoveryCommitRegistrationAction(clusterService(), transportService(), indicesService, actionFilters),
                    TransportRegisterCommitForRecoveryAction.TYPE,
                    new TransportRegisterCommitForRecoveryAction(transportService(), indicesService, clusterService(), actionFilters),
                    StatelessPrimaryRelocationAction.TYPE,
                    new TransportStatelessPrimaryRelocationAction(
                        transportService(),
                        clusterService(),
                        actionFilters,
                        indicesService,
                        peerRecoveryTargetService,
                        testStatelessPlugin.statelessCommitService,
                        mock(IndexShardCacheWarmer.class),
                        testStatelessPlugin.hollowShardsService,
                        HollowShardsMetrics.NOOP
                    ),
                    StatelessUnpromotableRelocationAction.TYPE,
                    new TransportStatelessUnpromotableRelocationAction(
                        transportService(),
                        clusterService(),
                        actionFilters,
                        indicesService,
                        peerRecoveryTargetService,
                        projectResolver,
                        searchService,
                        new PITRelocationService()
                    ),
                    TransportShardRefreshAction.TYPE,
                    new TransportShardRefreshAction(
                        settings,
                        transportService(),
                        clusterService(),
                        indicesService,
                        threadPool,
                        shardStateAction,
                        actionFilters,
                        projectResolver
                    ),
                    TransportGetVirtualBatchedCompoundCommitChunkAction.TYPE,
                    new TransportGetVirtualBatchedCompoundCommitChunkAction(
                        actionFilters,
                        bigArrays,
                        transportService(),
                        indicesService,
                        clusterService(),
                        mock(GetVirtualBatchedCompoundCommitChunksPressure.class),
                        testStatelessPlugin.statelessCommitService,
                        projectResolver
                    )
                );
            }

            @Override
            protected AllocationService createAllocationService(Settings settings, SnapshotsInfoService snapshotsInfoService) {
                final AllocationService allocationService = new AllocationService(
                    randomAllocationDeciders(settings, createBuiltInClusterSettings(settings), List.of(new ClusterPlugin() {
                        @Override
                        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                            return List.of(new StatelessAllocationDecider());
                        }
                    })),
                    createShardsAllocator(settings),
                    EmptyClusterInfoService.INSTANCE,
                    snapshotsInfoService,
                    new StatelessShardRoutingRoleStrategy()
                );
                allocationService.setExistingShardsAllocators(Map.of(StatelessPlugin.NAME, new StatelessExistingShardsAllocator()));
                return allocationService;
            }

            @Override
            protected Supplier<CoordinationState.PersistedState> getPersistedStateSupplier(ClusterState initialState, DiscoveryNode node) {
                final var persistedClusterStateService = new StatelessPersistedClusterStateService(
                    nodeEnv,
                    namedXContentRegistry,
                    clusterService().getClusterSettings(),
                    threadPool::relativeTimeInMillis,
                    () -> testStatelessPlugin.statelessElectionStrategy,
                    () -> testStatelessPlugin.objectStoreService,
                    threadPool,
                    CompatibilityVersions.EMPTY,
                    () -> false
                );

                final CoordinationState.PersistedState persistedState;
                try {
                    persistedState = persistedClusterStateService.createPersistedState(settings, node);
                } catch (IOException e) {
                    assert false;
                    throw new UncheckedIOException(e);
                }
                return () -> persistedState;
            }

            @Override
            protected ElectionStrategy createElectionStrategy() {
                return testStatelessPlugin.statelessElectionStrategy;
            }

            @Override
            protected LeaderHeartbeatService getLeaderHeartbeatService() {
                return testStatelessPlugin.storeHeartbeatService;
            }

            @Override
            protected Reconfigurator createReconfigurator() {
                return new SingleNodeReconfigurator(clusterService().getSettings(), clusterService().getClusterSettings());
            }

            @Override
            protected PreVoteCollector.Factory createPrevoteCollector() {
                return (
                    transportService,
                    startElection,
                    updateMaxTermSeen,
                    electionStrategy,
                    nodeHealthService,
                    leaderHeartbeatService) -> new AtomicRegisterPreVoteCollector(
                        (StoreHeartbeatService) leaderHeartbeatService,
                        startElection
                    );
            }
        }
    }

    public static class TestStatelessPlugin extends Plugin implements EnginePlugin {

        private static final Logger logger = LogManager.getLogger(TestStatelessPlugin.class);

        private final Settings settings;
        private final boolean hasIndexRole;
        private final boolean hasMasterRole;
        private final boolean hasSearchRole;
        private ProjectResolver projectResolver;
        private ThreadPool threadPool;
        private Executor bccHeaderReadExecutor;
        private Client client;
        private ClusterService clusterService;
        private ObjectStoreService objectStoreService;
        private StatelessSharedBlobCacheService cacheService;
        private CacheBlobReaderService cacheBlobReaderService;
        private SharedBlobCacheWarmingService cacheWarmingService;
        private StatelessElectionStrategy statelessElectionStrategy;
        private StoreHeartbeatService storeHeartbeatService;
        private StatelessClusterConsistencyService consistencyService;
        private StatelessCommitService statelessCommitService;
        private ClosedShardService closedShardService;
        private TranslogReplicator translogReplicator;
        private HollowShardsService hollowShardsService;
        private ReshardIndexService reshardIndexService;

        public TestStatelessPlugin(Settings settings) {
            this.settings = settings;
            this.hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
            this.hasMasterRole = DiscoveryNode.isMasterNode(settings);
            this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            this.projectResolver = services.projectResolver();
            this.threadPool = services.threadPool();
            this.bccHeaderReadExecutor = new BCCHeaderReadExecutor(threadPool);
            this.client = services.client();
            this.clusterService = services.clusterService();

            this.objectStoreService = new ObjectStoreService(
                settings,
                services.repositoriesService(),
                threadPool,
                clusterService,
                projectResolver
            );
            this.cacheService = new StatelessSharedBlobCacheService(
                services.nodeEnvironment(),
                settings,
                threadPool,
                new BlobCacheMetrics(MeterRegistry.NOOP)
            );

            this.cacheBlobReaderService = new CacheBlobReaderService(settings, cacheService, client, threadPool);
            this.cacheWarmingService = new SharedBlobCacheWarmingService(
                cacheService,
                threadPool,
                TelemetryProvider.NOOP,
                clusterService.getClusterSettings()
            ) {
                @Override
                public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
                    listener.onResponse(null);
                }

                @Override
                protected void warmCacheMerge(
                    String mergeId,
                    ShardId shardId,
                    Store store,
                    List<SegmentCommitInfo> segmentsToMerge,
                    Function<String, BlobLocation> blobLocationResolver,
                    BooleanSupplier mergeCancelled,
                    ActionListener<Void> listener
                ) {
                    listener.onResponse(null);
                }

                @Override
                protected void warmCacheRecovery(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory,
                    @Nullable Map<BlobFile, Integer> regionsToWarm,
                    ActionListener<Void> listener
                ) {
                    listener.onResponse(null);
                }
            };

            this.statelessElectionStrategy = new StatelessElectionStrategy(objectStoreService::getClusterStateBlobContainer, threadPool) {
                @Override
                protected Executor getExecutor() {
                    return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                }
            };
            this.storeHeartbeatService = StoreHeartbeatService.create(
                new StatelessHeartbeatStore(objectStoreService::getClusterStateHeartbeatContainer, threadPool),
                threadPool,
                settings,
                statelessElectionStrategy::getCurrentLeaseTerm
            );
            this.consistencyService = new StatelessClusterConsistencyService(
                clusterService,
                statelessElectionStrategy,
                threadPool,
                settings
            );
            this.statelessCommitService = new StatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                services.indicesService(),
                client,
                new StatelessCommitCleaner(consistencyService, threadPool, objectStoreService),
                cacheService,
                cacheWarmingService,
                TelemetryProvider.NOOP
            );
            clusterService.addListener(statelessCommitService);
            this.closedShardService = new ClosedShardService();
            this.translogReplicator = new TranslogReplicator(threadPool, settings, objectStoreService, consistencyService);

            hollowShardsService = mock(HollowShardsService.class);
            // Let hollowShardsService pass on mutable operation check
            doAnswer(invocation -> {
                ActionListener<Void> listener = invocation.getArgument(2);
                listener.onResponse(null);
                return null;
            }).when(hollowShardsService).onMutableOperation(any(), anyBoolean(), anyActionListener());
            when(hollowShardsService.isHollowShard(any(ShardId.class))).thenReturn(false);
            when(hollowShardsService.isHollowableIndexShard(any(IndexShard.class), anyBoolean())).thenReturn(false);

            // Let reshardIndexService pass on refresh check
            reshardIndexService = mock(ReshardIndexService.class);
            doAnswer(invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(reshardIndexService).maybeAwaitSplit(any(ShardId.class), anyActionListener());

            return List.of();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (hasIndexRole) {
                indexModule.addIndexEventListener(new IndexEventListener() {

                    @Override
                    public void afterIndexShardCreated(IndexShard indexShard) {
                        statelessCommitService.register(indexShard.shardId(), indexShard.getOperationPrimaryTerm(), () -> {
                            final var shardRouting = indexShard.routingEntry();
                            return shardRouting.initializing() && shardRouting.recoverySource().getType() != RecoverySource.Type.PEER;
                        }, () -> indexShard.mapperService().mappingLookup(), indexShard::addGlobalCheckpointListener, () -> {
                            Engine engineOrNull = indexShard.getEngineOrNull();
                            if (engineOrNull instanceof IndexEngine engine) {
                                engine.syncTranslogReplicator(ActionListener.noop());
                            } else if (engineOrNull == null) {
                                throw new AlreadyClosedException("engine is closed");
                            } else {
                                throw new AssertionError("Engine is " + engineOrNull);
                            }
                        });
                        translogReplicator.register(indexShard.shardId(), indexShard.getOperationPrimaryTerm(), seqNo -> {
                            var engine = indexShard.getEngineOrNull();
                            if (engine instanceof IndexEngine indexEngine) {
                                indexEngine.objectStorePersistedSeqNoConsumer().accept(seqNo);
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
                        statelessCommitService.registerCommitNotificationSuccessListener(indexShard.shardId(), (gen) -> {
                            indexShard.withEngineOrNull(engine -> {
                                if (engine instanceof IndexEngine e) {
                                    e.commitSuccess(gen);
                                }
                                return null;
                            });
                        });
                    }

                    @Override
                    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                        if (indexShard != null) {
                            statelessCommitService.unregisterCommitNotificationSuccessListener(shardId);
                            statelessCommitService.closeShard(shardId);
                        }
                    }

                    @Override
                    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                        statelessCommitService.delete(shardId);
                    }

                    @Override
                    public void onStoreClosed(ShardId shardId) {
                        statelessCommitService.unregister(shardId);
                        translogReplicator.unregister(shardId);
                    }
                });
                indexModule.setIndexCommitListener(createIndexCommitListener());
                indexModule.setDirectoryWrapper((in, shardRouting) -> {
                    if (shardRouting.isPromotableToPrimary()) {
                        Lucene.cleanLuceneIndex(in);
                        var indexCacheDirectory = new IndexBlobStoreCacheDirectory(cacheService, shardRouting.shardId());
                        return new IndexDirectory(in, indexCacheDirectory, statelessCommitService::onGenerationalFileDeletion, true);
                    } else {
                        return in;
                    }
                });
            }

            if (hasSearchRole) {
                indexModule.addIndexEventListener(new IndexEventListener() {
                    @Override
                    public void onStoreClosed(ShardId shardId) {
                        closedShardService.onStoreClose(shardId);
                    }
                });
                indexModule.setDirectoryWrapper((in, shardRouting) -> {
                    if (shardRouting.isSearchable()) {
                        in.close();
                        return new SearchDirectory(
                            cacheService,
                            cacheBlobReaderService,
                            new AtomicMutableObjectStoreUploadTracker(),
                            shardRouting.shardId()
                        );
                    } else {
                        return in;
                    }
                });
            }

            indexModule.addIndexEventListener(
                TestUtils.newStatelessIndexEventListener(
                    threadPool,
                    statelessCommitService,
                    objectStoreService,
                    translogReplicator,
                    new RecoveryCommitRegistrationHandler(client, clusterService),
                    cacheWarmingService,
                    hollowShardsService,
                    mock(SplitTargetService.class),
                    mock(SplitSourceService.class),
                    projectResolver,
                    bccHeaderReadExecutor,
                    clusterService.getClusterSettings(),
                    cacheService
                )
            );
        }

        private Engine.IndexCommitListener createIndexCommitListener() {
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
                            assert releaseFile == null : "release file is set to " + releaseFile + " but start file is not set";
                            translogRecoveryStartFile = translogReplicator.getMaxUploadedFile() + 1;
                            translogReleaseEndFile = -1;
                        } else {
                            translogRecoveryStartFile = Long.parseLong(startFile);
                            if (releaseFile == null) {
                                translogReleaseEndFile = translogRecoveryStartFile;
                            } else {
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

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                TranslogConfig translogConfig = config.getTranslogConfig();
                translogReplicator.setBigArrays(translogConfig.getBigArrays());
                if (config.isPromotableToPrimary()) {
                    TranslogConfig newTranslogConfig = new TranslogConfig(
                        translogConfig.getShardId(),
                        translogConfig.getTranslogPath(),
                        translogConfig.getIndexSettings(),
                        translogConfig.getBigArrays(),
                        translogConfig.getBufferSize(),
                        translogConfig.getDiskIoBufferPool(),
                        (operation, seqNo, location) -> translogReplicator.add(translogConfig.getShardId(), operation, seqNo, location),
                        false // translog is replicated to the object store, no need fsync that
                    );

                    EngineConfig newConfig = new EngineConfig(
                        config.getShardId(),
                        config.getThreadPool(),
                        config.getThreadPoolMergeExecutorService(),
                        config.getIndexSettings(),
                        config.getWarmer(),
                        config.getStore(),
                        config.getMergePolicy(),
                        config.getAnalyzer(),
                        config.getSimilarity(),
                        config.getCodecProvider(),
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
                        config.isPromotableToPrimary(),
                        config.getMapperService(),
                        config.getEngineResetLock(),
                        config.getMergeMetrics(),
                        policy -> policy
                    );

                    return new IndexEngine(
                        newConfig,
                        translogReplicator,
                        objectStoreService::getTranslogBlobContainer,
                        statelessCommitService,
                        mock(HollowShardsService.class),
                        cacheWarmingService,
                        RefreshThrottler.Noop::new,
                        reshardIndexService,
                        statelessCommitService.getCommitBCCResolverForShard(newConfig.getShardId()),
                        DocumentParsingProvider.EMPTY_INSTANCE,
                        new IndexEngine.EngineMetrics(TranslogRecoveryMetrics.NOOP, MergeMetrics.NOOP, HollowShardsMetrics.NOOP),
                        shardId -> true,
                        statelessCommitService.getShardLocalCommitsTracker(newConfig.getShardId()).shardLocalReadersTracker()
                    );
                } else {
                    return new SearchEngine(
                        config,
                        closedShardService,
                        cacheService,
                        clusterService.getClusterSettings(),
                        mock(SearchCommitPrefetcher.PrefetchExecutor.class), // prefetch is disabled
                        new SearchCommitPrefetcherDynamicSettings(clusterService.getClusterSettings()),
                        bccHeaderReadExecutor
                    );
                }
            });
        }
    }
}
