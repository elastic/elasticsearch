/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FakeTimeThreadPool;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.SearchRecoveryWaitOutcome;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SharedBlobCacheWarmingService#searchRecoveryTimeout} and
 * {@link SharedBlobCacheWarmingService#warmCacheForSearchShardRecovery}.
 */
public class SearchShardRecoveryWarmingTests extends ESTestCase {

    private static Set<Setting<?>> warmingServiceSettings() {
        return Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP,
                SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
                SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
                DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING,
                SharedBlobCacheWarmingService.UPLOAD_PREWARM_MAX_SIZE_SETTING,
                SharedBlobCacheWarmingService.WARM_BYTE_RANGE_THROTTLE_RATIO_SETTING,
                SharedBlobCacheWarmingService.WARM_BYTE_RANGE_PER_FILE_CONCURRENCY_SETTING,
                SharedBlobCacheWarmingService.PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING,
                SharedBlobCacheWarmingService.ID_LOOKUP_PREWARM_RATIO_SETTING
            )
        ).collect(Collectors.toSet());
    }

    private static ClusterSettings newClusterSettings(Settings settings) {
        return new ClusterSettings(settings, warmingServiceSettings());
    }

    private static SharedBlobCacheWarmingService newWarmingService(ThreadPool threadPool) {
        return newWarmingService(threadPool, TelemetryProvider.NOOP);
    }

    private static SharedBlobCacheWarmingService newWarmingService(ThreadPool threadPool, TelemetryProvider telemetryProvider) {
        return newWarmingService(threadPool, telemetryProvider, 0L, ignored -> {});
    }

    private static TelemetryProvider telemetryProvider(MeterRegistry meterRegistry) {
        return new TelemetryProvider() {
            @Override
            public Tracer getTracer() {
                return Tracer.NOOP;
            }

            @Override
            public MeterRegistry getMeterRegistry() {
                return meterRegistry;
            }

            @Override
            public HttpServerInstrumentation getHttpServerInstrumentation() {
                return HttpServerInstrumentation.NOOP;
            }

            @Override
            public void attemptFlush() {}
        };
    }

    /**
     * {@link SharedBlobCacheWarmingService} with {@link SharedBlobCacheWarmingService#warmCache} stubbed to sleep for
     * {@code delayMillis} before completing the listener, so that recorded warming-duration metrics are observably non-zero.
     */
    private static SharedBlobCacheWarmingService newWarmingService(
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        long delayMillis,
        Consumer<Future<?>> warmFutureConsumer
    ) {
        return newWarmingService(threadPool, telemetryProvider, Settings.EMPTY, delayMillis, warmFutureConsumer);
    }

    private static SharedBlobCacheWarmingService newWarmingService(
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        Settings settings,
        long delayMillis,
        Consumer<Future<?>> warmFutureConsumer
    ) {
        ClusterSettings clusterSettings = newClusterSettings(settings);
        return new SharedBlobCacheWarmingService(
            Mockito.mock(StatelessSharedBlobCacheService.class),
            threadPool,
            telemetryProvider,
            clusterSettings,
            new DefaultWarmingRatioProviderFactory().create(clusterSettings)
        ) {
            @Override
            protected void warmCache(
                SharedBlobCacheWarmingService.Type type,
                IndexShard indexShard,
                StatelessCompoundCommit commit,
                BlobStoreCacheDirectory directory,
                @Nullable Map<BlobFile, WarmTarget> endTargetsToWarm,
                boolean preWarmForIdLookup,
                ActionListener<Void> listener
            ) {
                warmFutureConsumer.accept(threadPool.generic().submit(() -> {
                    if (delayMillis > 0) {
                        try {
                            Thread.sleep(delayMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    listener.onResponse(null);
                }));
            }
        };
    }

    private static IndexShard mockIndexShard(ShardRouting self) {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.routingEntry()).thenReturn(self);
        when(indexShard.shardId()).thenReturn(self.shardId());
        return indexShard;
    }

    /** One primary-replica pair: {@link ShardRouting.Role#INDEX_ONLY} primary, {@link ShardRouting.Role#SEARCH_ONLY} replica. */
    private static ClusterState clusterStateOneSearchReplica(String indexName, ShardRoutingState replicaState) {
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            indexName,
            true,
            STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(new Tuple<>(replicaState, ShardRouting.Role.SEARCH_ONLY))
        );
    }

    /**
     * {@link ShardRouting.Role#INDEX_ONLY} primary and two {@link ShardRouting.Role#SEARCH_ONLY} replicas: an active
     * peer and an {@link ShardRoutingState#INITIALIZING} copy (the shard under recovery). The index
     * primary is not searchable, so the peer supplies the other active search copy.
     */
    private static ClusterState clusterStateInitializingSearchReplicaWithActivePeer(String indexName) {
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            indexName,
            true,
            STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(
                new Tuple<>(randomFrom(STARTED, RELOCATING), ShardRouting.Role.SEARCH_ONLY),
                new Tuple<>(INITIALIZING, ShardRouting.Role.SEARCH_ONLY)
            )
        );
    }

    /**
     * The {@link ShardRoutingState#INITIALIZING} search replica (second replica) from
     * {@link #clusterStateInitializingSearchReplicaWithActivePeer}.
     */
    private static ShardRouting initializingSearchReplica(ClusterState state, ShardId shardId) {
        var replicas = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards();
        assert replicas.size() == 2 : replicas;
        assert replicas.get(1).initializing();
        return replicas.get(1);
    }

    /** Returns {@code clusterState} with non-empty {@link Metadata#nodeShutdowns()} for a node that is NOT in the cluster (stale). */
    private static ClusterState withStaleNodeShutdownMetadata(ClusterState clusterState) {
        SingleNodeShutdownMetadata.Type type = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId("shutdown-test-node")
            .setType(type)
            .setReason("SearchShardRecoveryWarmingTests")
            .setStartedAtMillis(1L)
            .setNodeSeen(false)
            .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueSeconds(30) : null)
            .build();
        NodesShutdownMetadata shutdowns = new NodesShutdownMetadata(Map.of(shutdown.getNodeId(), shutdown));
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdowns).build())
            .build();
    }

    /**
     * Returns {@code clusterState} with non-empty {@link Metadata#nodeShutdowns()} for a node that IS currently in the cluster,
     * optionally excluding {@code excludeNodeId} from consideration (e.g. to avoid the relocation-source branch).
     */
    private static ClusterState withActiveShutdownNodeMetadata(ClusterState clusterState, @Nullable String excludeNodeId) {
        String targetNodeId = clusterState.nodes()
            .getNodes()
            .keySet()
            .stream()
            .filter(id -> excludeNodeId == null || id.equals(excludeNodeId) == false)
            .findFirst()
            .orElseThrow();
        SingleNodeShutdownMetadata.Type type = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(targetNodeId)
            .setType(type)
            .setReason("SearchShardRecoveryWarmingTests")
            .setStartedAtMillis(1L)
            .setNodeSeen(true)
            .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueSeconds(30) : null)
            .build();
        NodesShutdownMetadata shutdowns = new NodesShutdownMetadata(Map.of(shutdown.getNodeId(), shutdown));
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdowns).build())
            .build();
    }

    /**
     * {@link SharedBlobCacheWarmingService#searchRecoveryTimeout} applies to non-promotable search replicas only.
     * Index-only primary and a single {@link ShardRoutingState#INITIALIZING} {@link ShardRouting.Role#SEARCH_ONLY}
     * replica: no other active search copy to wait on.
     */
    public void testSearchRecoverySkipsWhenOnlyPrimaryActive() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting shardRouting = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(shardRouting));
            assertThat(plan.awaitWarming(), is(false));
            assertThat(plan.timeout(), equalTo(TimeValue.ZERO));
        }
    }

    /**
     * Non-relocation recovery of an {@link ShardRoutingState#INITIALIZING} {@link ShardRouting.Role#SEARCH_ONLY} replica while a started
     * search peer exists ({@link ShardRouting.Role#INDEX_ONLY} primary).
     */
    public void testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            if (randomBoolean()) {
                state = withStaleNodeShutdownMetadata(state);
                assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            }
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self));
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }

    /**
     * Same routing as {@link #testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy}, but a node that is still in the cluster is
     * shutting down: non-relocation path must not await warming to avoid potentially delaying the shutdown.
     */
    public void testSearchRecoveryNonRelocationSkipsWhenActiveShutdownNodePresent() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState base = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ClusterState state = withActiveShutdownNodeMetadata(base, null);
            assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self));
            assertThat(plan.awaitWarming(), is(false));
            assertThat(plan.timeout(), equalTo(TimeValue.ZERO));
        }
    }

    /**
     * Verify that we use the right timeout when it is a relocation.
     */
    public void testSearchRecoveryRelocationUsesRelocationTimeout() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState state = ClusterStateCreationUtils.state(
                DEFAULT_PROJECT_ID,
                "test",
                true,
                STARTED,
                ShardRouting.Role.INDEX_ONLY,
                List.of(new Tuple<>(STARTED, ShardRouting.Role.SEARCH_ONLY), new Tuple<>(RELOCATING, ShardRouting.Role.SEARCH_ONLY))
            );
            if (randomBoolean()) {
                state = withStaleNodeShutdownMetadata(state);
                assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            }
            ShardId shardId = new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            var shardTable = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId);
            ShardRouting relocatingSearchReplica = shardTable.shardsWithState(RELOCATING)
                .stream()
                .filter(s -> s.primary() == false)
                .findFirst()
                .orElseThrow();
            assertEquals(ShardRouting.Role.SEARCH_ONLY, relocatingSearchReplica.role());
            ShardRouting self = relocatingSearchReplica.getTargetRelocatingShard();
            assertTrue(self.initializing());
            assertNotNull(self.relocatingNodeId());
            assertEquals(ShardRouting.Role.SEARCH_ONLY, self.role());
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self));
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }

    /**
     * Same relocation routing as {@link #testSearchRecoveryRelocationUsesRelocationTimeout}, but a node other than the relocation source
     * is actively shutting down: must use the shorter with-shutdown relocation timeout.
     */
    public void testSearchRecoveryRelocationUsesShutdownTimeoutWhenAnotherClusterNodeShuttingDown() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState base = ClusterStateCreationUtils.state(
                DEFAULT_PROJECT_ID,
                "test",
                true,
                STARTED,
                ShardRouting.Role.INDEX_ONLY,
                List.of(new Tuple<>(STARTED, ShardRouting.Role.SEARCH_ONLY), new Tuple<>(RELOCATING, ShardRouting.Role.SEARCH_ONLY))
            );
            ShardId shardId = new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = base.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(shardId)
                .shardsWithState(RELOCATING)
                .stream()
                .filter(s -> s.primary() == false)
                .findFirst()
                .orElseThrow()
                .getTargetRelocatingShard();
            // exclude the relocation source so we test the "another node shutting down" branch, not the source-removal branch
            ClusterState state = withActiveShutdownNodeMetadata(base, self.relocatingNodeId());
            assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self));
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING.getDefault(
                        Settings.EMPTY
                    )
                )
            );
        }
    }

    /**
     * When the relocation source is shutting down, the per-target warming timeout scales linearly with the number of search shards
     * concurrently relocating from that source to the same target. Two targets in the same cluster state — one receiving 3 such
     * relocations, the other receiving 1 — must yield timeouts in a 3:1 ratio because all other inputs (remaining grace, shards on
     * source, share factor) are identical between the two calls.
     */
    public void testSearchRecoveryTimeoutScalesByConcurrentRelocationsToTarget() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            threadPool.setCurrentTimeInMillis(0L);
            var service = newWarmingService(threadPool);

            final Index index = new Index("idx", randomUUID());
            final String primaryNodeId = "primary-node";
            final String sourceNodeId = "source-node";
            final String targetT1 = "target-t1";
            final String targetT2 = "target-t2";
            final String masterNodeId = "master-node";

            final int totalSearchShards = 4;
            final int relocationsToTarget1 = 3;

            // Each search shard is modeled as its own ShardId with an INDEX_ONLY primary; ShardIds must be distinct because every
            // search replica is currently on sourceNodeId, and IndexShardRoutingTable forbids two routings of the same ShardId on
            // the same node. The primary is placed on a separate primaryNodeId for the same reason (it must not collide with the
            // relocating replica's current or target node).
            final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
                .settings(indexSettings(IndexVersion.current(), index.getUUID(), totalSearchShards, 1))
                .build();

            final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(index);
            for (int s = 0; s < totalSearchShards; s++) {
                final ShardId sid = new ShardId(index, s);
                final ShardRouting primary = TestShardRouting.shardRoutingBuilder(sid, primaryNodeId, true, STARTED)
                    .withRole(ShardRouting.Role.INDEX_ONLY)
                    .build();
                final String relocationTarget = s < relocationsToTarget1 ? targetT1 : targetT2;
                final ShardRouting relocating = TestShardRouting.shardRoutingBuilder(sid, sourceNodeId, false, RELOCATING)
                    .withRelocatingNodeId(relocationTarget)
                    .withRole(ShardRouting.Role.SEARCH_ONLY)
                    .build();
                routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(sid).addShard(primary).addShard(relocating));
            }

            long shutdownStartedMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedMillis);
            final SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
                .setNodeId(sourceNodeId)
                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                .setReason(getTestName())
                .setStartedAtMillis(threadPool.absoluteTimeInMillis())
                .setNodeSeen(true)
                .build();

            final ClusterState state = ClusterState.builder(new ClusterName("test"))
                .nodes(
                    DiscoveryNodes.builder()
                        .add(DiscoveryNodeUtils.create(masterNodeId))
                        .masterNodeId(masterNodeId)
                        .localNodeId(masterNodeId)
                        .add(DiscoveryNodeUtils.create(primaryNodeId))
                        .add(DiscoveryNodeUtils.create(sourceNodeId))
                        .add(DiscoveryNodeUtils.create(targetT1))
                        .add(DiscoveryNodeUtils.create(targetT2))
                        .build()
                )
                .metadata(
                    Metadata.builder()
                        .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(sourceNodeId, shutdown)))
                        .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                        .build()
                )
                .routingTable(
                    GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build()
                )
                .build();

            assertThat(state.getRoutingNodes().node(sourceNodeId).size(), equalTo(totalSearchShards));
            assertThat(state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceNodeId), is(true));

            final ShardRouting selfT1 = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(0, relocationsToTarget1 - 1)))
                .shardsWithState(RELOCATING)
                .get(0)
                .getTargetRelocatingShard();
            final ShardRouting selfT2 = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(relocationsToTarget1, totalSearchShards - 1)))
                .shardsWithState(RELOCATING)
                .get(0)
                .getTargetRelocatingShard();
            assertThat(selfT1.currentNodeId(), equalTo(targetT1));
            assertThat(selfT2.currentNodeId(), equalTo(targetT2));

            // advance time
            threadPool.setCurrentTimeInMillis(shutdownStartedMillis + randomLongBetween(1, 100_000));
            SharedBlobCacheWarmingService.SearchRecoveryTimeout planT1 = service.searchRecoveryTimeout(state, mockIndexShard(selfT1));
            SharedBlobCacheWarmingService.SearchRecoveryTimeout planT2 = service.searchRecoveryTimeout(state, mockIndexShard(selfT2));

            assertThat(planT1.awaitWarming(), is(true));
            assertThat(planT2.awaitWarming(), is(true));

            assertThat(planT1.timeout().millis(), greaterThan(0L));
            assertThat(planT2.timeout().millis(), greaterThan(0L));
            // Out of a total of 4 search shards, there are 3 shards concurrently relocating to the target1 node and only one relocating to
            // the target2 node. So, we should allow approx 3x more time for recovery for the shards recovering to target1 than target2.
            assertThat(Math.round(((double) planT1.timeout().millis()) / planT2.timeout().millis()), is(3L));
        }
    }

    @SuppressForbidden(reason = "Future#cancel()")
    public void testWarmCacheForSearchShardRecoveryNullEndOffsetsUsesResumesRecoveryBeforeWarmingCompletes() throws Exception {
        AtomicReference<Future<?>> warmFutureReference = new AtomicReference<>();
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            try {
                var service = newWarmingService(
                    threadPool,
                    TelemetryProvider.NOOP,
                    TimeValue.timeValueHours(1).millis(), // delays warming completion
                    warmFutureReference::set
                );
                ClusterState state = clusterStateOneSearchReplica("idx", STARTED);
                ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
                ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
                service.warmCacheForSearchShardRecovery(state, mockIndexShard(self), null, null, null, resume);
                // recovery is resumed
                assertTrue(resume.isDone());
                // AND warming still runs (sleeps)
                assertBusy(() -> {
                    assertNotNull(warmFutureReference.get());
                    assertFalse(warmFutureReference.get().isDone());
                    assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1));
                });
            } finally {
                var warmFuture = warmFutureReference.get();
                if (warmFuture != null) {
                    warmFuture.cancel(true);
                }
            }
        }
    }

    /**
     * Same routing layout as {@link #testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy}: {@link ShardRoutingState#INITIALIZING}
     * self search replica with a started search peer; warming uses the race listener when {@code endOffsetsToWarm} is set.
     */
    @SuppressForbidden(reason = "Future#cancel()")
    public void testWarmCacheForSearchShardRecoveryWithReplica() throws Exception {
        AtomicReference<Future<?>> warmFutureReference = new AtomicReference<>();
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            PlainActionFuture<Void> resume = new PlainActionFuture<>() {
                @Override
                public void onResponse(Void result) {
                    ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                    super.onResponse(result);
                }
            };
            try {
                var service = newWarmingService(
                    threadPool,
                    TelemetryProvider.NOOP,
                    TimeValue.timeValueHours(1).millis(),
                    warmFutureReference::set
                );
                ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
                ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
                ShardRouting self = initializingSearchReplica(state, shardId);
                service.warmCacheForSearchShardRecovery(
                    state,
                    mockIndexShard(self),
                    null,
                    null,
                    Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), WarmTarget.withUnknownTimestamp(1L)),
                    resume
                );
                // recovery is NOT resumed
                assertFalse(resume.isDone());
                // AND warming still runs (sleeps)
                assertBusy(() -> {
                    assertNotNull(warmFutureReference.get());
                    assertFalse(warmFutureReference.get().isDone());
                    assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1));
                });
            } finally {
                var warmFuture = warmFutureReference.get();
                if (warmFuture != null) {
                    warmFuture.cancel(true);
                }
            }
        }
    }

    /**
     * Same shard layout as {@link #testSearchRecoverySkipsWhenOnlyPrimaryActive}:
     * {@link SharedBlobCacheWarmingService#searchRecoveryTimeout} skips, so {@code warmCacheForSearchShardRecovery} resumes recovery
     * synchronously (fire-and-forget warming) even when {@code endOffsetsToWarm} is set.
     */
    @SuppressForbidden(reason = "Future#cancel()")
    public void testWarmCacheForSearchShardRecoveryNoOtherActive() throws Exception {
        AtomicReference<Future<?>> warmFutureReference = new AtomicReference<>();
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            try {
                var service = newWarmingService(
                    threadPool,
                    TelemetryProvider.NOOP,
                    TimeValue.timeValueHours(1).millis(),
                    warmFutureReference::set
                );
                ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
                ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
                ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
                service.warmCacheForSearchShardRecovery(
                    state,
                    mockIndexShard(self),
                    null,
                    null,
                    Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), WarmTarget.withUnknownTimestamp(1L)),
                    resume
                );
                // recovery resumed (synchronously)
                assertTrue(resume.isDone());
                // AND warming still runs (sleeps)
                assertBusy(() -> {
                    assertNotNull(warmFutureReference.get());
                    assertFalse(warmFutureReference.get().isDone());
                    assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1));
                });
            } finally {
                var warmFuture = warmFutureReference.get();
                if (warmFuture != null) {
                    warmFuture.cancel(true);
                }
            }
        }
    }

    /**
     * Fire-and-forget path (no active peer to wait on): the warm-duration metric must reflect the delay injected before {@code
     * warmCache}'s stub completes its listener, but the wait-duration metric must record exactly 0 since recovery never actually waits
     * in this path (see {@link SearchRecoveryWaitOutcome#NO_WAIT}).
     */
    public void testWarmCacheForSearchShardRecoveryRecordsMetricsFireAndForget() throws Exception {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            long delayMillis = randomLongBetween(20, 100);
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), delayMillis, ignored -> {});
            ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(state, mockIndexShard(self), null, null, null, resume);
            // recovery resumed (synchronously)
            assertTrue(resume.isDone());
            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
            assertThat(measurements, hasSize(1));
            Measurement measurement = measurements.get(0);
            assertThat(measurement.getDouble(), equalTo(0.0));
            assertThat(
                measurement.attributes(),
                equalTo(
                    Map.of(
                        SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_OUTCOME_ATTRIBUTE_KEY,
                        SearchRecoveryWaitOutcome.NO_WAIT.name()
                    )
                )
            );
            assertBusy(() -> {
                assertSingleDurationMeasurementAtLeast(
                    meterRegistry,
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
                    delayMillis
                );
            });
        }
    }

    /**
     * Await-warming path (another active search copy exists): both metrics must reflect the delay injected before {@code warmCache}
     * completes its listener, since {@code resumeRecoveryListener} is invoked via the race listener only once warming completes
     * (the default 5-minute await timeout is nowhere close to firing first). The wait outcome must be {@code WARMING_COMPLETE} since
     * warming (not the timeout) is what completed the race.
     */
    public void testWarmCacheForSearchShardRecoveryRecordsMetricsWhenAwaitingWarming() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            long delayMillis = randomLongBetween(20, 100);
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), delayMillis, ignored -> {});
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            PlainActionFuture<Void> resumeFuture = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(
                state,
                mockIndexShard(self),
                null,
                null,
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), new WarmTarget(1L, 1L)),
                resumeFuture
            );
            safeGet(resumeFuture);
            assertSingleDurationMeasurementAtLeast(
                meterRegistry,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
                delayMillis
            );
            Measurement wait = assertSingleDurationMeasurementAtLeast(
                meterRegistry,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC,
                delayMillis
            );
            assertWaitOutcome(wait, SearchRecoveryWaitOutcome.WARMING_COMPLETE);
        }
    }

    /**
     * Configures {@link SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING} (the timeout that applies
     * to this test's non-relocation, another-active-copy routing) so short that it always fires before the listener passed in
     * (simulating warming) is ever completed: the wait outcome must be {@code TIMEOUT}.
     */
    public void testSearchRecoveryWarmingListenerRecordsTimedOutOutcome() throws Exception {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            long delayMillis = randomLongBetween(20, 100);
            long waitMillis = randomLongBetween(1, 10);
            Settings settings = Settings.builder()
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING.getKey(),
                    TimeValue.timeValueMillis(waitMillis)
                )
                .build();
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), settings, delayMillis, ignored -> {});
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            PlainActionFuture<Void> resumeFuture = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(
                state,
                mockIndexShard(self),
                null,
                null,
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), new WarmTarget(1L, 1L)),
                resumeFuture
            );
            safeGet(resumeFuture);
            // the warm-duration metric is only recorded once the background warmCache stub's full delayMillis sleep completes, which
            // happens well after the (much shorter) timeout resumes recovery, so poll for it rather than asserting immediately.
            assertBusy(() -> {
                assertSingleDurationMeasurementAtLeast(
                    meterRegistry,
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
                    delayMillis
                );
                Measurement wait = assertSingleDurationMeasurementAtLeast(
                    meterRegistry,
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC,
                    waitMillis
                );
                assertWaitOutcome(wait, SearchRecoveryWaitOutcome.TIMEOUT);
            });
        }
    }

    /**
     * Asserts that exactly one measurement was recorded for {@code metricName} and that its value (in seconds) is at least
     * {@code minMillis} (the artificial delay every caller injects via {@link #newWarmingService}) and, generously,
     * under a minute (catches gross unit/overflow errors without being sensitive to CI slowness). Returns the measurement so callers
     * can additionally assert on its attributes.
     */
    private static Measurement assertSingleDurationMeasurementAtLeast(
        RecordingMeterRegistry meterRegistry,
        String metricName,
        long minMillis
    ) {
        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, metricName);
        assertThat(measurements, hasSize(1));
        Measurement measurement = measurements.get(0);
        assertThat(measurement.getDouble(), greaterThanOrEqualTo(minMillis / 1000.0));
        assertThat(measurement.getDouble(), lessThan(TimeValue.timeValueMinutes(1).millis() / 1000.0));
        return measurement;
    }

    private static void assertWaitOutcome(Measurement waitDurationMeasurement, SearchRecoveryWaitOutcome outcome) {
        assertThat(
            waitDurationMeasurement.attributes(),
            equalTo(Map.of(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_OUTCOME_ATTRIBUTE_KEY, outcome.name()))
        );
    }
}
