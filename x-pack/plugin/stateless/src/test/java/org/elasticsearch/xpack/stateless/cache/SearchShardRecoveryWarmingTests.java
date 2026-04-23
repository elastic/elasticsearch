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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
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
                SharedBlobCacheWarmingService.PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING,
                SharedBlobCacheWarmingService.ID_LOOKUP_PREWARM_RATIO_SETTING
            )
        ).collect(Collectors.toSet());
    }

    private static ClusterSettings newClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, warmingServiceSettings());
    }

    private static SharedBlobCacheWarmingService newWarmingService(ThreadPool threadPool) {
        ClusterSettings clusterSettings = newClusterSettings();
        return new SharedBlobCacheWarmingService(
            Mockito.mock(StatelessSharedBlobCacheService.class),
            threadPool,
            TelemetryProvider.NOOP,
            clusterSettings,
            new DefaultWarmingRatioProviderFactory().create(clusterSettings)
        );
    }

    /**
     * {@link SharedBlobCacheWarmingService} with {@link SharedBlobCacheWarmingService#warmCache} stubbed to complete immediately after
     * {@code validateWarmCacheListener} runs (typically Hamcrest assertions on the listener passed from recovery warming).
     */
    private static SharedBlobCacheWarmingService newWarmingServiceWithWarmCacheListenerCheck(
        ThreadPool threadPool,
        Consumer<ActionListener<Void>> validateWarmCacheListener
    ) {
        ClusterSettings clusterSettings = newClusterSettings();
        return new SharedBlobCacheWarmingService(
            Mockito.mock(StatelessSharedBlobCacheService.class),
            threadPool,
            TelemetryProvider.NOOP,
            clusterSettings,
            new DefaultWarmingRatioProviderFactory().create(clusterSettings)
        ) {
            @Override
            protected void warmCache(
                SharedBlobCacheWarmingService.Type type,
                IndexShard indexShard,
                StatelessCompoundCommit commit,
                BlobStoreCacheDirectory directory,
                @Nullable Map<BlobFile, Long> endOffsetsToWarm,
                boolean preWarmForIdLookup,
                ActionListener<Void> listener
            ) {
                validateWarmCacheListener.accept(listener);
                listener.onResponse(null);
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

    /** Returns {@code clusterState} with non-empty {@link Metadata#nodeShutdowns()} (arbitrary node id). */
    private static ClusterState withNonEmptyNodeShutdownMetadata(ClusterState clusterState) {
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
     * Same routing as {@link #testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy} (another active search copy), but with
     * cluster shutdown metadata present: non-relocation path should not await warming.
     */
    public void testSearchRecoveryNonRelocationSkipsWhenShutdownMetadataPresent() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingService(threadPool);
            ClusterState base = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ClusterState state = withNonEmptyNodeShutdownMetadata(base);
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

    public void testWarmCacheForSearchShardRecoveryNullEndOffsetsUsesNoopWarmListener() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingServiceWithWarmCacheListenerCheck(
                threadPool,
                l -> assertThat(l, sameInstance(ActionListener.<Void>noop()))
            );
            ClusterState state = clusterStateOneSearchReplica("idx", STARTED);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(state, mockIndexShard(self), null, null, null, resume);
            assertTrue(resume.isDone());
        }
    }

    /**
     * Same routing layout as {@link #testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy}: {@link ShardRoutingState#INITIALIZING}
     * self search replica with a started search peer; warming uses the race listener when {@code endOffsetsToWarm} is set.
     */
    public void testWarmCacheForSearchShardRecoveryWithReplica() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingServiceWithWarmCacheListenerCheck(
                threadPool,
                l -> assertThat(l, not(sameInstance(ActionListener.<Void>noop())))
            );
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            PlainActionFuture<Void> resumeFuture = new PlainActionFuture<>() {
                @Override
                public void onResponse(Void result) {
                    ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                    super.onResponse(result);
                }
            };
            service.warmCacheForSearchShardRecovery(
                state,
                mockIndexShard(self),
                null,
                null,
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), 1L),
                resumeFuture
            );
            safeGet(resumeFuture);
        }
    }

    /**
     * Same shard layout as {@link #testSearchRecoverySkipsWhenOnlyPrimaryActive}:
     * {@link SharedBlobCacheWarmingService#searchRecoveryTimeout} skips;
     * {@code warmCacheForSearchShardRecovery} uses a noop warm listener even when {@code endOffsetsToWarm} is set.
     */
    public void testWarmCacheForSearchShardRecoveryNoOtherActive() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newWarmingServiceWithWarmCacheListenerCheck(
                threadPool,
                l -> assertThat(l, sameInstance(ActionListener.<Void>noop()))
            );
            ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(
                state,
                mockIndexShard(self),
                null,
                null,
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), 1L),
                resume
            );
            assertTrue(resume.isDone());
        }
    }
}
