/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
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
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FakeTimeThreadPool;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.reshard.SplitTargetService;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateInitializingSearchReplicaWithActivePeer;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateOneSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.initializingSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.mockIndexShard;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.totalBytesToWarm;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.when;

public class SearchRecoveryTimeoutCalculationServiceTests extends ESTestCase {

    private static SearchRecoveryTimeoutCalculationService newCalculationService(ThreadPool threadPool) {
        return newCalculationService(threadPool, Settings.EMPTY, 0L);
    }

    /// @param cacheSize what the (otherwise unused) shared blob cache reports as its size; only the data volume heuristic reads it, and the
    /// real cache service needs a live shared cache file to report one.
    private static SearchRecoveryTimeoutCalculationService newCalculationService(ThreadPool threadPool, Settings settings, long cacheSize) {
        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING,
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING
            )
        );
        final var cacheService = Mockito.mock(StatelessSharedBlobCacheService.class);
        when(cacheService.getCacheSize()).thenReturn(cacheSize);
        return new SearchRecoveryTimeoutCalculationService(cacheService, threadPool, clusterSettings);
    }

    /// Returns `clusterState` with non-empty [Metadata#nodeShutdowns()] for a node that is NOT in the cluster (stale).
    private static ClusterState withStaleNodeShutdownMetadata(ClusterState clusterState) {
        SingleNodeShutdownMetadata.Type type = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId("shutdown-test-node")
            .setType(type)
            .setReason("SearchRecoveryTimeoutCalculationServiceTests")
            .setStartedAtMillis(1L)
            .setNodeSeen(false)
            .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueSeconds(30) : null)
            .build();
        NodesShutdownMetadata shutdowns = new NodesShutdownMetadata(Map.of(shutdown.getNodeId(), shutdown));
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdowns).build())
            .build();
    }

    /// Returns `clusterState` with non-empty [Metadata#nodeShutdowns()] for a node that IS currently in the cluster,
    /// optionally excluding `excludeNodeId` from consideration (e.g. to avoid the relocation-source branch).
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
            .setReason("SearchRecoveryTimeoutCalculationServiceTests")
            .setStartedAtMillis(1L)
            .setNodeSeen(true)
            .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueSeconds(30) : null)
            .build();
        NodesShutdownMetadata shutdowns = new NodesShutdownMetadata(Map.of(shutdown.getNodeId(), shutdown));
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdowns).build())
            .build();
    }

    /// Builds a cluster state with `numShards` SEARCH\_ONLY replicas on `sourceNodeId`,
    /// with the first `numShardsToTarget` relocating to `targetNodeId` and the remainder
    /// relocating to `"other-node"`. The source is marked for REMOVE shutdown starting at `startedAtMillis`; the effective grace period
    /// is controlled via [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING].
    private static ClusterState clusterStateSearchShardsRelocatingFromShuttingDownSource(
        int numShards,
        int numShardsToTarget,
        Index index,
        String sourceNodeId,
        String targetNodeId,
        long startedAtMillis
    ) {
        assert numShardsToTarget <= numShards;
        final String primaryNodeId = "primary-node";
        final String masterNodeId = "master-node";
        final String otherNodeId = "other-node";
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), numShards, 1))
            .build();
        final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(index);
        for (int s = 0; s < numShards; s++) {
            final ShardId sid = new ShardId(index, s);
            final ShardRouting primary = TestShardRouting.shardRoutingBuilder(sid, primaryNodeId, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final String dest = s < numShardsToTarget ? targetNodeId : otherNodeId;
            final ShardRouting relocating = TestShardRouting.shardRoutingBuilder(sid, sourceNodeId, false, RELOCATING)
                .withRelocatingNodeId(dest)
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(sid).addShard(primary).addShard(relocating));
        }
        final SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(sourceNodeId)
            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
            .setReason("SearchRecoveryTimeoutCalculationServiceTests")
            .setStartedAtMillis(startedAtMillis)
            .setNodeSeen(true)
            .build();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(masterNodeId))
            .masterNodeId(masterNodeId)
            .localNodeId(masterNodeId)
            .add(DiscoveryNodeUtils.create(primaryNodeId))
            .add(DiscoveryNodeUtils.create(sourceNodeId))
            .add(DiscoveryNodeUtils.create(targetNodeId));
        if (numShardsToTarget < numShards) {
            nodes.add(DiscoveryNodeUtils.create(otherNodeId));
        }
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes.build())
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(sourceNodeId, shutdown)))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build())
            .build();
    }

    /// [SearchRecoveryTimeoutCalculationService#searchRecoveryTimeout] applies to non-promotable search replicas only.
    /// Index-only primary and a single [org.elasticsearch.cluster.routing.ShardRoutingState#INITIALIZING] [ShardRouting.Role#SEARCH_ONLY]
    /// replica: no other active search copy to wait on.
    public void testSearchRecoverySkipsWhenOnlyPrimaryActive() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
            ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting shardRouting = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().getFirst();
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(shardRouting), 0L, null);
            assertThat(plan.awaitWarming(), is(false));
            assertThat(plan.timeout(), equalTo(TimeValue.ZERO));
        }
    }

    /// Non-relocation recovery of an [org.elasticsearch.cluster.routing.ShardRoutingState#INITIALIZING] [ShardRouting.Role#SEARCH_ONLY]
    /// replica while a started search peer exists ([ShardRouting.Role#INDEX_ONLY] primary).
    public void testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            if (randomBoolean()) {
                state = withStaleNodeShutdownMetadata(state);
                assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            }
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }

    /// Same routing as [#testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy], but a node that is still in the cluster is
    /// shutting down: non-relocation path must not await warming to avoid potentially delaying the shutdown.
    public void testSearchRecoveryNonRelocationSkipsWhenActiveShutdownNodePresent() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
            ClusterState base = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ClusterState state = withActiveShutdownNodeMetadata(base, null);
            assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            IndexShard indexShard = mockIndexShard(self);
            var plan = service.searchRecoveryTimeout(state, indexShard, 0L, null);
            assertThat(plan.awaitWarming(), is(false));
            assertThat(plan.timeout(), equalTo(TimeValue.ZERO));
        }
    }

    /// Verify that we use the right timeout when it is a relocation.
    public void testSearchRecoveryRelocationUsesRelocationTimeout() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
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
            IndexShard indexShard = mockIndexShard(self);
            var plan = service.searchRecoveryTimeout(state, indexShard, 0L, null);
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }

    /// Same relocation routing as [#testSearchRecoveryRelocationUsesRelocationTimeout], but a node other than the relocation source
    /// is actively shutting down: must use the shorter with-shutdown relocation timeout.
    public void testSearchRecoveryRelocationUsesShutdownTimeoutWhenAnotherClusterNodeShuttingDown() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
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
            IndexShard indexShard = mockIndexShard(self);
            var plan = service.searchRecoveryTimeout(state, indexShard, 0L, null);
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
     * A relocation starts with no shutdown anywhere in the cluster, so the initial plan is extendable and uses the relocation timeout.
     * Then a node other than the relocation source starts shutting down: the re-evaluation must switch to the shorter with-shutdown
     * relocation timeout rather than keep extending by the regular relocation slice.
     */
    public void testSearchRecoveryReevaluationSwitchesToShutdownTimeoutWhenAnotherClusterNodeStartsShuttingDown() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            final var service = newCalculationService(threadPool);
            final ClusterState stateWithoutShutdown = ClusterStateCreationUtils.state(
                DEFAULT_PROJECT_ID,
                "test",
                true,
                STARTED,
                ShardRouting.Role.INDEX_ONLY,
                List.of(new Tuple<>(STARTED, ShardRouting.Role.SEARCH_ONLY), new Tuple<>(RELOCATING, ShardRouting.Role.SEARCH_ONLY))
            );
            final ShardId shardId = new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            final ShardRouting self = stateWithoutShutdown.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(shardId)
                .shardsWithState(RELOCATING)
                .stream()
                .filter(s -> s.primary() == false)
                .findFirst()
                .orElseThrow()
                .getTargetRelocatingShard();

            IndexShard indexShard1 = mockIndexShard(self);
            final var initialPlan = service.searchRecoveryTimeout(stateWithoutShutdown, indexShard1, 0L, null);
            assertThat(initialPlan.awaitWarming(), is(true));
            assertThat(
                initialPlan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getDefault(Settings.EMPTY))
            );
            assertThat(initialPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN));
            assertThat(initialPlan.extendable(), is(true));

            // exclude the relocation source so we test the "another node shutting down" branch, not the source-removal branch
            final ClusterState stateWithOtherNodeShutdown = withActiveShutdownNodeMetadata(stateWithoutShutdown, self.relocatingNodeId());
            assertThat(stateWithOtherNodeShutdown.metadata().nodeShutdowns().isNodeMarkedForRemoval(self.relocatingNodeId()), is(false));

            IndexShard indexShard = mockIndexShard(self);
            final var reevaluatedPlan = service.searchRecoveryTimeout(stateWithOtherNodeShutdown, indexShard, 0L, null);
            assertThat(reevaluatedPlan.awaitWarming(), is(true));
            assertThat(
                reevaluatedPlan.timeout(),
                equalTo(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING.getDefault(
                        Settings.EMPTY
                    )
                )
            );
            assertThat(
                reevaluatedPlan.timeoutContext(),
                equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT)
            );
            assertThat(reevaluatedPlan.extendable(), is(true));
        }
    }

    /// When the relocation source is shutting down, the per-target warming timeout scales linearly with the number of search shards
    /// concurrently relocating from that source to the same target. Two targets in the same cluster state — one receiving 3 such
    /// relocations, the other receiving 1 — must yield timeouts in a 3:1 ratio because all other inputs (remaining grace, shards on
    /// source, share factor) are identical between the two calls.
    public void testSearchRecoveryTimeoutScalesByConcurrentRelocationsToTarget() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            threadPool.setCurrentTimeInMillis(0L);
            var service = newCalculationService(threadPool);

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
                .getFirst()
                .getTargetRelocatingShard();
            final ShardRouting selfT2 = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(relocationsToTarget1, totalSearchShards - 1)))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            assertThat(selfT1.currentNodeId(), equalTo(targetT1));
            assertThat(selfT2.currentNodeId(), equalTo(targetT2));

            // advance time
            threadPool.setCurrentTimeInMillis(shutdownStartedMillis + randomLongBetween(1, 100_000));
            IndexShard indexShard1 = mockIndexShard(selfT1);
            SearchRecoveryTimeout planT1 = service.searchRecoveryTimeout(state, indexShard1, 0L, null);
            IndexShard indexShard = mockIndexShard(selfT2);
            SearchRecoveryTimeout planT2 = service.searchRecoveryTimeout(state, indexShard, 0L, null);

            assertThat(planT1.awaitWarming(), is(true));
            assertThat(planT2.awaitWarming(), is(true));

            assertThat(planT1.timeout().millis(), greaterThan(0L));
            assertThat(planT2.timeout().millis(), greaterThan(0L));
            // Out of a total of 4 search shards, there are 3 shards concurrently relocating to the target1 node and only one relocating to
            // the target2 node. So, we should allow approx 3x more time for recovery for the shards recovering to target1 than target2.
            assertThat(Math.round(((double) planT1.timeout().millis()) / planT2.timeout().millis()), is(3L));
        }
    }

    public void testDataVolumeProportionalTimeoutWinsWhenLargerThanEqualShare() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            // grace cap=10s; factor=0.1 keeps equal-share well below data-volume
            Settings settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 0.1)
                .build();
            // cacheSize=1000, default cacheRatio=0.5 → warmingCacheBytes = round(1000 × 0.5) = 500
            var service = newCalculationService(threadPool, settings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();

            final Index index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String targetNodeId = "target-node";

            // 3 shards total; only 1 relocates to targetNodeId, 2 to other-node
            // → shardsOnSource=3, ongoingRelocations=1
            final ClusterState stateUncapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                3,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            // 3 shards total, all relocating to targetNodeId → ongoingRelocations=3
            final ClusterState stateCapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                3,
                3,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );

            // advance 2000ms into the 10s grace → remaining = 8000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final Map<BlobFile, WarmTarget> endTargetsToWarm = Map.of(
                new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)),
                WarmTarget.withUnknownTimestamp(400L, randomLongBetween(400L, 4_000L))
            );

            // Scenario 1: 1 ongoing relocation — data-volume heuristic value (6400ms) is visible uncapped
            final ShardRouting selfUncapped = stateUncapped.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            IndexShard indexShard2 = mockIndexShard(selfUncapped);
            final SearchRecoveryTimeout planUncapped = service.searchRecoveryTimeout(
                stateUncapped,
                indexShard2,
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(planUncapped.awaitWarming(), is(true));
            assertThat(planUncapped.timeout().millis(), equalTo(6400L)); // 6400 × 1 < 8000
            assertThat(planUncapped.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
            assertThat("data volume based plans are never extended", planUncapped.extendable(), is(false));

            // A data-volume plan is never accepted as an extension of a plan that was already computed for a shutting-down source
            IndexShard indexShard1 = mockIndexShard(selfUncapped);
            final var reevaluatedPlan = service.searchRecoveryTimeout(
                stateUncapped,
                indexShard1,
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(reevaluatedPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
            assertThat(reevaluatedPlan.extendable(), is(false));
            // the previous plan must itself be extendable, otherwise the loop would not have re-evaluated at all
            final var previousEqualSharePlan = new SearchRecoveryTimeout(
                planUncapped.timeout(),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE
            );
            assertThat(reevaluatedPlan.shouldExtendAfter(previousEqualSharePlan), is(false));

            // Scenario 2: 3 ongoing relocations — same per-shard value scaled by 3 exceeds remaining → capped
            final ShardRouting selfCapped = stateCapped.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            IndexShard indexShard = mockIndexShard(selfCapped);
            final SearchRecoveryTimeout planCapped = service.searchRecoveryTimeout(
                stateCapped,
                indexShard,
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(planCapped.awaitWarming(), is(true));
            assertThat(planCapped.timeout().millis(), equalTo(8000L)); // min(8000, 6400 × 3 = 19200)
            assertThat(planCapped.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
        }
    }

    /// A re-evaluation of an equal-share plan only hands out saved time, even when the data-volume heuristic would win: with 4 shards on
    /// the source each was budgeted 2500ms of the 10s grace; 2000ms later the fresh share is 4000ms when two shards finished early (1500ms
    /// saved) and 2000ms when none did (nothing saved), while the data-volume heuristic would give the whole remaining 8000ms.
    public void testReevaluationOfEqualSharePlanUsesSavedTimeEvenIfDataVolumeWins() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final Settings settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .build();
            // cacheSize=1000, default cacheRatio=0.5 -> warmingCacheBytes = 500, so totalBytesToWarm=500 -> dataVolume = remaining
            final var service = newCalculationService(threadPool, settings, 1000L);
            final long totalBytesToWarm = 500L;

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String targetNodeId = "target-node";

            final var previous = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(2500),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                2500.0
            );
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final int shardsLeft = randomFrom(2, 4);
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                shardsLeft,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, previous);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME));
            assertThat(plan.extendable(), is(true));
            assertThat(plan.timeout().millis(), equalTo(shardsLeft == 2 ? 1500L : 0L));
            assertThat(plan.perShardShareMs(), equalTo(8000.0 / shardsLeft));
            assertThat(plan.shouldExtendAfter(previous), is(true));

            // the first calculation of the shutdown phase still lets the data-volume heuristic win
            IndexShard indexShard = mockIndexShard(self);
            final var firstPlan = service.searchRecoveryTimeout(state, indexShard, totalBytesToWarm, null);
            assertThat(firstPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
        }
    }

    /// [SearchRecoveryTimeout#shouldExtendAfter] for every pair of contexts: any extendable context can be the previous plan (the loop
    /// stops before re-evaluating after a non-extendable one), and any context can be the re-evaluated plan. Every pair is listed
    /// explicitly, either as extending or as stopping, so that a new [TimeoutContext] fails this test until a decision is made for it.
    public void testShouldExtendAfterForEveryTransition() {
        final var anotherActiveCopy = TimeoutContext.NON_RELOCATION_ANOTHER_ACTIVE_COPY;
        final var reshardTarget = TimeoutContext.RESHARD_SPLIT_TARGET;
        final var noShutdown = TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN;
        final var metadataPresent = TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT;
        final var graceElapsed = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED;
        final var dataVolume = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME;
        final var equalShare = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE;
        final var savedTime = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME;
        final var skip = TimeoutContext.SKIP;

        // previous context -> pair(v1: contexts of the re-evaluated plan that extend the wait, v2: contexts that stop it)
        final Map<TimeoutContext, Tuple<Set<TimeoutContext>, Set<TimeoutContext>>> transitions = Map.of(
            // no shutdown involved yet: everything but "nothing to wait for" extends, including the first data-volume plan
            anotherActiveCopy,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, reshardTarget, noShutdown, metadataPresent, dataVolume, equalShare, savedTime),
                EnumSet.of(graceElapsed, skip)
            ),
            noShutdown,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, reshardTarget, noShutdown, metadataPresent, dataVolume, equalShare, savedTime),
                EnumSet.of(graceElapsed, skip)
            ),
            // staying in "metadata present" does not extend again
            metadataPresent,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, reshardTarget, noShutdown, dataVolume, equalShare, savedTime),
                EnumSet.of(metadataPresent, graceElapsed, skip)
            ),
            // within the shutdown phase only time saved by finished shards is handed out, a data-volume plan never follows
            equalShare,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, reshardTarget, noShutdown, metadataPresent, equalShare, savedTime),
                EnumSet.of(dataVolume, graceElapsed, skip)
            ),
            savedTime,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, reshardTarget, noShutdown, metadataPresent, equalShare, savedTime),
                EnumSet.of(dataVolume, graceElapsed, skip)
            )
        );

        final var extendablePreviousContexts = EnumSet.noneOf(TimeoutContext.class);
        for (final var context : TimeoutContext.values()) {
            if (context.extendable()) {
                extendablePreviousContexts.add(context);
            }
        }
        assertThat("every extendable context must have its transitions listed", transitions.keySet(), equalTo(extendablePreviousContexts));

        for (final var entry : transitions.entrySet()) {
            final var previousContext = entry.getKey();
            final var extending = entry.getValue().v1();
            final var stopping = entry.getValue().v2();
            final var decided = EnumSet.copyOf(extending);
            decided.addAll(stopping);
            assertThat("every context must be decided after " + previousContext, decided, equalTo(EnumSet.allOf(TimeoutContext.class)));
            assertThat(
                "a context cannot both extend and stop after " + previousContext,
                extending.size() + stopping.size(),
                equalTo(decided.size())
            );

            final var previous = new SearchRecoveryTimeout(TimeValue.timeValueMillis(100), previousContext);
            for (final var context : TimeoutContext.values()) {
                final var reevaluated = new SearchRecoveryTimeout(TimeValue.timeValueMillis(100), context);
                assertThat(
                    previousContext + " -> " + context,
                    reevaluated.shouldExtendAfter(previous),
                    equalTo(extending.contains(context))
                );
            }
        }
    }

    public void testEqualShareTimeoutWinsWhenDataVolumeIsSmall() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            // grace cap=10s; factor=2.0: equal-share wins over data-volume, and scales past remaining
            // when multiplied by 3 ongoing relocations (factor > 1 is required to cap the equal-share path)
            Settings settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 2.0)
                .build();
            var service = newCalculationService(threadPool, settings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();

            final Index index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String targetNodeId = "target-node";

            // 4 shards total; 1 relocates to targetNodeId, 3 to other-node
            // → shardsOnSource=4, ongoingRelocations=1
            final ClusterState stateUncapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                4,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            // 4 shards total; 3 relocate to targetNodeId, 1 to other-node
            // → shardsOnSource=4, ongoingRelocations=3
            final ClusterState stateCapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                4,
                3,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );

            // advance 2000ms into the 10s grace → remaining = 8000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            // totalBytesToWarm=20 → dataVolume = (20/500.0) × 8000 = 320 < equalShare 4000
            final Map<BlobFile, WarmTarget> endTargetsToWarm = Map.of(
                new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)),
                WarmTarget.withUnknownTimestamp(20L, randomLongBetween(20L, 2_000L))
            );

            // Scenario 1: 1 ongoing relocation — equal-share value (4000ms) is visible uncapped
            final ShardRouting selfUncapped = stateUncapped.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            IndexShard indexShard1 = mockIndexShard(selfUncapped);
            final SearchRecoveryTimeout planUncapped = service.searchRecoveryTimeout(
                stateUncapped,
                indexShard1,
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(planUncapped.awaitWarming(), is(true));
            assertThat(planUncapped.timeout().millis(), equalTo(4000L)); // 4000 × 1 < 8000
            assertThat(planUncapped.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));

            // Scenario 2: 3 ongoing relocations — same per-shard value scaled by 3 exceeds remaining → capped
            final ShardRouting selfCapped = stateCapped.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            IndexShard indexShard = mockIndexShard(selfCapped);
            final SearchRecoveryTimeout planCapped = service.searchRecoveryTimeout(
                stateCapped,
                indexShard,
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(planCapped.awaitWarming(), is(true));
            assertThat(planCapped.timeout().millis(), equalTo(8000L)); // min(8000, 4000 × 3 = 12000)
            assertThat(planCapped.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
        }
    }

    /**
     * The relocation source is already shutting down when the first plan is computed, and the equal-share heuristic wins over the
     * data-volume one. The plan must be extendable, and a re-evaluation after the first slice has expired must land on equal-share
     * again, be extendable again, and be sized from the time remaining at that point.
     */
    public void testEqualShareTimeoutIsExtendedWhenSourceWasAlreadyShuttingDown() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final var gracePeriodCap = TimeValue.timeValueSeconds(10);
            final Settings settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), gracePeriodCap)
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 1.0)
                .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
                .build();
            // cacheSize=1000, default cacheRatio=0.5 → warmingCacheBytes = 500
            final var service = newCalculationService(threadPool, settings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();

            final Index index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String targetNodeId = "target-node";

            // 4 shards on the source, 1 of them relocating to targetNodeId → shardsOnSource=4, ongoingRelocations=1
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                4,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .get(0)
                .getTargetRelocatingShard();

            // totalBytesToWarm=20 → dataVolume = (20/500) × remaining, always well below the equal share
            final Map<BlobFile, WarmTarget> endTargetsToWarm = Map.of(
                new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)),
                WarmTarget.withUnknownTimestamp(20L, randomLongBetween(20L, 2_000L))
            );
            final long totalBytesToWarm = totalBytesToWarm(endTargetsToWarm);

            // 2000ms into the 10s grace → remaining = 8000ms, equal share = 8000 / 4 = 2000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);
            IndexShard indexShard = mockIndexShard(self);
            final var initialPlan = service.searchRecoveryTimeout(state, indexShard, totalBytesToWarm, null);
            assertThat(initialPlan.timeout().millis(), equalTo(2000L));
            assertThat(initialPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
            assertThat(initialPlan.extendable(), is(true));

            // the first slice expires and no shard has left the source: 4000ms into the 10s grace → remaining = 6000ms, fresh share
            // = 6000 / 4 = 1500ms, which is below the 2000ms already budgeted per shard, so nothing was saved
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 4000);
            final var reevaluatedPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, initialPlan);
            assertThat("nothing saved, so nothing to extend by", reevaluatedPlan.timeout().millis(), equalTo(0L));
            assertThat(reevaluatedPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME));
            assertThat(reevaluatedPlan.extendable(), is(true));
        }
    }

    /// A re-evaluation only hands out the time saved by shards that left the source since the previous plan, split across the shards
    /// still on it: 10s grace, 4 shards on the source gives each 2500ms; after 2000ms two of them are done, so the remaining two share
    /// the 8000ms left (4000ms each), i.e. 1500ms more than they were budgeted.
    public void testReevaluationExtendsOnlyByTimeSavedByFinishedShards() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final Settings settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
                .build();
            final var service = newCalculationService(threadPool, settings, 0L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String targetNodeId = "target-node";

            final ClusterState initialState = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                4,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            final ShardRouting self = initialState.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            IndexShard indexShard = mockIndexShard(self);
            final var initialPlan = service.searchRecoveryTimeout(initialState, indexShard, 0L, null);
            assertThat(initialPlan.timeout().millis(), equalTo(10_000L / 4));
            assertThat(initialPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));

            // two other shards finished on time, one slice later only two shards are left on the source
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);
            final ClusterState stateAfterEarlyFinishes = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                2,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            final var reevaluatedPlan = service.searchRecoveryTimeout(stateAfterEarlyFinishes, mockIndexShard(self), 0L, initialPlan);
            assertThat(reevaluatedPlan.timeout().millis(), equalTo(8000L / 2 - 10_000L / 4));
            assertThat(reevaluatedPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME));
            assertThat(reevaluatedPlan.extendable(), is(true));

            // the saved-time plan carries the share it was computed from, so only the time saved after it is handed out next
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 4000);
            final var nextPlan = service.searchRecoveryTimeout(stateAfterEarlyFinishes, mockIndexShard(self), 0L, reevaluatedPlan);
            assertThat(nextPlan.timeout().millis(), equalTo(0L));
        }
    }

    /**
     * Builds a cluster state where shard 1 of {@code indexName} is an INITIALIZING {@link ShardRouting.Role#SEARCH_ONLY} replica
     * representing a resharding split target. The index has resharding metadata that identifies shard 0 as the source shard and shard 1
     * as the target, matching what {@link IndexReshardingMetadata#newSplitByMultiple(int, int)} produces for a 1→2 split.
     * Shard 1 has no active search copy (it is brand-new), so the non-relocation {@code hasAnotherActiveSearchShardCopy} branch does
     * not apply — only the reshard-target branch applies.
     */
    private static ClusterState clusterStateReshardTargetInitializingSearchShard(String indexName) {
        final String primaryNodeId = "primary-node";
        final String targetNodeId = "target-node";
        final String masterNodeId = "master-node";
        // Build the base metadata first so that routingNumShards is initialized, then layer resharding on top.
        final IndexMetadata baseIndexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), IndexMetadata.INDEX_UUID_NA_VALUE, 1, 0))
            .primaryTerm(0, 1)
            .build();
        final IndexReshardingMetadata reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(1, 2);
        final IndexMetadata indexMetadata = IndexMetadata.builder(baseIndexMetadata)
            .reshardingMetadata(reshardingMetadata)
            .reshardAddShards(reshardingMetadata.shardCountAfter())
            .primaryTerm(1, 1)
            .build();
        final ShardId shard0 = new ShardId(indexMetadata.getIndex(), 0);
        final ShardId shard1 = new ShardId(indexMetadata.getIndex(), 1);
        // Each shard's IndexShardRoutingTable requires exactly one primary. For shard 1, the INDEX_ONLY primary
        // is recovering on the same node as the search-only replica; the SEARCH_ONLY shard is the one under test.
        final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shard0).addShard(
                    TestShardRouting.shardRoutingBuilder(shard0, primaryNodeId, true, STARTED)
                        .withRole(ShardRouting.Role.INDEX_ONLY)
                        .build()
                )
            )
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shard1).addShard(
                    TestShardRouting.shardRoutingBuilder(shard1, primaryNodeId, true, STARTED)
                        .withRole(ShardRouting.Role.INDEX_ONLY)
                        .build()
                )
                    .addShard(
                        TestShardRouting.shardRoutingBuilder(shard1, targetNodeId, false, INITIALIZING)
                            .withRole(ShardRouting.Role.SEARCH_ONLY)
                            .build()
                    )
            );
        return ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(primaryNodeId))
                    .add(DiscoveryNodeUtils.create(targetNodeId))
                    .add(DiscoveryNodeUtils.create(masterNodeId))
                    .localNodeId(targetNodeId)
                    .masterNodeId(masterNodeId)
                    .build()
            )
            .metadata(
                Metadata.builder().put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false)).generateClusterUuidIfNeeded()
            )
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build())
            .build();
    }

    /**
     * The reshard-target warming timeout default must be at least a few seconds smaller than the search-shards-online timeout so that
     * warming has time to finish before the state machine gives up waiting for the shard to go GREEN and publishes SPLIT without it.
     */
    public void testReshardTargetWarmingTimeoutDefaultIsSmallerThanOnlineTimeout() {
        final long warmingDefault = SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING.getDefault(
            Settings.EMPTY
        ).millis();
        final long onlineDefault = SplitTargetService.RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.getDefault(Settings.EMPTY).millis();
        assertThat(
            "reshard target warming timeout default must leave at least 3 s margin before the search-shards-online timeout",
            warmingDefault,
            lessThan(onlineDefault - TimeValue.timeValueSeconds(3).millis())
        );
    }

    /**
     * Resharding split target: the shard is brand-new (no other active search copy), so
     * {@link SearchRecoveryTimeoutCalculationService#searchRecoveryTimeout} must use the reshard-target timeout rather than skip.
     */
    public void testSearchRecoveryReshardTargetAwaitsWarming() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
            ClusterState state = clusterStateReshardTargetInitializingSearchShard("idx");
            ShardId shard1 = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 1);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shard1).replicaShards().getFirst();
            IndexShard indexShard = mockIndexShard(self);
            var plan = service.searchRecoveryTimeout(state, indexShard, 0L, null);
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }

    /**
     * Resharding split target with an active cluster shutdown: the timeout is short enough that warming still proceeds — unlike the
     * non-relocation branch, the reshard-target branch does not suppress warming during shutdown.
     */
    public void testSearchRecoveryReshardTargetAwaitsWarmingEvenWithActiveShutdown() {
        try (var threadPool = new TestThreadPool(getTestName(), StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true))) {
            var service = newCalculationService(threadPool);
            ClusterState base = clusterStateReshardTargetInitializingSearchShard("idx");
            ClusterState state = withActiveShutdownNodeMetadata(base, null);
            assertThat(state.metadata().nodeShutdowns().getAll().isEmpty(), is(false));
            ShardId shard1 = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 1);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shard1).replicaShards().getFirst();
            IndexShard indexShard = mockIndexShard(self);
            var plan = service.searchRecoveryTimeout(state, indexShard, 0L, null);
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }
}
