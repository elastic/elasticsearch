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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateInitializingSearchReplicaWithActivePeer;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateOneSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.initializingSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.mockIndexShard;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.totalBytesToWarm;
import static org.hamcrest.Matchers.empty;
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
        return newCalculationService(threadPool, newClusterSettings(settings), cacheSize);
    }

    private static SearchRecoveryTimeoutCalculationService newCalculationService(
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        long cacheSize
    ) {
        final var cacheService = Mockito.mock(StatelessSharedBlobCacheService.class);
        when(cacheService.getCacheSize()).thenReturn(cacheSize);
        // Unknown heap size, so that only the static incoming recovery limit applies.
        return new SearchRecoveryTimeoutCalculationService(cacheService, threadPool, clusterSettings, ByteSizeValue.ZERO);
    }

    private static ClusterSettings newClusterSettings(Settings settings) {
        final Set<Setting<?>> registeredSettings = new HashSet<>(DataNodeRecoveryThrottlingSettings.settings());
        registeredSettings.addAll(
            List.of(
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
        return new ClusterSettings(settings, registeredSettings);
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

    /// Builds a cluster state with `numShards` SEARCH_ONLY replicas on `sourceNodeId`,
    /// with the first `numShardsToTarget` relocating to `targetNodeId` and the remainder
    /// relocating to `other-node`, plus `numPendingShards` additional SEARCH_ONLY shards that are STARTED on `sourceNodeId` (not
    /// relocating yet). The source is marked for REMOVE shutdown starting at `startedAtMillis`, the effective grace period is controlled
    /// via [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING].
    private static ClusterState clusterStateSearchShardsRelocatingFromShuttingDownSource(
        int numShards,
        int numShardsToTarget,
        Index index,
        String sourceNodeId,
        String targetNodeId,
        long startedAtMillis,
        int numPendingShards
    ) {
        assert numShardsToTarget <= numShards;
        final String primaryNodeId = "primary-node";
        final String masterNodeId = "master-node";
        final String otherNodeId = "other-node";
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), numShards + numPendingShards, 1))
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
        for (int s = numShards; s < numShards + numPendingShards; s++) {
            final ShardId sid = new ShardId(index, s);
            final ShardRouting primary = TestShardRouting.shardRoutingBuilder(sid, primaryNodeId, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final ShardRouting pending = TestShardRouting.shardRoutingBuilder(sid, sourceNodeId, false, STARTED)
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(sid).addShard(primary).addShard(pending));
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

    /// Builds search replicas on two REMOVE sources and a RESTART source. The first `numShardsToTarget` shards relocate
    /// from `sourceNodeId` to `targetNodeId`, followed by `numShardsFromOtherSourceToTarget` from `otherSourceNodeId`.
    /// Remaining source shards are started, except for `numShardsToOtherTarget` relocating from `sourceNodeId` to `otherTargetNodeId`.
    /// The restarting source's shards all relocate to `targetNodeId`, but do not compete for shutdown relocation slots.
    /// The index-only primaries sit on a node that is also marked for removal, and stale shutdown metadata is present, so these
    /// nodes are visited when counting relocations from shutting-down nodes but contribute none.
    private static ClusterState clusterStateSearchShardsRelocatingFromMultipleSources(
        int numShards,
        int numShardsToTarget,
        int numShardsToOtherTarget,
        int numShardsOnOtherSource,
        int numShardsFromOtherSourceToTarget,
        int numShardsFromRestartingSource,
        Index index,
        String sourceNodeId,
        String otherSourceNodeId,
        String restartingSourceNodeId,
        String targetNodeId,
        String otherTargetNodeId,
        long startedAtMillis,
        long otherStartedAtMillis
    ) {
        assert numShardsToTarget + numShardsToOtherTarget <= numShards;
        assert numShardsFromOtherSourceToTarget <= numShardsOnOtherSource;
        final String masterNodeId = "master-node";
        final String primaryNodeId = "primary-node";

        // Each search shard is represented by its current node and relocation target (null if not relocating yet).
        final List<Tuple<String, String>> searchShards = new ArrayList<>();
        searchShards.addAll(Collections.nCopies(numShardsToTarget, new Tuple<>(sourceNodeId, targetNodeId)));
        searchShards.addAll(Collections.nCopies(numShardsFromOtherSourceToTarget, new Tuple<>(otherSourceNodeId, targetNodeId)));
        searchShards.addAll(Collections.nCopies(numShards - numShardsToTarget - numShardsToOtherTarget, new Tuple<>(sourceNodeId, null)));
        searchShards.addAll(
            Collections.nCopies(numShardsOnOtherSource - numShardsFromOtherSourceToTarget, new Tuple<>(otherSourceNodeId, null))
        );
        searchShards.addAll(Collections.nCopies(numShardsToOtherTarget, new Tuple<>(sourceNodeId, otherTargetNodeId)));

        // Relocations off a node that is not marked for removal (here a RESTART shutdown) do not affect the timeout computation.
        searchShards.addAll(Collections.nCopies(numShardsFromRestartingSource, new Tuple<>(restartingSourceNodeId, targetNodeId)));

        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), searchShards.size(), 1))
            .build();
        final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(index);
        for (int s = 0; s < searchShards.size(); s++) {
            final var sid = new ShardId(index, s);
            final var primary = TestShardRouting.shardRoutingBuilder(sid, primaryNodeId, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final String currentNodeId = searchShards.get(s).v1();
            final String relocationTarget = searchShards.get(s).v2();
            final ShardRouting searchShard = relocationTarget == null
                ? TestShardRouting.shardRoutingBuilder(sid, currentNodeId, false, STARTED).withRole(ShardRouting.Role.SEARCH_ONLY).build()
                : TestShardRouting.shardRoutingBuilder(sid, currentNodeId, false, RELOCATING)
                    .withRelocatingNodeId(relocationTarget)
                    .withRole(ShardRouting.Role.SEARCH_ONLY)
                    .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(sid).addShard(primary).addShard(searchShard));
        }

        // the node holding the index shards is shutting down too, but index shards are not counted
        final Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        for (final String nodeId : List.of(sourceNodeId, otherSourceNodeId, primaryNodeId, "stale-node", restartingSourceNodeId)) {
            shutdowns.put(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setType(
                        nodeId.equals(restartingSourceNodeId)
                            ? SingleNodeShutdownMetadata.Type.RESTART
                            : SingleNodeShutdownMetadata.Type.REMOVE
                    )
                    .setReason("SearchRecoveryTimeoutCalculationServiceTests")
                    .setStartedAtMillis(nodeId.equals(otherSourceNodeId) ? otherStartedAtMillis : startedAtMillis)
                    .setNodeSeen(true)
                    .build()
            );
        }
        return ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(masterNodeId))
                    .masterNodeId(masterNodeId)
                    .localNodeId(masterNodeId)
                    .add(DiscoveryNodeUtils.create(primaryNodeId))
                    .add(DiscoveryNodeUtils.create(sourceNodeId))
                    .add(DiscoveryNodeUtils.create(otherSourceNodeId))
                    .add(DiscoveryNodeUtils.create(restartingSourceNodeId))
                    .add(DiscoveryNodeUtils.create(targetNodeId))
                    .add(DiscoveryNodeUtils.create(otherTargetNodeId))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdowns))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build())
            .build();
    }

    private static ClusterState clusterStateSearchShardsRelocatingFromShuttingDownSource(
        int numShards,
        int numShardsToTarget,
        Index index,
        String sourceNodeId,
        String targetNodeId,
        long startedAtMillis
    ) {
        return clusterStateSearchShardsRelocatingFromShuttingDownSource(
            numShards,
            numShardsToTarget,
            index,
            sourceNodeId,
            targetNodeId,
            startedAtMillis,
            0
        );
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
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
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
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
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
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
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

            final var initialPlan = service.searchRecoveryTimeout(stateWithoutShutdown, mockIndexShard(self), 0L, null);
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

            final var reevaluatedPlan = service.searchRecoveryTimeout(stateWithOtherNodeShutdown, mockIndexShard(self), 0L, null);
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

    /// When the relocation source is shutting down and the target does not throttle relocation recoveries, the per-target
    /// warming timeout scales linearly with the number of search shards relocating from that source to the same target
    /// in the cluster state.
    public void testSearchRecoveryTimeoutScalesByClusterStateRelocationsToTarget() {
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

            // Each search shard is modeled as its own ShardId with an INDEX_ONLY primary. ShardIds must be distinct because every
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

            // Make it divisible by four to simplify the ratio below (no rounding).
            threadPool.setCurrentTimeInMillis(shutdownStartedMillis + 4 * randomLongBetween(1, 25_000));
            SearchRecoveryTimeout planT1 = service.searchRecoveryTimeout(state, mockIndexShard(selfT1), 0L, null);
            SearchRecoveryTimeout planT2 = service.searchRecoveryTimeout(state, mockIndexShard(selfT2), 0L, null);

            assertThat(planT1.awaitWarming(), is(true));
            assertThat(planT2.awaitWarming(), is(true));

            assertThat(planT1.timeout().millis(), greaterThan(0L));
            assertThat(planT2.timeout().millis(), greaterThan(0L));
            // Out of a total of 4 search shards, there are 3 shards concurrently relocating to the target1 node and only one
            // relocating to the target2 node. Both divide by all four shards on the source, preserving the original ratio.
            assertThat(planT1.timeout().millis(), equalTo(planT2.timeout().millis() * 3));
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

            // 3 shards total: only 1 relocates to targetNodeId, the other 2 relocate to another node
            // so shardsOnSource=3 and concurrentRelocations=1
            final ClusterState stateUncapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                3,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            // 3 shards total, all relocating to targetNodeId so shardsOnSource=3 and concurrentRelocations=3
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
            final SearchRecoveryTimeout planUncapped = service.searchRecoveryTimeout(
                stateUncapped,
                mockIndexShard(selfUncapped),
                SharedBlobCacheWarmingService.totalBytesToWarm(endTargetsToWarm),
                null
            );
            assertThat(planUncapped.awaitWarming(), is(true));
            assertThat(planUncapped.timeout().millis(), equalTo(6400L)); // 6400 × 1 < 8000
            assertThat(planUncapped.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
            assertThat("data volume based plans are never extended", planUncapped.extendable(), is(false));

            // A data-volume plan is never accepted as an extension of a plan that was already computed for a shutting-down source
            final var reevaluatedPlan = service.searchRecoveryTimeout(
                stateUncapped,
                mockIndexShard(selfUncapped),
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
            final SearchRecoveryTimeout planCapped = service.searchRecoveryTimeout(
                stateCapped,
                mockIndexShard(selfCapped),
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
                shardsLeft - 1,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis,
                1
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, previous);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
            assertThat(plan.extendable(), is(true));
            assertThat(plan.timeout().millis(), equalTo(shardsLeft == 2 ? 1500L : 0L));
            assertThat(plan.perShardShareMs(), equalTo(8000.0 / shardsLeft));
            assertThat(plan.shouldExtendAfter(previous), is(true));

            // the first calculation of the shutdown phase still lets the data-volume heuristic win
            final var firstPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, null);
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
        final var skip = TimeoutContext.SKIP;

        // previous context -> pair(v1: contexts of the re-evaluated plan that extend the wait, v2: contexts that stop it)
        final Map<TimeoutContext, Tuple<Set<TimeoutContext>, Set<TimeoutContext>>> transitions = Map.of(
            // no shutdown involved yet: everything but "nothing to wait for" and a reshard target plan (which never extends a wait)
            // extends, including the first data-volume plan
            anotherActiveCopy,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, noShutdown, metadataPresent, dataVolume, equalShare),
                EnumSet.of(reshardTarget, graceElapsed, skip)
            ),
            noShutdown,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, noShutdown, metadataPresent, dataVolume, equalShare),
                EnumSet.of(reshardTarget, graceElapsed, skip)
            ),
            // staying in "metadata present" does not extend again
            metadataPresent,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, noShutdown, dataVolume, equalShare),
                EnumSet.of(reshardTarget, metadataPresent, graceElapsed, skip)
            ),
            // within the shutdown phase only time saved by finished shards is handed out, a data-volume plan never follows
            equalShare,
            Tuple.tuple(
                EnumSet.of(anotherActiveCopy, noShutdown, metadataPresent, equalShare),
                EnumSet.of(reshardTarget, dataVolume, graceElapsed, skip)
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

    /// A [SearchRecoveryTimeoutCalculationService] whose plan calculation returns a fixed plan, so that
    /// [SearchRecoveryTimeoutCalculationService#extendedTimeout] can be tested without building cluster state (which the real calculation
    /// needs, and which [#testShouldExtendAfterForEveryTransition] and the tests above already cover).
    private static class FixedPlanCalculationService extends SearchRecoveryTimeoutCalculationService {

        private final SearchRecoveryTimeout plan;
        private final List<Long> bytesToWarmSeen = new ArrayList<>();
        private final List<SearchRecoveryTimeout> previousPlansSeen = new ArrayList<>();

        FixedPlanCalculationService(ThreadPool threadPool, Settings settings, SearchRecoveryTimeout plan) {
            this(threadPool, newClusterSettings(settings), plan);
        }

        FixedPlanCalculationService(ThreadPool threadPool, ClusterSettings clusterSettings, SearchRecoveryTimeout plan) {
            super(Mockito.mock(StatelessSharedBlobCacheService.class), threadPool, clusterSettings, ByteSizeValue.ZERO);
            this.plan = plan;
        }

        @Override
        public SearchRecoveryTimeout searchRecoveryTimeout(
            ClusterState state,
            IndexShard indexShard,
            long bytesToWarm,
            @Nullable SearchRecoveryTimeout previousPlan
        ) {
            bytesToWarmSeen.add(bytesToWarm);
            previousPlansSeen.add(previousPlan);
            return plan;
        }

        SearchRecoveryTimeout extendedTimeout(
            long totalBytesToWarm,
            long bytesWarmedSoFar,
            SearchRecoveryTimeout latestPlan,
            TimeValue elapsed
        ) {
            // the shard is only passed through to the (overridden) calculation
            return extendedTimeout(
                ClusterState.EMPTY_STATE,
                Mockito.mock(IndexShard.class),
                totalBytesToWarm,
                bytesWarmedSoFar,
                latestPlan,
                elapsed
            );
        }
    }

    private static Settings reevaluationSettings(boolean enabled, TimeValue totalTimeoutCap, TimeValue abortThreshold) {
        return Settings.builder()
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), enabled)
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING.getKey(), totalTimeoutCap)
            .put(
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING.getKey(),
                abortThreshold
            )
            .build();
    }

    private static SearchRecoveryTimeout extendablePlan(long timeoutMillis) {
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(timeoutMillis),
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN
        );
    }

    /// With re-evaluation disabled the wait ends at the first expiry, and the plan is not even recalculated.
    public void testExtendedTimeoutSkipsWhenReevaluationDisabled() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(false, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50));
            final var service = new FixedPlanCalculationService(threadPool, settings, extendablePlan(200));

            final var result = service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.ZERO);

            assertThat(result.awaitWarming(), is(false));
            assertThat(service.bytesToWarmSeen, is(empty()));
        }
    }

    /// A plan whose context is not extendable ends the wait at its expiry whatever the re-evaluation would say, and is not recalculated.
    public void testExtendedTimeoutSkipsWhenLatestPlanIsNotExtendable() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(true, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50));
            final var service = new FixedPlanCalculationService(threadPool, settings, extendablePlan(200));

            for (final var context : TimeoutContext.values()) {
                if (context.extendable() == false) {
                    final var latestPlan = new SearchRecoveryTimeout(TimeValue.timeValueMillis(200), context);
                    assertThat(context.name(), service.extendedTimeout(1_000L, 0L, latestPlan, TimeValue.ZERO).awaitWarming(), is(false));
                }
            }
            assertThat(service.bytesToWarmSeen, is(empty()));
        }
    }

    /// The re-evaluated plan must be allowed to follow the latest one, see [SearchRecoveryTimeout#shouldExtendAfter]: a data-volume plan
    /// never follows a plan computed for a source that is already shutting down.
    public void testExtendedTimeoutSkipsWhenReevaluatedPlanMayNotFollowLatestPlan() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(true, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50));
            final var latestPlan = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(200),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                2500.0
            );
            final var reevaluated = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(200),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME
            );
            assertThat(reevaluated.shouldExtendAfter(latestPlan), is(false));
            final var service = new FixedPlanCalculationService(threadPool, settings, reevaluated);

            assertThat(service.extendedTimeout(1_000L, 0L, latestPlan, TimeValue.ZERO).awaitWarming(), is(false));
            assertThat("the plan was recalculated against the latest plan", service.previousPlansSeen, equalTo(List.of(latestPlan)));
        }
    }

    /// An extension within the total budget is returned as calculated, keeping its context and per-shard share.
    public void testExtendedTimeoutReturnsReevaluatedPlanWithinBudget() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(true, TimeValue.timeValueMillis(1_000), TimeValue.timeValueMillis(50));
            final var reevaluated = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(200),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                2500.0
            );
            final var service = new FixedPlanCalculationService(threadPool, settings, reevaluated);

            final var result = service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(300));

            assertThat(result, equalTo(reevaluated));
        }
    }

    /// The extension is cut to what is left of the total timeout cap after the time already waited, so a wait cannot exceed the cap by
    /// being extended.
    public void testExtendedTimeoutIsCappedToTheRemainingTotalBudget() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(true, TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(50));
            final var reevaluated = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(200),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                2500.0
            );
            final var service = new FixedPlanCalculationService(threadPool, settings, reevaluated);

            // 200ms waited: the 200ms extension is cut to the 100ms left of the 300ms cap
            final var result = service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(200));

            assertThat(result.awaitWarming(), is(true));
            assertThat(result.timeout(), equalTo(TimeValue.timeValueMillis(100)));
            assertThat(result.timeoutContext(), equalTo(reevaluated.timeoutContext()));
            assertThat(result.perShardShareMs(), equalTo(reevaluated.perShardShareMs()));
        }
    }

    /// A capped extension below the abort threshold is not worth waiting for and ends the wait, one at the threshold still extends it.
    /// Once the whole cap was used up, or when it is zero, nothing is left to extend by.
    public void testExtendedTimeoutAbortsBelowThreshold() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var abortThreshold = TimeValue.timeValueMillis(50);
            final var settings = reevaluationSettings(true, TimeValue.timeValueMillis(300), abortThreshold);
            final var service = new FixedPlanCalculationService(threadPool, settings, extendablePlan(200));

            assertThat(
                "exactly the threshold is left",
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(250)).timeout(),
                equalTo(abortThreshold)
            );
            assertThat(
                "just below the threshold is left",
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(251)).awaitWarming(),
                is(false)
            );
            assertThat(
                "the cap is used up",
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(300)).awaitWarming(),
                is(false)
            );
            assertThat(
                "waited longer than the cap",
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.timeValueMillis(500)).awaitWarming(),
                is(false)
            );

            final var zeroCapService = new FixedPlanCalculationService(
                threadPool,
                reevaluationSettings(true, TimeValue.ZERO, abortThreshold),
                extendablePlan(200)
            );
            assertThat(
                "a zero cap disables extensions",
                zeroCapService.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.ZERO).awaitWarming(),
                is(false)
            );
        }
    }

    public void testExtendedTimeoutFollowsReevaluationSettingsUpdates() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var disabled = reevaluationSettings(false, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50));
            final var clusterSettings = newClusterSettings(disabled);
            final var service = new FixedPlanCalculationService(threadPool, clusterSettings, extendablePlan(200));
            final var elapsed = TimeValue.timeValueMillis(200);

            assertThat(service.extendedTimeout(1_000L, 0L, extendablePlan(200), elapsed).awaitWarming(), is(false));

            // Every update restates all the settings, since the ones left out go back to their defaults.
            clusterSettings.applySettings(reevaluationSettings(true, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50)));
            assertThat(
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), elapsed).timeout(),
                equalTo(TimeValue.timeValueMillis(200))
            );

            // 100ms are left of the lowered cap
            clusterSettings.applySettings(reevaluationSettings(true, TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(50)));
            assertThat(
                service.extendedTimeout(1_000L, 0L, extendablePlan(200), elapsed).timeout(),
                equalTo(TimeValue.timeValueMillis(100))
            );

            // the 100ms left are no longer worth waiting for
            clusterSettings.applySettings(reevaluationSettings(true, TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(150)));
            assertThat(service.extendedTimeout(1_000L, 0L, extendablePlan(200), elapsed).awaitWarming(), is(false));

            clusterSettings.applySettings(reevaluationSettings(false, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50)));
            assertThat(service.extendedTimeout(1_000L, 0L, extendablePlan(200), elapsed).awaitWarming(), is(false));
        }
    }

    /// The re-evaluation is given the bytes still to warm: the total minus what was warmed so far, never below zero since the warmed
    /// counter also covers reads that are not part of the total.
    public void testExtendedTimeoutRecalculatesForTheBytesStillToWarm() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final var settings = reevaluationSettings(true, TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(50));
            final var service = new FixedPlanCalculationService(threadPool, settings, extendablePlan(200));

            service.extendedTimeout(1_000L, 0L, extendablePlan(200), TimeValue.ZERO);
            service.extendedTimeout(1_000L, 400L, extendablePlan(200), TimeValue.ZERO);
            service.extendedTimeout(1_000L, 1_000L, extendablePlan(200), TimeValue.ZERO);
            service.extendedTimeout(1_000L, 10_000L, extendablePlan(200), TimeValue.ZERO);

            assertThat(service.bytesToWarmSeen, equalTo(List.of(1_000L, 600L, 0L, 0L)));
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

            // 4 shards total; 1 relocates to targetNodeId, the other 3 relocate to another node
            // so shardsOnSource=4 and concurrentRelocations=1
            final ClusterState stateUncapped = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                4,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis
            );
            // 4 shards total; 3 relocate to targetNodeId, 1 relocates to another node
            // so shardsOnSource=4 and concurrentRelocations=3
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
            final SearchRecoveryTimeout planUncapped = service.searchRecoveryTimeout(
                stateUncapped,
                mockIndexShard(selfUncapped),
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
            final SearchRecoveryTimeout planCapped = service.searchRecoveryTimeout(
                stateCapped,
                mockIndexShard(selfCapped),
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

            // 4 shards on the source: 3 relocating (1 of them to targetNodeId) and 1 pending → shardsOnSource=4, ongoingRelocations=1
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                3,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis,
                1
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
            final var initialPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, null);
            assertThat(initialPlan.timeout().millis(), equalTo(2000L));
            assertThat(initialPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
            assertThat(initialPlan.extendable(), is(true));

            // the first slice expires and no shard has left the source: 4000ms into the 10s grace → remaining = 6000ms, fresh share
            // = 6000 / 4 = 1500ms, which is below the 2000ms already budgeted per shard, so nothing was saved
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 4000);
            final var reevaluatedPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, initialPlan);
            assertThat("nothing saved, so nothing to extend by", reevaluatedPlan.timeout().millis(), equalTo(0L));
            assertThat(reevaluatedPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
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
                3,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis,
                1
            );
            final ShardRouting self = initialState.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            final var initialPlan = service.searchRecoveryTimeout(initialState, mockIndexShard(self), 0L, null);
            assertThat(initialPlan.timeout().millis(), equalTo(10_000L / 4));
            assertThat(initialPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));

            // two other shards finished on time, one slice later only two shards (one of them still pending) are left on the source
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);
            final ClusterState stateAfterEarlyFinishes = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                1,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis,
                1
            );
            final var reevaluatedPlan = service.searchRecoveryTimeout(stateAfterEarlyFinishes, mockIndexShard(self), 0L, initialPlan);
            assertThat(reevaluatedPlan.timeout().millis(), equalTo(8000L / 2 - 10_000L / 4));
            assertThat(reevaluatedPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
            assertThat(reevaluatedPlan.extendable(), is(true));

            // the saved-time plan carries the share it was computed from, so only the time saved after it is handed out next
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 4000);
            final var nextPlan = service.searchRecoveryTimeout(stateAfterEarlyFinishes, mockIndexShard(self), 0L, reevaluatedPlan);
            assertThat(nextPlan.timeout().millis(), equalTo(0L));
        }
    }

    /// If neither the time nor the cluster state changed since a plan was computed, a re-evaluation has nothing to add: the fresh share
    /// equals the one the previous plan was computed from, so the extension is exactly 0, and so it stays on further re-evaluations.
    public void testReevaluationWithNothingChangedExtendsByNothing() {
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

            // 4 shards on the source: 3 relocating (1 of them to targetNodeId) and 1 pending, so remaining time is not simply handed out
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                3,
                1,
                index,
                sourceNodeId,
                targetNodeId,
                startedAtMillis,
                1
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            // 2000ms into the 10s grace -> remaining = 8000ms, equal share = 8000 / 4 = 2000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);
            final var firstPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
            assertThat(firstPlan.timeout().millis(), equalTo(2000L));
            assertThat(firstPlan.perShardShareMs(), equalTo(2000.0));

            var previous = firstPlan;
            for (int i = 0; i < 3; i++) {
                final var reevaluated = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, previous);
                assertThat("re-evaluation " + i, reevaluated.timeout().millis(), equalTo(0L));
                assertThat(reevaluated.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
                assertThat(reevaluated.perShardShareMs(), equalTo(2000.0));
                previous = reevaluated;
            }
        }
    }

    /// When the first plan of a shutting-down source expires and nothing changed since, which means no shard left the source, the wait is
    /// not extended, whichever heuristic produced that first plan: an equal-share plan has no saved time to hand out, and a data-volume
    /// plan is never extended.
    public void testExtendedTimeoutDoesNotExtendWhenNothingChangedSinceTheFirstPlan() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final long graceMs = 10_000;
            final int relocatingShards = 3;
            final int relocatingShardsToTarget = 1;
            final int pendingShards = 1;
            final long cacheSize = 1_000L;
            final Settings settings = Settings.builder()
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(),
                    TimeValue.timeValueMillis(graceMs)
                )
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING.getKey(), 1.0)
                .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
                .build();
            final var service = newCalculationService(threadPool, settings, cacheSize);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final var index = new Index("idx", randomUUID());
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocatingShards,
                relocatingShardsToTarget,
                index,
                "source-node",
                "target-node",
                threadPool.absoluteTimeInMillis(),
                pendingShards
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            // an empty shard makes the equal share win, one that fills the whole warming cache makes the data volume win
            final boolean dataVolumeWins = randomBoolean();
            final long totalBytesToWarm = dataVolumeWins ? cacheSize : 0L;
            final var firstPlan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm, null);
            assertThat(
                firstPlan.timeoutContext(),
                equalTo(
                    dataVolumeWins
                        ? TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME
                        : TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE
                )
            );

            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + firstPlan.timeout().millis());
            final var extended = service.extendedTimeout(
                state,
                mockIndexShard(self),
                totalBytesToWarm,
                randomLongBetween(0, totalBytesToWarm),
                firstPlan,
                firstPlan.timeout()
            );
            assertThat(extended.awaitWarming(), is(false));
        }
    }

    /// With no shard pending on the source nobody waits for a slice behind the relocating shards, which wait concurrently, so a
    /// re-evaluation extends by all the remaining grace even when nothing was saved. With a pending shard, nothing saved means no
    /// extension: 10s grace, 4 shards on the source gives each 2500ms; 2000ms later the fresh share is 2000ms, below what was budgeted.
    public void testReevaluationExtendsByRemainingGraceOnlyWhenNoShardIsPending() {
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

            final var previous = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(2500),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                2500.0
            );
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            // 4 shards on the source in both cases, 1 of them relocating to targetNodeId
            IntStream.of(0, 1).forEach(pendingShards -> {
                final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                    4 - pendingShards,
                    1,
                    index,
                    sourceNodeId,
                    targetNodeId,
                    startedAtMillis,
                    pendingShards
                );
                final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                    .shardRoutingTable(new ShardId(index, 0))
                    .shardsWithState(RELOCATING)
                    .getFirst()
                    .getTargetRelocatingShard();
                final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, previous);
                assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
                assertThat(plan.perShardShareMs(), equalTo(2000.0));
                assertThat("pending shards: " + pendingShards, plan.timeout().millis(), equalTo(pendingShards == 0 ? 8000L : 0L));
            });
        }
    }

    public void testReevaluationIsComputedLikeAFirstPlanAfterAnyPlanWithoutShare() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final long graceMs = 10_000;
            final long elapsedMs = 2_000;
            final long remainingMs = graceMs - elapsedMs;
            final int relocatingShards = 3;
            final int relocatingShardsToTarget = 1;
            final int pendingShards = 1;
            // the whole warming cache is still to be filled
            final long cacheSize = 1_000L;
            final Settings settings = Settings.builder()
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(),
                    TimeValue.timeValueMillis(graceMs)
                )
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING.getKey(), 1.0)
                .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
                .build();
            final var service = newCalculationService(threadPool, settings, cacheSize);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocatingShards,
                relocatingShardsToTarget,
                index,
                "source-node",
                "target-node",
                startedAtMillis,
                pendingShards
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + elapsedMs);

            final var slice = TimeValue.timeValueSeconds(1);
            final List<SearchRecoveryTimeout> computedFromScratch = List.of(
                new SearchRecoveryTimeout(slice, TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE),
                new SearchRecoveryTimeout(slice, TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME),
                new SearchRecoveryTimeout(slice, TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN)
            );
            for (final var previous : computedFromScratch) {
                final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), cacheSize, previous);
                final var description = previous.timeoutContext().name();
                assertThat(description, plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME));
                // all the remaining time, as the data-volume heuristic gives the whole cache to the only relocation to the target
                assertThat(description, plan.timeout().millis(), equalTo(remainingMs));
            }
        }
    }

    /// The time saved by shards that finished early is handed out for every shard that relocates concurrently, bounded by the target's
    /// incoming recovery slots.
    public void testReevaluationExtensionScalesByConcurrentRelocations() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final int maxConcurrentRelocationRecoveries = randomIntBetween(1, 8);
            // chosen so that all the shares below are whole milliseconds
            final long graceMs = 12_000;
            final long elapsedMs = 600;
            // the previous plan was computed while one more shard was still on the source, so it budgeted a smaller share per shard
            final int relocatingShards = 2;
            final int pendingShards = 1;
            final int shardsWhenPreviousPlanWasComputed = relocatingShards + pendingShards + 1;
            final long previousShareMs = graceMs / shardsWhenPreviousPlanWasComputed;
            final Settings settings = Settings.builder()
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(),
                    TimeValue.timeValueMillis(graceMs)
                )
                .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
                .put(
                    DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    maxConcurrentRelocationRecoveries
                )
                .build();
            final var service = newCalculationService(threadPool, settings, 0L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);
            final long startedAtMillis = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final var previous = new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(previousShareMs),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                previousShareMs
            );

            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + elapsedMs);
            final ClusterState state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocatingShards,
                relocatingShards,
                index,
                "source-node",
                "target-node",
                startedAtMillis,
                pendingShards
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, 0))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, previous);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE));
            // each shard still on the source gets an equal share of the remaining grace, of which only the part above the previous share is
            // new, and it is handed out for every shard relocating concurrently, however many slots the target has
            final long remainingMs = graceMs - elapsedMs;
            final long savedPerShardMs = remainingMs / (relocatingShards + pendingShards) - previousShareMs;
            assertThat(plan.timeout().millis(), equalTo(savedPerShardMs * Math.min(relocatingShards, maxConcurrentRelocationRecoveries)));
        }
    }

    public void testEqualShareTimeoutScalesByMaxConcurrentRelocationRecoveries() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final int maxConcurrentRelocationRecoveries = randomIntBetween(1, 8);
            final var settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 0.5)
                .put(
                    DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    maxConcurrentRelocationRecoveries
                )
                .build();
            final var service = newCalculationService(threadPool, settings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);

            final var index = new Index("idx", randomUUID());
            final int relocationsToTarget = 4;
            final var state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocationsToTarget,
                relocationsToTarget,
                index,
                "source-node",
                "target-node",
                threadPool.absoluteTimeInMillis()
            );

            // advance 2000ms into the 10s grace, remaining = 8000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(0, relocationsToTarget - 1)))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
            // grace cap=10s and factor=0.5, so equalShare = 8000 / 4 × 0.5 = 1000ms per shard, which keeps the timeout below `remaining`
            final long expectedEqualShareMs = 1000L;
            assertTrue(plan.awaitWarming());
            assertThat(
                plan.timeout().millis(),
                equalTo(expectedEqualShareMs * Math.min(relocationsToTarget, maxConcurrentRelocationRecoveries))
            );
        }
    }

    public void testTimeoutFollowsIncomingRecoveryThrottleSettingsUpdates() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            final var settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 0.5)
                .build();
            final var clusterSettings = newClusterSettings(settings);
            final var service = newCalculationService(threadPool, clusterSettings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);

            final var index = new Index("idx", randomUUID());
            final int relocationsToTarget = 4;
            final var state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocationsToTarget,
                relocationsToTarget,
                index,
                "source-node",
                "target-node",
                threadPool.absoluteTimeInMillis()
            );

            // advance 2000ms into the 10s grace, remaining = 8000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(0, relocationsToTarget - 1)))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            // equalShare = 8000 / 4 × 0.5 = 1000ms per shard
            final long expectedEqualShareMs = 1000L;
            // Unthrottled by default, so all relocations to the target are assumed to run concurrently.
            assertThat(
                service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null).timeout().millis(),
                equalTo(expectedEqualShareMs * relocationsToTarget)
            );

            // 2 incoming recoveries, of which 50% may be relocations, so a single relocation slot.
            clusterSettings.applySettings(
                Settings.builder()
                    .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 2)
                    .put(
                        DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_INCOMING_RECOVERIES_MAX_RELOCATION_PROPORTION_SETTING.getKey(),
                        "50%"
                    )
                    .build()
            );
            assertThat(
                service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null).timeout().millis(),
                equalTo(expectedEqualShareMs)
            );

            // Back to unthrottled.
            clusterSettings.applySettings(Settings.EMPTY);
            assertThat(
                service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null).timeout().millis(),
                equalTo(expectedEqualShareMs * relocationsToTarget)
            );
        }
    }

    public void testDataVolumeTimeoutScalesByMaxConcurrentRelocationRecoveries() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            // grace cap=10s, factor=0.1 keeps equal-share (8000 / 3 × 0.1 ~ 267ms), which will be below data-volume
            final int maxConcurrentRelocationRecoveries = randomIntBetween(1, 6);
            final var settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 0.1)
                .put(
                    DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    maxConcurrentRelocationRecoveries
                )
                .build();
            // cacheSize=1000, default cacheRatio=0.5 so warmingCacheBytes = 500
            final var service = newCalculationService(threadPool, settings, 1000L);

            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);

            final var index = new Index("idx", randomUUID());
            final int relocationsToTarget = 3;
            final var state = clusterStateSearchShardsRelocatingFromShuttingDownSource(
                relocationsToTarget,
                relocationsToTarget,
                index,
                "source-node",
                "target-node",
                threadPool.absoluteTimeInMillis()
            );

            // advance 2000ms into the 10s grace, remaining = 8000ms
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final Map<BlobFile, WarmTarget> endTargetsToWarm = Map.of(
                new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)),
                WarmTarget.withUnknownTimestamp(100L, randomLongBetween(100L, 1_000L))
            );
            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(0, relocationsToTarget - 1)))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();

            final var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), totalBytesToWarm(endTargetsToWarm), null);
            final int expectedConcurrentRelocations = Math.min(relocationsToTarget, maxConcurrentRelocationRecoveries);
            // totalBytesToWarm=100, so dataVolume = (100 / 500.0) × 8000 = 1600ms per shard, which keeps the timeout below `remaining`
            final long expectedDataVolumeMs = 1600L;
            assertTrue(plan.awaitWarming());
            assertThat(plan.timeout().millis(), equalTo(expectedDataVolumeMs * expectedConcurrentRelocations));
        }
    }

    public void testTimeoutSharesMaxConcurrentRelocationRecoveriesProportionallyBetweenShuttingDownSources() {
        try (
            var threadPool = new FakeTimeThreadPool(
                getTestName(),
                randomNonNegativeLong() / 2,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            // grace cap=10s and factor=0.5. Each source divides the remaining time by its own shard count.
            final var settings = Settings.builder()
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING.getKey(), 0.5)
                .build();
            final long shutdownCurrentTimeMs = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs);

            final var index = new Index("idx", randomUUID());
            final String sourceNodeId = "source-node";
            final String otherSourceNodeId = "other-shutting-down-node";
            final int numShards = 5;
            final int numShardsToTarget = 2;
            final int numShardsToOtherTarget = 1;
            final int numShardsOnOtherSource = 4;
            final int numShardsFromOtherSourceToTarget = 3;
            final int numShardsFromRestartingSource = 3;
            final long startedAtMillis = threadPool.absoluteTimeInMillis();
            final var state = clusterStateSearchShardsRelocatingFromMultipleSources(
                numShards,
                numShardsToTarget,
                numShardsToOtherTarget,
                numShardsOnOtherSource,
                numShardsFromOtherSourceToTarget,
                numShardsFromRestartingSource,
                index,
                sourceNodeId,
                otherSourceNodeId,
                "restarting-source-node",
                "target-node",
                "other-target-node",
                startedAtMillis,
                startedAtMillis + 1000
            );

            // Advance 2000ms into the first source's 10s grace. The other source has 9000ms remaining.
            threadPool.setCurrentTimeInMillis(shutdownCurrentTimeMs + 2000);

            final ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(new ShardId(index, randomIntBetween(0, numShardsToTarget - 1)))
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            assertThat(self.relocatingNodeId(), equalTo(sourceNodeId));

            final var other = state.routingTable(DEFAULT_PROJECT_ID)
                .shardRoutingTable(
                    new ShardId(index, randomIntBetween(numShardsToTarget, numShardsToTarget + numShardsFromOtherSourceToTarget - 1))
                )
                .shardsWithState(RELOCATING)
                .getFirst()
                .getTargetRelocatingShard();
            assertThat(other.relocatingNodeId(), equalTo(otherSourceNodeId));

            // Columns: { cap, source equal-share timeout, other source equal-share timeout, source data-volume timeout,
            // other source data-volume timeout}.
            // Equal-share bases are 8000 / 5 * 0.5 = 800ms and 9000 / 4 * 0.5 = 1125ms.
            // Warming 100 bytes of a 500-byte warming cache gives data-volume bases of 1600ms and 1800ms respectively.
            final long[][] cases = {
                { 1, 320, 675, 640, 1080 },
                { 3, 960, 2025, 1920, 3240 },
                { 5, 1600, 3375, 3200, 5400 },
                { Integer.MAX_VALUE, 1600, 3375, 3200, 5400 } };
            for (final var testCase : cases) {
                final var service = newCalculationService(
                    threadPool,
                    Settings.builder()
                        .put(settings)
                        .put(
                            DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                            testCase[0]
                        )
                        .build(),
                    1000L
                );
                assertThat(service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null).timeout().millis(), equalTo(testCase[1]));
                assertThat(service.searchRecoveryTimeout(state, mockIndexShard(other), 0L, null).timeout().millis(), equalTo(testCase[2]));
                assertThat(service.searchRecoveryTimeout(state, mockIndexShard(self), 100L, null).timeout().millis(), equalTo(testCase[3]));
                assertThat(
                    service.searchRecoveryTimeout(state, mockIndexShard(other), 100L, null).timeout().millis(),
                    equalTo(testCase[4])
                );
            }
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
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
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
            var plan = service.searchRecoveryTimeout(state, mockIndexShard(self), 0L, null);
            assertThat(plan.awaitWarming(), is(true));
            assertThat(
                plan.timeout(),
                equalTo(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING.getDefault(Settings.EMPTY))
            );
        }
    }
}
