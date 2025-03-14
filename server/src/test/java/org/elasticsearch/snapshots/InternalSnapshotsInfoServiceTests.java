/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.elasticsearch.snapshots.InternalSnapshotsInfoService.INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalSnapshotsInfoServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private RerouteService rerouteService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        repositoriesService = mock(RepositoriesService.class);
        rerouteService = (reason, priority, listener) -> listener.onResponse(null);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        final boolean terminated = terminate(threadPool);
        assert terminated;
        clusterService.close();
    }

    public void testSnapshotShardSizes() throws Exception {
        final int maxConcurrentFetches = randomIntBetween(1, 10);

        final int numberOfShards = randomIntBetween(1, 50);
        final CountDownLatch rerouteLatch = new CountDownLatch(numberOfShards);
        final RerouteService rerouteService = (reason, priority, listener) -> {
            listener.onResponse(null);
            assertThat(rerouteLatch.getCount(), greaterThanOrEqualTo(0L));
            rerouteLatch.countDown();
        };

        final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
            Settings.builder().put(INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.getKey(), maxConcurrentFetches).build(),
            clusterService,
            repositoriesService,
            () -> rerouteService
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long[] expectedShardSizes = new long[numberOfShards];
        for (int i = 0; i < expectedShardSizes.length; i++) {
            expectedShardSizes[i] = randomNonNegativeLong();
        }

        final AtomicInteger getShardSnapshotStatusCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final Repository mockRepository = new FilterRepository(mock(Repository.class)) {
            @Override
            public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
                assertThat(indexId.getName(), equalTo(indexName));
                assertThat(shardId.id(), allOf(greaterThanOrEqualTo(0), lessThan(numberOfShards)));
                safeAwait(latch);
                getShardSnapshotStatusCount.incrementAndGet();
                return IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, expectedShardSizes[shardId.id()], null);
            }
        };
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        applyClusterState("add-unassigned-shards", clusterState -> addUnassignedShards(clusterState, indexName, numberOfShards));
        waitForMaxActiveGenericThreads(Math.min(numberOfShards, maxConcurrentFetches));

        if (randomBoolean()) {
            applyClusterState(
                "reapply-last-cluster-state-to-check-deduplication-works",
                state -> ClusterState.builder(state).incrementVersion().build()
            );
        }

        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes(), equalTo(numberOfShards));
        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes(), equalTo(0));

        latch.countDown();

        assertTrue(rerouteLatch.await(30L, TimeUnit.SECONDS));
        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes(), equalTo(numberOfShards));
        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes(), equalTo(0));
        assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes(), equalTo(0));
        assertThat(getShardSnapshotStatusCount.get(), equalTo(numberOfShards));

        final SnapshotShardSizeInfo snapshotShardSizeInfo = snapshotsInfoService.snapshotShardSizes();
        for (int i = 0; i < numberOfShards; i++) {
            final ShardRouting shardRouting = clusterService.state().routingTable().index(indexName).shard(i).primaryShard();
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting), equalTo(expectedShardSizes[i]));
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting, Long.MIN_VALUE), equalTo(expectedShardSizes[i]));
        }
    }

    public void testErroneousSnapshotShardSizes() throws Exception {
        final int maxShardsToCreate = scaledRandomIntBetween(10, 500);

        final PlainActionFuture<Void> waitForAllReroutesProcessed = new PlainActionFuture<>();
        final CountDown reroutes = new CountDown(maxShardsToCreate);
        final RerouteService rerouteService = (reason, priority, listener) -> {
            try {
                listener.onResponse(null);
            } finally {
                if (reroutes.countDown()) {
                    waitForAllReroutesProcessed.onResponse(null);
                }
            }
        };

        final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
            Settings.builder().put(INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.getKey(), randomIntBetween(1, 10)).build(),
            clusterService,
            repositoriesService,
            () -> rerouteService
        );

        final Map<InternalSnapshotsInfoService.SnapshotShard, Long> results = new ConcurrentHashMap<>();
        final Repository mockRepository = new FilterRepository(mock(Repository.class)) {
            @Override
            public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
                final InternalSnapshotsInfoService.SnapshotShard snapshotShard = new InternalSnapshotsInfoService.SnapshotShard(
                    new Snapshot("_repo", snapshotId),
                    indexId,
                    shardId
                );
                if (randomBoolean()) {
                    results.put(snapshotShard, Long.MIN_VALUE);
                    throw new SnapshotException(snapshotShard.snapshot(), "simulated");
                } else {
                    final long shardSize = randomNonNegativeLong();
                    results.put(snapshotShard, shardSize);
                    return IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, shardSize, null);
                }
            }
        };
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        final Thread addSnapshotRestoreIndicesThread = new Thread(() -> {
            int remainingShards = maxShardsToCreate;
            while (remainingShards > 0) {
                final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                final int numberOfShards = randomIntBetween(1, remainingShards);
                try {
                    applyClusterState(
                        "add-more-unassigned-shards-for-" + indexName,
                        clusterState -> addUnassignedShards(clusterState, indexName, numberOfShards)
                    );
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    remainingShards -= numberOfShards;
                }
            }
        });
        addSnapshotRestoreIndicesThread.start();
        addSnapshotRestoreIndicesThread.join();

        final Predicate<Long> failedSnapshotShardSizeRetrieval = shardSize -> shardSize == Long.MIN_VALUE;
        assertBusy(() -> {
            assertThat(
                snapshotsInfoService.numberOfKnownSnapshotShardSizes(),
                equalTo((int) results.values().stream().filter(Predicate.not(failedSnapshotShardSizeRetrieval)).count())
            );
            assertThat(
                snapshotsInfoService.numberOfFailedSnapshotShardSizes(),
                equalTo((int) results.values().stream().filter(failedSnapshotShardSizeRetrieval).count())
            );
            assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes(), equalTo(0));
        });

        final SnapshotShardSizeInfo snapshotShardSizeInfo = snapshotsInfoService.snapshotShardSizes();
        for (Map.Entry<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShard : results.entrySet()) {
            final ShardId shardId = snapshotShard.getKey().shardId();
            final ShardRouting shardRouting = clusterService.state()
                .routingTable()
                .index(shardId.getIndexName())
                .shard(shardId.id())
                .primaryShard();
            assertThat(shardRouting, notNullValue());

            final boolean success = failedSnapshotShardSizeRetrieval.test(snapshotShard.getValue()) == false;
            assertThat(
                snapshotShardSizeInfo.getShardSize(shardRouting),
                success ? equalTo(results.get(snapshotShard.getKey())) : equalTo(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            );
            final long defaultValue = randomNonNegativeLong();
            assertThat(
                snapshotShardSizeInfo.getShardSize(shardRouting, defaultValue),
                success ? equalTo(results.get(snapshotShard.getKey())) : equalTo(defaultValue)
            );
        }

        waitForAllReroutesProcessed.get(60L, TimeUnit.SECONDS);
        assertThat("Expecting all snapshot shard size fetches to provide a size", results.size(), equalTo(maxShardsToCreate));
        assertTrue("Expecting all snapshot shard size fetches to execute a Reroute", reroutes.isCountedDown());
    }

    public void testNoLongerMaster() throws Exception {
        final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
            Settings.EMPTY,
            clusterService,
            repositoriesService,
            () -> rerouteService
        );

        final Repository mockRepository = new FilterRepository(mock(Repository.class)) {
            @Override
            public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
                return IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, randomNonNegativeLong(), null);
            }
        };
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            final int nbShards = randomIntBetween(1, 5);
            applyClusterState(
                "restore-indices-when-master-" + indexName,
                clusterState -> addUnassignedShards(clusterState, indexName, nbShards)
            );
        }

        applyClusterState("demote-current-master", this::demoteMasterNode);

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            final int nbShards = randomIntBetween(1, 5);
            applyClusterState(
                "restore-indices-when-no-longer-master-" + indexName,
                clusterState -> addUnassignedShards(clusterState, indexName, nbShards)
            );
        }

        assertBusy(() -> {
            assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes(), equalTo(0));
            assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes(), equalTo(0));
            assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes(), equalTo(0));
        });
    }

    public void testCleanUpSnapshotShardSizes() throws Exception {
        final Repository mockRepository = new FilterRepository(mock(Repository.class)) {
            @Override
            public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
                if (randomBoolean()) {
                    throw new SnapshotException(new Snapshot("_repo", snapshotId), "simulated");
                } else {
                    return IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, randomNonNegativeLong(), null);
                }
            }
        };
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
            Settings.EMPTY,
            clusterService,
            repositoriesService,
            () -> rerouteService
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int nbShards = randomIntBetween(1, 10);

        applyClusterState(
            "new snapshot restore for index " + indexName,
            clusterState -> addUnassignedShards(clusterState, indexName, nbShards)
        );

        // waiting for snapshot shard size fetches to be executed, as we want to verify that they are cleaned up
        assertBusy(
            () -> assertThat(
                snapshotsInfoService.numberOfFailedSnapshotShardSizes() + snapshotsInfoService.numberOfKnownSnapshotShardSizes(),
                equalTo(nbShards)
            )
        );

        if (randomBoolean()) {
            // simulate initialization and start of the shards
            final AllocationService allocationService = ESAllocationTestCase.createAllocationService(
                Settings.builder()
                    .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), nbShards)
                    .put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), nbShards)
                    .put("cluster.routing.allocation.type", "balanced") // TODO fix for desired_balance
                    .build(),
                snapshotsInfoService
            );
            assertCriticalWarnings(
                "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the breaking changes documentation for the next major version."
            );
            applyClusterState(
                "starting shards for " + indexName,
                clusterState -> ESAllocationTestCase.startInitializingShardsAndReroute(allocationService, clusterState, indexName)
            );
            RoutingNodes routingNodes = clusterService.state().getRoutingNodes();
            assertTrue(RoutingNodesHelper.shardsWithState(routingNodes, ShardRoutingState.UNASSIGNED).isEmpty());

        } else {
            // simulate deletion of the index
            applyClusterState("delete index " + indexName, clusterState -> deleteIndex(clusterState, indexName));
            assertFalse(clusterService.state().metadata().getProject().hasIndex(indexName));
        }

        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes(), equalTo(0));
        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes(), equalTo(0));
        assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes(), equalTo(0));
    }

    private void applyClusterState(final String reason, final Function<ClusterState, ClusterState> applier) {
        safeAwait(
            (ActionListener<Void> listener) -> clusterService.getClusterApplierService()
                .onNewClusterState(reason, () -> applier.apply(clusterService.state()), listener)
        );
    }

    private void waitForMaxActiveGenericThreads(final int nbActive) throws Exception {
        assertBusy(() -> {
            final ThreadPoolStats threadPoolStats = clusterService.getClusterApplierService().threadPool().stats();
            ThreadPoolStats.Stats generic = null;
            for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
                if (ThreadPool.Names.GENERIC.equals(threadPoolStat.name())) {
                    generic = threadPoolStat;
                }
            }
            assertThat(generic, notNullValue());
            assertThat(generic.active(), equalTo(nbActive));
        }, 30L, TimeUnit.SECONDS);
    }

    private ClusterState addUnassignedShards(final ClusterState currentState, String indexName, int numberOfShards) {
        assertThat(currentState.metadata().getProject().hasIndex(indexName), is(false));

        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), numberOfShards, 0).put(SETTING_CREATION_DATE, System.currentTimeMillis()));

        for (int i = 0; i < numberOfShards; i++) {
            indexMetadataBuilder.putInSyncAllocationIds(i, Collections.singleton(AllocationId.newInitializing().getId()));
        }

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
            .put(indexMetadataBuilder.build(), true)
            .generateClusterUuidIfNeeded();

        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(random()),
            new Snapshot("_repo", new SnapshotId(randomAlphaOfLength(5), UUIDs.randomBase64UUID(random()))),
            IndexVersion.current(),
            new IndexId(indexName, UUIDs.randomBase64UUID(random()))
        );

        final IndexMetadata indexMetadata = metadata.get(indexName);
        final Index index = indexMetadata.getIndex();

        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());
        routingTable.add(
            IndexRoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, index)
                .initializeAsNewRestore(indexMetadata, recoverySource, new HashSet<>())
                .build()
        );

        final RestoreInProgress.Builder restores = new RestoreInProgress.Builder(RestoreInProgress.get(currentState));
        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = new HashMap<>();
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            shards.put(new ShardId(index, i), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
        }

        restores.add(
            new RestoreInProgress.Entry(
                recoverySource.restoreUUID(),
                recoverySource.snapshot(),
                RestoreInProgress.State.INIT,
                false,
                Collections.singletonList(indexName),
                shards
            )
        );

        return ClusterState.builder(currentState)
            .putCustom(RestoreInProgress.TYPE, restores.build())
            .routingTable(routingTable.build())
            .metadata(metadata)
            .build();
    }

    private ClusterState demoteMasterNode(final ClusterState currentState) {
        final DiscoveryNode node = DiscoveryNodeUtils.create("other");
        assertThat(currentState.nodes().get(node.getId()), nullValue());

        return ClusterState.builder(currentState)
            .nodes(DiscoveryNodes.builder(currentState.nodes()).add(node).masterNodeId(node.getId()))
            .build();
    }

    private ClusterState deleteIndex(final ClusterState currentState, final String indexName) {
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).remove(indexName))
            .routingTable(RoutingTable.builder(currentState.routingTable()).remove(indexName).build())
            .build();
    }
}
