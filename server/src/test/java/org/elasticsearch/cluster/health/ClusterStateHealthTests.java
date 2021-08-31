/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.health;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterStateHealthTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("ClusterStateHealthTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        CapturingTransport transport = new CapturingTransport();
        transportService = transport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testClusterHealthWaitsForClusterStateApplication() throws InterruptedException, ExecutionException {
        final CountDownLatch applyLatch = new CountDownLatch(1);
        final CountDownLatch listenerCalled = new CountDownLatch(1);

        setState(clusterService, ClusterState.builder(clusterService.state())
            .nodes(DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null)).build());

        clusterService.addStateApplier(event -> {
            listenerCalled.countDown();
            try {
                applyLatch.await();
            } catch (InterruptedException e) {
                logger.debug("interrupted", e);
            }
        });

        logger.info("--> submit task to restore master");
        ClusterState currentState = clusterService.getClusterApplierService().state();
        clusterService.getClusterApplierService().onNewClusterState("restore master",
            () -> ClusterState.builder(currentState)
                .nodes(DiscoveryNodes.builder(currentState.nodes()).masterNodeId(currentState.nodes().getLocalNodeId())).build(),
            e -> {});

        logger.info("--> waiting for listener to be called and cluster state being blocked");
        listenerCalled.await();

        TransportClusterHealthAction action = new TransportClusterHealthAction(transportService,
            clusterService, threadPool, new ActionFilters(new HashSet<>()), indexNameExpressionResolver,
            new AllocationService(null, new TestGatewayAllocator(), null, null, null));
        PlainActionFuture<ClusterHealthResponse> listener = new PlainActionFuture<>();
        action.execute(new ClusterHealthRequest().waitForGreenStatus(), listener);

        assertFalse(listener.isDone());

        logger.info("--> realising task to restore master");
        applyLatch.countDown();
        listener.get();
    }

    public void testClusterHealth() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = randomInt(4); i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata
                    .builder("test_" + Integer.toString(i))
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                                                .metadata(metadata)
                                                .routingTable(routingTable.build())
                                                .build();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            clusterState, IndicesOptions.strictExpand(), (String[]) null
        );
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        logger.info("cluster status: {}, expected {}", clusterStateHealth.getStatus(), counter.status());
        clusterStateHealth = maybeSerialize(clusterStateHealth);
        assertClusterHealth(clusterStateHealth, counter);
    }

    public void testClusterHealthOnIndexCreation() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        final List<ClusterState> clusterStates = simulateIndexCreationStates(indexName, false);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is always YELLOW, up until the last state where it should be GREEN
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            if (i < clusterStates.size() - 1) {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            } else {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            }
        }
    }

    public void testClusterHealthOnIndexCreationWithFailedAllocations() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        final List<ClusterState> clusterStates = simulateIndexCreationStates(indexName, true);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, which contains primary shard
            // failed allocations that should make the cluster health RED
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            if (i < clusterStates.size() - 1) {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            } else {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));
            }
        }
    }

    public void testClusterHealthOnClusterRecovery() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, false, false);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, when it turns GREEN
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            if (i < clusterStates.size() - 1) {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            } else {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            }
        }
    }

    public void testClusterHealthOnClusterRecoveryWithFailures() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, false, true);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, which contains primary shard
            // failed allocations that should make the cluster health RED
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            if (i < clusterStates.size() - 1) {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            } else {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));
            }
        }
    }

    public void testClusterHealthOnClusterRecoveryWithPreviousAllocationIds() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, true, false);
        for (int i = 0; i < clusterStates.size(); i++) {
            // because there were previous allocation ids, we should be RED until the primaries are started,
            // then move to YELLOW, and the last state should be GREEN when all shards have been started
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            if (i < clusterStates.size() - 1) {
                // if the inactive primaries are due solely to recovery (not failed allocation or previously being allocated),
                // then cluster health is YELLOW, otherwise RED
                if (primaryInactiveDueToRecovery(indexName, clusterState)) {
                    assertThat(health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
                } else {
                    assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));
                }
            } else {
                assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            }
        }
    }

    public void testClusterHealthOnClusterRecoveryWithPreviousAllocationIdsAndAllocationFailures() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        for (final ClusterState clusterState : simulateClusterRecoveryStates(indexName, true, true)) {
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices);
            // if the inactive primaries are due solely to recovery (not failed allocation or previously being allocated)
            // then cluster health is YELLOW, otherwise RED
            if (primaryInactiveDueToRecovery(indexName, clusterState)) {
                assertThat("clusterState is:\n" + clusterState, health.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            } else {
                assertThat("clusterState is:\n" + clusterState, health.getStatus(), equalTo(ClusterHealthStatus.RED));
            }
        }
    }

    ClusterStateHealth maybeSerialize(ClusterStateHealth clusterStateHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterStateHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterStateHealth = new ClusterStateHealth(in);
        }
        return clusterStateHealth;
    }

    private List<ClusterState> simulateIndexCreationStates(final String indexName, final boolean withPrimaryAllocationFailures) {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(1, numberOfShards);
        // initial index creation and new routing table info
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                                                         .settings(settings(Version.CURRENT)
                                                                       .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
                                                         .numberOfShards(numberOfShards)
                                                         .numberOfReplicas(numberOfReplicas)
                                                         .build();
        final Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
        final RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test_cluster"))
                                                .metadata(metadata)
                                                .routingTable(routingTable)
                                                .build();
        return generateClusterStates(clusterState, indexName, numberOfReplicas, withPrimaryAllocationFailures);
    }

    private List<ClusterState> simulateClusterRecoveryStates(final String indexName,
                                                             final boolean withPreviousAllocationIds,
                                                             final boolean withPrimaryAllocationFailures) {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(1, numberOfShards);
        // initial index creation and new routing table info
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                                                   .settings(settings(Version.CURRENT)
                                                                 .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
                                                   .numberOfShards(numberOfShards)
                                                   .numberOfReplicas(numberOfReplicas)
                                                   .state(IndexMetadata.State.OPEN)
                                                   .build();
        if (withPreviousAllocationIds) {
            final IndexMetadata.Builder idxMetaWithAllocationIds = IndexMetadata.builder(indexMetadata);
            boolean atLeastOne = false;
            for (int i = 0; i < numberOfShards; i++) {
                if (atLeastOne == false || randomBoolean()) {
                    idxMetaWithAllocationIds.putInSyncAllocationIds(i, Sets.newHashSet(UUIDs.randomBase64UUID()));
                    atLeastOne = true;
                }
            }
            indexMetadata = idxMetaWithAllocationIds.build();
        }
        final Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
        final RoutingTable routingTable = RoutingTable.builder().addAsRecovery(indexMetadata).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test_cluster"))
                                                .metadata(metadata)
                                                .routingTable(routingTable)
                                                .build();
        return generateClusterStates(clusterState, indexName, numberOfReplicas, withPrimaryAllocationFailures);
    }

    private List<ClusterState> generateClusterStates(final ClusterState originalClusterState,
                                                     final String indexName,
                                                     final int numberOfReplicas,
                                                     final boolean withPrimaryAllocationFailures) {
        // generate random node ids
        final Set<String> nodeIds = new HashSet<>();
        final int numNodes = randomIntBetween(numberOfReplicas + 1, 10);
        for (int i = 0; i < numNodes; i++) {
            nodeIds.add(randomAlphaOfLength(8));
        }

        final List<ClusterState> clusterStates = new ArrayList<>();
        clusterStates.add(originalClusterState);
        ClusterState clusterState = originalClusterState;

        // initialize primaries
        RoutingTable routingTable = originalClusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary()) {
                    newIndexRoutingTable.addShard(
                        shardRouting.initialize(randomFrom(nodeIds), null, shardRouting.getExpectedShardSize())
                    );
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        clusterStates.add(clusterState);

        // some primaries started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        ImmutableOpenIntMap.Builder<Set<String>> allocationIds = ImmutableOpenIntMap.<Set<String>>builder();
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary() && randomBoolean()) {
                    final ShardRouting newShardRouting = shardRouting.moveToStarted();
                    allocationIds.fPut(newShardRouting.getId(), Sets.newHashSet(newShardRouting.allocationId().getId()));
                    newIndexRoutingTable.addShard(newShardRouting);
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        IndexMetadata.Builder idxMetaBuilder = IndexMetadata.builder(clusterState.metadata().index(indexName));
        for (final IntObjectCursor<Set<String>> entry : allocationIds.build()) {
            idxMetaBuilder.putInSyncAllocationIds(entry.key, entry.value);
        }
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata()).put(idxMetaBuilder);
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadataBuilder).build();
        clusterStates.add(clusterState);

        if (withPrimaryAllocationFailures) {
            boolean alreadyFailedPrimary = false;
            // some primaries failed to allocate
            indexRoutingTable = routingTable.index(indexName);
            newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
                final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
                for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                    if (shardRouting.primary() && (shardRouting.started() == false || alreadyFailedPrimary == false)) {
                        newIndexRoutingTable.addShard(shardRouting.moveToUnassigned(
                            new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "unlucky shard")));
                        alreadyFailedPrimary = true;
                    } else {
                        newIndexRoutingTable.addShard(shardRouting);
                    }
                }
            }
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
            clusterStates.add(ClusterState.builder(clusterState).routingTable(routingTable).build());
            return clusterStates;
        }

        // all primaries started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        allocationIds = ImmutableOpenIntMap.<Set<String>>builder();
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary() && shardRouting.started() == false) {
                    final ShardRouting newShardRouting = shardRouting.moveToStarted();
                    allocationIds.fPut(newShardRouting.getId(), Sets.newHashSet(newShardRouting.allocationId().getId()));
                    newIndexRoutingTable.addShard(newShardRouting);
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        idxMetaBuilder = IndexMetadata.builder(clusterState.metadata().index(indexName));
        for (final IntObjectCursor<Set<String>> entry : allocationIds.build()) {
            idxMetaBuilder.putInSyncAllocationIds(entry.key, entry.value);
        }
        metadataBuilder = Metadata.builder(clusterState.metadata()).put(idxMetaBuilder);
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadataBuilder).build();
        clusterStates.add(clusterState);

        // initialize replicas
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
            Set<String> allocatedNodes = new HashSet<>();
            allocatedNodes.add(primaryNodeId);
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary() == false) {
                    // give the replica a different node id than the primary
                    String replicaNodeId = randomFrom(Sets.difference(nodeIds, allocatedNodes));
                    newIndexRoutingTable.addShard(shardRouting.initialize(replicaNodeId, null, shardRouting.getExpectedShardSize()));
                    allocatedNodes.add(replicaNodeId);
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        clusterStates.add(ClusterState.builder(clusterState).routingTable(routingTable).build());

        // some replicas started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary() == false && randomBoolean()) {
                    newIndexRoutingTable.addShard(shardRouting.moveToStarted());
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        clusterStates.add(ClusterState.builder(clusterState).routingTable(routingTable).build());

        // all replicas started
        boolean replicaStateChanged = false;
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (final ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary() == false && shardRouting.started() == false) {
                    newIndexRoutingTable.addShard(shardRouting.moveToStarted());
                    replicaStateChanged = true;
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        // all of the replicas may have moved to started in the previous phase already
        if (replicaStateChanged) {
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
            clusterStates.add(ClusterState.builder(clusterState).routingTable(routingTable).build());
        }

        return clusterStates;
    }

    // returns true if the inactive primaries in the index are only due to cluster recovery
    // (not because of allocation of existing shard or previously having allocation ids assigned)
    private boolean primaryInactiveDueToRecovery(final String indexName, final ClusterState clusterState) {
        for (final IntObjectCursor<IndexShardRoutingTable> shardRouting : clusterState.routingTable().index(indexName).shards()) {
            final ShardRouting primaryShard = shardRouting.value.primaryShard();
            if (primaryShard.active() == false) {
                if (clusterState.metadata().index(indexName).inSyncAllocationIds(shardRouting.key).isEmpty() == false) {
                    return false;
                }
                if (primaryShard.recoverySource() != null &&
                    primaryShard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    return false;
                }
                if (primaryShard.unassignedInfo().getNumFailedAllocations() > 0) {
                    return false;
                }
                if (primaryShard.unassignedInfo().getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    return false;
                }
            }
        }
        return true;
    }

    private void assertClusterHealth(ClusterStateHealth clusterStateHealth, RoutingTableGenerator.ShardCounter counter) {
        assertThat(clusterStateHealth.getStatus(), equalTo(counter.status()));
        assertThat(clusterStateHealth.getActiveShards(), equalTo(counter.active));
        assertThat(clusterStateHealth.getActivePrimaryShards(), equalTo(counter.primaryActive));
        assertThat(clusterStateHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(clusterStateHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(clusterStateHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(clusterStateHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }
}
