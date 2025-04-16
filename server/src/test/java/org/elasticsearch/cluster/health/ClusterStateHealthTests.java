/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
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
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
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

        var state = ClusterState.builder(clusterService.state()).nodes(clusterService.state().nodes().withMasterNodeId(null)).build();
        var projectId = state.metadata().projects().keySet().iterator().next();
        // Randomly add an extra project.
        if (randomBoolean()) {
            state = ClusterState.builder(state).putProjectMetadata(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
        }
        setState(clusterService, state);

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
        clusterService.getClusterApplierService()
            .onNewClusterState(
                "restore master",
                () -> ClusterState.builder(currentState)
                    .nodes(currentState.nodes().withMasterNodeId(currentState.nodes().getLocalNodeId()))
                    .incrementVersion()
                    .build(),
                ActionListener.noop()
            );

        logger.info("--> waiting for listener to be called and cluster state being blocked");
        listenerCalled.await();

        TransportClusterHealthAction action = new TransportClusterHealthAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(new HashSet<>()),
            indexNameExpressionResolver,
            new AllocationService(null, new TestGatewayAllocator(), null, null, null, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY),
            TestProjectResolvers.singleProject(projectId)
        );
        PlainActionFuture<ClusterHealthResponse> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(
            action,
            new CancellableTask(1, "direct", TransportClusterHealthAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of()),
            new ClusterHealthRequest(TEST_REQUEST_TIMEOUT).waitForGreenStatus(),
            listener
        );

        assertFalse(listener.isDone());

        logger.info("--> realising task to restore master");
        applyLatch.countDown();
        listener.get();
    }

    public void testClusterHealth() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
        for (int i = randomInt(4); i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + Integer.toString(i))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            project.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
            .build();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            clusterState.metadata().getProject(projectId),
            IndicesOptions.strictExpand(),
            (String[]) null
        );
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices, projectId);
        logger.info("cluster status: {}, expected {}", clusterStateHealth.getStatus(), counter.status());
        clusterStateHealth = maybeSerialize(clusterStateHealth);
        assertClusterHealth(clusterStateHealth, counter);
    }

    public void testClusterHealthOnIndexCreation() {
        final String indexName = "test-idx";
        final String[] indices = new String[] { indexName };
        var projectId = randomUniqueProjectId();
        final List<ClusterState> clusterStates = simulateIndexCreationStates(indexName, false, projectId);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is always YELLOW, up until the last state where it should be GREEN
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
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
        var projectId = randomUniqueProjectId();
        final List<ClusterState> clusterStates = simulateIndexCreationStates(indexName, true, projectId);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, which contains primary shard
            // failed allocations that should make the cluster health RED
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
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
        var projectId = randomUniqueProjectId();
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, false, false, projectId);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, when it turns GREEN
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
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
        var projectId = randomUniqueProjectId();
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, false, true, projectId);
        for (int i = 0; i < clusterStates.size(); i++) {
            // make sure cluster health is YELLOW up until the final cluster state, which contains primary shard
            // failed allocations that should make the cluster health RED
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
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
        var projectId = randomUniqueProjectId();
        final List<ClusterState> clusterStates = simulateClusterRecoveryStates(indexName, true, false, projectId);
        for (int i = 0; i < clusterStates.size(); i++) {
            // because there were previous allocation ids, we should be RED until the primaries are started,
            // then move to YELLOW, and the last state should be GREEN when all shards have been started
            final ClusterState clusterState = clusterStates.get(i);
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
            if (i < clusterStates.size() - 1) {
                // if the inactive primaries are due solely to recovery (not failed allocation or previously being allocated),
                // then cluster health is YELLOW, otherwise RED
                if (primaryInactiveDueToRecovery(indexName, clusterState, projectId)) {
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
        var projectId = randomUniqueProjectId();
        for (final ClusterState clusterState : simulateClusterRecoveryStates(indexName, true, true, projectId)) {
            final ClusterStateHealth health = new ClusterStateHealth(clusterState, indices, projectId);
            // if the inactive primaries are due solely to recovery (not failed allocation or previously being allocated)
            // then cluster health is YELLOW, otherwise RED
            if (primaryInactiveDueToRecovery(indexName, clusterState, projectId)) {
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

    private List<ClusterState> simulateIndexCreationStates(
        final String indexName,
        final boolean withPrimaryAllocationFailures,
        ProjectId projectId
    ) {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(1, numberOfShards);
        // initial index creation and new routing table info
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();
        final var mdBuilder = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, true).build());
        final var rtBuilder = GlobalRoutingTable.builder()
            .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata));
        final int nrOfProjects = randomIntBetween(0, 5);
        for (int i = 0; i < nrOfProjects; i++) {
            var id = randomUniqueProjectId();
            mdBuilder.put(ProjectMetadata.builder(id).build());
            rtBuilder.put(id, RoutingTable.EMPTY_ROUTING_TABLE);
        }

        ClusterState clusterState = ClusterState.builder(new ClusterName("test_cluster"))
            .metadata(mdBuilder.build())
            .routingTable(rtBuilder.build())
            .build();
        return generateClusterStates(clusterState, indexName, numberOfReplicas, withPrimaryAllocationFailures, projectId);
    }

    private List<ClusterState> simulateClusterRecoveryStates(
        final String indexName,
        final boolean withPreviousAllocationIds,
        final boolean withPrimaryAllocationFailures,
        final ProjectId projectId
    ) {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(1, numberOfShards);
        // initial index creation and new routing table info
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
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
        final var mdBuilder = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, true).build());
        final var rtBuilder = GlobalRoutingTable.builder()
            .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata));
        final int nrOfProjects = randomIntBetween(0, 5);
        for (int i = 0; i < nrOfProjects; i++) {
            var id = randomUniqueProjectId();
            mdBuilder.put(ProjectMetadata.builder(id).build());
            rtBuilder.put(id, RoutingTable.EMPTY_ROUTING_TABLE);
        }

        ClusterState clusterState = ClusterState.builder(new ClusterName("test_cluster"))
            .metadata(mdBuilder.build())
            .routingTable(rtBuilder.build())
            .build();
        return generateClusterStates(clusterState, indexName, numberOfReplicas, withPrimaryAllocationFailures, projectId);
    }

    private List<ClusterState> generateClusterStates(
        final ClusterState originalClusterState,
        final String indexName,
        final int numberOfReplicas,
        final boolean withPrimaryAllocationFailures,
        ProjectId projectId
    ) {
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
        RoutingTable routingTable = originalClusterState.routingTable(projectId);
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.primary()) {
                    newIndexRoutingTable.addShard(shardRouting.initialize(randomFrom(nodeIds), null, shardRouting.getExpectedShardSize()));
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
            .build();
        clusterStates.add(clusterState);

        // some primaries started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        Map<Integer, Set<String>> allocationIds = new HashMap<>();
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.primary() && randomBoolean()) {
                    final ShardRouting newShardRouting = shardRouting.moveToStarted(UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    allocationIds.put(newShardRouting.getId(), Set.of(newShardRouting.allocationId().getId()));
                    newIndexRoutingTable.addShard(newShardRouting);
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        final IndexMetadata.Builder idxMetaBuilder = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index(indexName));
        allocationIds.forEach(idxMetaBuilder::putInSyncAllocationIds);
        var projectBuilder = ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(idxMetaBuilder);
        clusterState = ClusterState.builder(clusterState)
            .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
            .putProjectMetadata(projectBuilder)
            .build();
        clusterStates.add(clusterState);

        if (withPrimaryAllocationFailures) {
            boolean alreadyFailedPrimary = false;
            // some primaries failed to allocate
            indexRoutingTable = routingTable.index(indexName);
            newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                    ShardRouting shardRouting = shardRoutingTable.shard(copy);
                    if (shardRouting.primary() && (shardRouting.started() == false || alreadyFailedPrimary == false)) {
                        newIndexRoutingTable.addShard(
                            shardRouting.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "unlucky shard"))
                        );
                        alreadyFailedPrimary = true;
                    } else {
                        newIndexRoutingTable.addShard(shardRouting);
                    }
                }
            }
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
            clusterStates.add(
                ClusterState.builder(clusterState)
                    .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
                    .build()
            );
            return clusterStates;
        }

        // all primaries started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        allocationIds = new HashMap<>();
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.primary() && shardRouting.started() == false) {
                    final ShardRouting newShardRouting = shardRouting.moveToStarted(UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    allocationIds.put(newShardRouting.getId(), Set.of(newShardRouting.allocationId().getId()));
                    newIndexRoutingTable.addShard(newShardRouting);
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        final IndexMetadata.Builder idxMetaBuilder2 = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index(indexName));
        allocationIds.forEach(idxMetaBuilder2::putInSyncAllocationIds);
        projectBuilder = ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(idxMetaBuilder2);
        clusterState = ClusterState.builder(clusterState)
            .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
            .putProjectMetadata(projectBuilder)
            .build();
        clusterStates.add(clusterState);

        // initialize replicas
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
            Set<String> allocatedNodes = new HashSet<>();
            allocatedNodes.add(primaryNodeId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
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
        clusterStates.add(
            ClusterState.builder(clusterState)
                .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
                .build()
        );

        // some replicas started
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.primary() == false && randomBoolean()) {
                    newIndexRoutingTable.addShard(shardRouting.moveToStarted(UNAVAILABLE_EXPECTED_SHARD_SIZE));
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        clusterStates.add(
            ClusterState.builder(clusterState)
                .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
                .build()
        );

        // all replicas started
        boolean replicaStateChanged = false;
        indexRoutingTable = routingTable.index(indexName);
        newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.primary() == false && shardRouting.started() == false) {
                    newIndexRoutingTable.addShard(shardRouting.moveToStarted(UNAVAILABLE_EXPECTED_SHARD_SIZE));
                    replicaStateChanged = true;
                } else {
                    newIndexRoutingTable.addShard(shardRouting);
                }
            }
        }
        // all of the replicas may have moved to started in the previous phase already
        if (replicaStateChanged) {
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
            clusterStates.add(
                ClusterState.builder(clusterState)
                    .routingTable(GlobalRoutingTable.builder(clusterState.globalRoutingTable()).put(projectId, routingTable).build())
                    .build()
            );
        }

        return clusterStates;
    }

    // returns true if the inactive primaries in the index are only due to cluster recovery
    // (not because of allocation of existing shard or previously having allocation ids assigned)
    private boolean primaryInactiveDueToRecovery(final String indexName, final ClusterState clusterState, final ProjectId projectId) {
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable(projectId).index(indexName);
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRouting = indexRoutingTable.shard(i);
            final ShardRouting primaryShard = shardRouting.primaryShard();
            if (primaryShard.active() == false) {
                if (clusterState.metadata()
                    .getProject(projectId)
                    .index(indexName)
                    .inSyncAllocationIds(shardRouting.shardId().id())
                    .isEmpty() == false) {
                    return false;
                }
                if (primaryShard.recoverySource() != null
                    && primaryShard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    return false;
                }
                if (primaryShard.unassignedInfo().failedAllocations() > 0) {
                    return false;
                }
                if (primaryShard.unassignedInfo().lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
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
        assertThat(clusterStateHealth.getUnassignedPrimaryShards(), equalTo(counter.unassignedPrimary));
        assertThat(clusterStateHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }
}
