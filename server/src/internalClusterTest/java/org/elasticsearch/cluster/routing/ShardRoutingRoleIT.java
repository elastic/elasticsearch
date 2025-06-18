/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@SuppressWarnings("resource")
public class ShardRoutingRoleIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ShardRoutingRoleIT.class);

    public static class TestPlugin extends Plugin implements ClusterPlugin, EnginePlugin, ActionPlugin {

        volatile int numIndexingCopies = 1;
        static final String NODE_ATTR_UNPROMOTABLE_ONLY = "unpromotableonly";

        @Override
        public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
            return new ShardRoutingRoleStrategy() {
                @Override
                public ShardRouting.Role newReplicaRole() {
                    return ShardRouting.Role.SEARCH_ONLY;
                }

                @Override
                public ShardRouting.Role newEmptyRole(int copyIndex) {
                    assert 0 < numIndexingCopies;
                    return copyIndex < numIndexingCopies ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
                }
            };
        }

        // This is implemented in stateless, but for the tests we need to provide a simple implementation
        public static class TransportTestUnpromotableRelocationAction extends TransportAction<
            StatelessUnpromotableRelocationAction.Request,
            ActionResponse.Empty> {

            private final IndicesService indicesService;
            private final PeerRecoveryTargetService peerRecoveryTargetService;

            @Inject
            public TransportTestUnpromotableRelocationAction(
                ActionFilters actionFilters,
                IndicesService indicesService,
                PeerRecoveryTargetService peerRecoveryTargetService,
                TransportService transportService
            ) {
                super(
                    StatelessUnpromotableRelocationAction.TYPE.name(),
                    actionFilters,
                    transportService.getTaskManager(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                );
                this.indicesService = indicesService;
                this.peerRecoveryTargetService = peerRecoveryTargetService;
            }

            @Override
            protected void doExecute(
                Task task,
                StatelessUnpromotableRelocationAction.Request request,
                ActionListener<ActionResponse.Empty> listener
            ) {
                try (var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.getRecoveryId(), request.getShardId())) {
                    final var indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
                    final var indexShard = indexService.getShard(request.getShardId().id());
                    final var recoveryTarget = recoveryRef.target();
                    final var recoveryState = recoveryTarget.state();

                    ActionListener.completeWith(listener, () -> {
                        indexShard.prepareForIndexRecovery();
                        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
                        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
                        indexShard.openEngineAndSkipTranslogRecovery();
                        recoveryState.getIndex().setFileDetailsComplete();
                        recoveryState.setStage(RecoveryState.Stage.FINALIZE);
                        return ActionResponse.Empty.INSTANCE;
                    });
                }
            }
        }

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(StatelessUnpromotableRelocationAction.TYPE, TransportTestUnpromotableRelocationAction.class));
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new AllocationDecider() {
                @Override
                public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    // once a primary is cancelled it _stays_ cancelled
                    if (shardRouting.unassignedInfo().reason() == UnassignedInfo.Reason.REROUTE_CANCELLED) {
                        return Decision.NO;
                    }
                    return super.canForceAllocatePrimary(shardRouting, node, allocation);
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    var nodesWithUnpromotableOnly = allocation.getClusterState()
                        .nodes()
                        .stream()
                        .filter(n -> Objects.equals("true", n.getAttributes().get(NODE_ATTR_UNPROMOTABLE_ONLY)))
                        .map(DiscoveryNode::getName)
                        .collect(Collectors.toUnmodifiableSet());
                    if (nodesWithUnpromotableOnly.isEmpty() == false) {
                        if (nodesWithUnpromotableOnly.contains(node.node().getName())) {
                            if (shardRouting.isPromotableToPrimary()) {
                                return allocation.decision(
                                    Decision.NO,
                                    "test",
                                    "shard is promotable to primary so may not be assigned to [" + node.node().getName() + "]"
                                );
                            }
                        } else {
                            if (shardRouting.isPromotableToPrimary() == false) {
                                return allocation.decision(
                                    Decision.NO,
                                    "test",
                                    "shard is not promotable to primary so may not be assigned to [" + node.node().getName() + "]"
                                );
                            }
                        }
                    }
                    return Decision.YES;
                }
            });
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                if (config.isPromotableToPrimary()) {
                    return new InternalEngine(config);
                } else {
                    try {
                        config.getStore().createEmpty();
                    } catch (IOException e) {
                        logger.error("Error creating empty store", e);
                        throw new RuntimeException(e);
                    }

                    return new NoOpEngine(EngineTestCase.copy(config, () -> -1L));
                }
            });
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(MockTransportService.TestPlugin.class, TestPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    private static TestPlugin getMasterNodePlugin() {
        return internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(TestPlugin.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no plugin"));
    }

    private static final String INDEX_NAME = "test";

    private static class RoutingTableWatcher implements ClusterStateListener {

        final int numShards = between(1, 3);
        int numIndexingCopies = between(1, 2);
        int numReplicas = between(numIndexingCopies - 1, 3);

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            assertRoles(event.state().getRoutingTable().index(INDEX_NAME));
        }

        private void assertRoles(IndexRoutingTable indexRoutingTable) {
            if (indexRoutingTable == null) {
                return;
            }
            var message = indexRoutingTable.prettyPrint();
            assertEquals("number_of_shards: " + message, numShards, indexRoutingTable.size());
            for (int shardId = 0; shardId < numShards; shardId++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(shardId);
                assertEquals("number_of_replicas: " + message, numReplicas + 1, indexShardRoutingTable.size());
                var indexingShards = 0;
                for (int shardCopy = 0; shardCopy < numReplicas + 1; shardCopy++) {
                    final var shardRouting = indexShardRoutingTable.shard(shardCopy);
                    switch (shardRouting.role()) {
                        case INDEX_ONLY -> indexingShards += 1;
                        case SEARCH_ONLY -> assertFalse(shardRouting.primary());
                        case DEFAULT -> fail("should not have any DEFAULT shards");
                    }
                    if (shardRouting.relocating()) {
                        assertEquals("role on relocation: " + message, shardRouting.role(), shardRouting.getTargetRelocatingShard().role());
                    }
                }
                assertEquals("number_of_indexing_shards: " + message, Math.min(numIndexingCopies, numReplicas + 1), indexingShards);
            }
        }

        Settings getIndexSettings() {
            logger.info("--> numShards={}, numReplicas={}", numShards, numReplicas);
            return indexSettings(numShards, numReplicas).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertRolesInRoutingTableXContent(ClusterState state) {
        try {
            final var routingTable = (Map<String, Object>) XContentTestUtils.convertToMap(state).get("routing_table");
            final var routingTableIndices = (Map<String, Object>) routingTable.get("indices");
            final var routingTableIndex = (Map<String, Object>) routingTableIndices.get("test");
            final var routingTableShards = (Map<String, Object>) routingTableIndex.get("shards");
            for (final var routingTableShardValue : routingTableShards.values()) {
                for (Object routingTableShardCopy : (List<Object>) routingTableShardValue) {
                    final var routingTableShard = (Map<String, String>) routingTableShardCopy;
                    assertNotNull(ShardRouting.Role.valueOf(routingTableShard.get("role")));
                }
            }
        } catch (IOException e) {
            fail(e);
        }
    }

    private static void installMockTransportVerifications(RoutingTableWatcher routingTableWatcher) {
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (routingTableWatcher.numIndexingCopies == 1) {
                    assertThat("no recovery action should be exchanged", action, not(startsWith("internal:index/shard/recovery/")));
                    assertThat("no replicated action should be exchanged", action, not(containsString("[r]")));
                }
                connection.sendRequest(requestId, action, request, options);
            });
            mockTransportService.addRequestHandlingBehavior(
                TransportUnpromotableShardRefreshAction.NAME + "[u]",
                (handler, request, channel, task) -> {
                    // Skip handling the request and send an immediate empty response
                    channel.sendResponse(ActionResponse.Empty.INSTANCE);
                }
            );
        }
    }

    public void testShardCreation() throws Exception {
        var routingTableWatcher = new RoutingTableWatcher();

        var numDataNodes = routingTableWatcher.numReplicas + 2;
        internalCluster().ensureAtLeastNumDataNodes(numDataNodes);
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());

            final var clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setRoutingTable(true).get().getState();

            // verify non-DEFAULT roles reported in cluster state XContent
            assertRolesInRoutingTableXContent(clusterState);

            // verify non-DEFAULT roles reported in cluster state string representation
            var stateAsString = clusterState.toString();
            assertThat(stateAsString, containsString("[" + ShardRouting.Role.INDEX_ONLY + "]"));
            assertThat(stateAsString, not(containsString("[" + ShardRouting.Role.DEFAULT + "]")));
            if (routingTableWatcher.numReplicas + 1 > routingTableWatcher.numIndexingCopies) {
                assertThat(stateAsString, containsString("[" + ShardRouting.Role.SEARCH_ONLY + "]"));
            }

            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            // new replicas get the SEARCH_ONLY role
            routingTableWatcher.numReplicas += 1;
            setReplicaCount(routingTableWatcher.numReplicas, "test");

            ensureGreen(INDEX_NAME);
            assertEngineTypes();
            indexRandom(randomBoolean(), INDEX_NAME, randomIntBetween(50, 100));

            // removing replicas drops SEARCH_ONLY copies first
            while (routingTableWatcher.numReplicas > 0) {
                routingTableWatcher.numReplicas -= 1;
                setReplicaCount(routingTableWatcher.numReplicas, "test");
            }

            // restoring the index from a snapshot may change the number of indexing replicas because the routing table is created afresh
            var repoPath = randomRepoPath();
            assertAcked(
                clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                    .setType("fs")
                    .setSettings(Settings.builder().put("location", repoPath))
            );

            assertEquals(
                SnapshotState.SUCCESS,
                clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
                    .setWaitForCompletion(true)
                    .get()
                    .getSnapshotInfo()
                    .state()
            );

            if (randomBoolean()) {
                assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));
            } else {
                assertAcked(indicesAdmin().prepareClose(INDEX_NAME));
                ensureGreen(INDEX_NAME);
            }

            routingTableWatcher.numReplicas = between(0, numDataNodes - 1);
            routingTableWatcher.numIndexingCopies = between(1, 2);
            getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

            assertEquals(
                0,
                clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
                    .setIndices(INDEX_NAME)
                    .setIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, routingTableWatcher.numReplicas))
                    .setWaitForCompletion(true)
                    .get()
                    .getRestoreInfo()
                    .failedShards()
            );
            ensureGreen(INDEX_NAME);
            assertEngineTypes();
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    private void assertEngineTypes() {
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    final var engine = indexShard.getEngineOrNull();
                    assertNotNull(engine);
                    if (indexShard.routingEntry().isPromotableToPrimary()
                        && indexShard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.OPEN) {
                        assertThat(engine, instanceOf(InternalEngine.class));
                    } else {
                        assertThat(engine, instanceOf(NoOpEngine.class));
                    }
                }
            }
        }
    }

    public void testRelocation() {
        var routingTableWatcher = new RoutingTableWatcher();

        var numDataNodes = routingTableWatcher.numReplicas + 2;
        internalCluster().ensureAtLeastNumDataNodes(numDataNodes);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());

            for (String nodeName : internalCluster().getNodeNames()) {
                updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", nodeName), "test");
                ensureGreen(INDEX_NAME);
            }
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    public void testPromotion() {
        var routingTableWatcher = new RoutingTableWatcher();

        var numDataNodes = routingTableWatcher.numReplicas + 2;
        internalCluster().ensureAtLeastNumDataNodes(numDataNodes);
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());
            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", "not-a-node"), "test");
            AllocationCommand cancelPrimaryCommand;
            while ((cancelPrimaryCommand = getCancelPrimaryCommand()) != null) {
                ClusterRerouteUtils.reroute(client(), cancelPrimaryCommand);
            }
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    @Nullable
    public AllocationCommand getCancelPrimaryCommand() {
        final var indexRoutingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .routingTable()
            .index(INDEX_NAME);
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final var indexShardRoutingTable = indexRoutingTable.shard(shardId);
            if (indexShardRoutingTable.primaryShard().assignedToNode()) {
                return new CancelAllocationCommand(INDEX_NAME, shardId, indexShardRoutingTable.primaryShard().currentNodeId(), true);
            } else {
                assertThat(indexShardRoutingTable.assignedShards(), empty());
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    final var shardRouting = indexShardRoutingTable.shard(copy);
                    assertEquals(
                        shardRouting.role().isPromotableToPrimary()
                            ? UnassignedInfo.Reason.REROUTE_CANCELLED
                            : UnassignedInfo.Reason.UNPROMOTABLE_REPLICA,
                        shardRouting.unassignedInfo().reason()
                    );
                }
            }
        }
        return null;
    }

    public void testSearchRouting() throws Exception {

        var routingTableWatcher = new RoutingTableWatcher();
        routingTableWatcher.numReplicas = Math.max(1, routingTableWatcher.numReplicas);
        routingTableWatcher.numIndexingCopies = Math.min(routingTableWatcher.numIndexingCopies, routingTableWatcher.numReplicas);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        internalCluster().ensureAtLeastNumDataNodes(routingTableWatcher.numReplicas + 1);
        installMockTransportVerifications(routingTableWatcher);

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());
            indexRandom(randomBoolean(), INDEX_NAME, randomIntBetween(50, 100));
            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            final var searchShardProfileKeys = new HashSet<String>();
            final var indexRoutingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setRoutingTable(true)
                .get()
                .getState()
                .routingTable()
                .index(INDEX_NAME);

            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int shardCopy = 0; shardCopy < indexShardRoutingTable.size(); shardCopy++) {
                    final var shardRouting = indexShardRoutingTable.shard(shardCopy);
                    if (shardRouting.role() == ShardRouting.Role.SEARCH_ONLY) {
                        searchShardProfileKeys.add("[" + shardRouting.currentNodeId() + "][" + INDEX_NAME + "][" + shardId + "]");
                    }
                }
            }
            // Regular search
            for (int i = 0; i < 10; i++) {
                final var search = prepareSearch(INDEX_NAME).setProfile(true);
                switch (randomIntBetween(0, 2)) {
                    case 0 -> search.setRouting(randomAlphaOfLength(10));
                    case 1 -> search.setPreference(randomSearchPreference(routingTableWatcher.numShards, internalCluster().getNodeNames()));
                    default -> {
                        // do nothing
                    }
                }
                assertResponse(search, resp -> {
                    final var profileResults = resp.getProfileResults();
                    assertThat(profileResults, not(anEmptyMap()));
                    for (final var searchShardProfileKey : profileResults.keySet()) {
                        assertThat(searchShardProfileKeys, hasItem(searchShardProfileKey));
                    }
                });
            }
            // Search with PIT
            for (int i = 0; i < 10; i++) {
                final var openRequest = new OpenPointInTimeRequest(INDEX_NAME).keepAlive(TimeValue.timeValueMinutes(1));
                switch (randomIntBetween(0, 2)) {
                    case 0 -> openRequest.routing(randomAlphaOfLength(10));
                    case 1 -> openRequest.preference(
                        randomSearchPreference(routingTableWatcher.numShards, internalCluster().getNodeNames())
                    );
                    default -> {
                        // do nothing
                    }
                }
                BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openRequest).actionGet().getPointInTimeId();
                try {
                    assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)).setProfile(true), response -> {
                        var profileResults = response.getProfileResults();
                        assertThat(profileResults, not(anEmptyMap()));
                        for (final var profileKey : profileResults.keySet()) {
                            assertThat(profileKey, in(searchShardProfileKeys));
                        }
                    });
                } finally {
                    client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId));
                }
            }
            // search-shards API
            for (int i = 0; i < 10; i++) {
                final var search = new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, INDEX_NAME);
                switch (randomIntBetween(0, 2)) {
                    case 0 -> search.routing(randomAlphaOfLength(10));
                    case 1 -> search.routing(randomSearchPreference(routingTableWatcher.numShards, internalCluster().getNodeNames()));
                    default -> {
                        // do nothing
                    }
                }
                ClusterSearchShardsGroup[] groups = safeExecute(client(), TransportClusterSearchShardsAction.TYPE, search).getGroups();
                for (ClusterSearchShardsGroup group : groups) {
                    for (ShardRouting shr : group.getShards()) {
                        String profileKey = "[" + shr.currentNodeId() + "][" + INDEX_NAME + "][" + shr.id() + "]";
                        assertThat(profileKey, in(searchShardProfileKeys));
                    }
                }
            }
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    private String randomSearchPreference(int numShards, String... nodeIds) {
        final var preference = randomFrom(Preference.SHARDS, Preference.PREFER_NODES, Preference.LOCAL);
        // ONLY_LOCAL and ONLY_NODES omitted here because they may yield no shard copies which causes the search to fail
        // TODO add support for ONLY_LOCAL and ONLY_NODES too
        return switch (preference) {
            case LOCAL, ONLY_LOCAL -> preference.type();
            case PREFER_NODES, ONLY_NODES -> preference.type() + ":" + String.join(",", randomNonEmptySubsetOf(Arrays.asList(nodeIds)));
            case SHARDS -> preference.type()
                + ":"
                + String.join(
                    ",",
                    randomSubsetOf(between(1, numShards), IntStream.range(0, numShards).mapToObj(Integer::toString).toList())
                );
        };
    }

    public void testClosedIndex() {
        var routingTableWatcher = new RoutingTableWatcher();

        var numDataNodes = routingTableWatcher.numReplicas + 2;
        internalCluster().ensureAtLeastNumDataNodes(numDataNodes);
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());
            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            assertAcked(indicesAdmin().prepareClose(INDEX_NAME));
            ensureGreen(INDEX_NAME);
            assertEngineTypes();
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    public void testRefreshOfUnpromotableShards() throws Exception {
        var routingTableWatcher = new RoutingTableWatcher();

        var numDataNodes = routingTableWatcher.numReplicas + 2;
        internalCluster().ensureAtLeastNumDataNodes(numDataNodes);
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;
        final AtomicInteger unpromotableRefreshActions = new AtomicInteger(0);

        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.startsWith(TransportUnpromotableShardRefreshAction.NAME)) {
                    unpromotableRefreshActions.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(
                INDEX_NAME,
                Settings.builder()
                    .put(routingTableWatcher.getIndexSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .build()
            );
            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            indexRandom(true, INDEX_NAME, randomIntBetween(1, 10));

            int singleRefreshExpectedUnpromotableActions = (routingTableWatcher.numReplicas - (routingTableWatcher.numIndexingCopies - 1))
                * routingTableWatcher.numShards;
            if (singleRefreshExpectedUnpromotableActions > 0) {
                assertThat(
                    "at least one refresh is expected where each primary sends an unpromotable refresh to each unpromotable replica shard.",
                    unpromotableRefreshActions.get(),
                    greaterThanOrEqualTo(singleRefreshExpectedUnpromotableActions)
                );
                assertThat(
                    "the number of unpromotable refreshes seen is expected to be a multiple of the occurred refreshes",
                    unpromotableRefreshActions.get() % singleRefreshExpectedUnpromotableActions,
                    is(equalTo(0))
                );
            }
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    public void testRefreshFailsIfUnpromotableDisconnects() throws Exception {
        var routingTableWatcher = new RoutingTableWatcher();
        var additionalNumberOfNodesWithUnpromotableShards = 1;
        routingTableWatcher.numReplicas = routingTableWatcher.numIndexingCopies + additionalNumberOfNodesWithUnpromotableShards - 1;
        internalCluster().ensureAtLeastNumDataNodes(routingTableWatcher.numIndexingCopies + 1);
        final String nodeWithUnpromotableOnly = internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr." + TestPlugin.NODE_ATTR_UNPROMOTABLE_ONLY, "true").build()
        );
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);

            createIndex(
                INDEX_NAME,
                Settings.builder()
                    .put(routingTableWatcher.getIndexSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .build()
            );
            ensureGreen(INDEX_NAME);
            assertEngineTypes();

            indexRandom(false, INDEX_NAME, randomIntBetween(1, 10));

            for (var transportService : internalCluster().getInstances(TransportService.class)) {
                MockTransportService mockTransportService = (MockTransportService) transportService;
                mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (action.equals(TransportUnpromotableShardRefreshAction.NAME + "[u]")
                        && nodeWithUnpromotableOnly.equals(connection.getNode().getName())) {
                        logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                        throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }

            BroadcastResponse response = indicesAdmin().prepareRefresh(INDEX_NAME).get();
            assertThat(
                "each unpromotable replica shard should be added to the shard failures",
                response.getFailedShards(),
                equalTo((routingTableWatcher.numReplicas - (routingTableWatcher.numIndexingCopies - 1)) * routingTableWatcher.numShards)
            );
            assertThat(
                "the total shards is incremented with the unpromotable shard failures",
                response.getTotalShards(),
                equalTo(response.getSuccessfulShards() + response.getFailedShards())
            );
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

    public void testNodesWithUnpromotableShardsNeverGetReplicationActions() throws Exception {
        var routingTableWatcher = new RoutingTableWatcher();
        var additionalNumberOfNodesWithUnpromotableShards = randomIntBetween(1, 3);
        routingTableWatcher.numReplicas = routingTableWatcher.numIndexingCopies + additionalNumberOfNodesWithUnpromotableShards - 1;
        internalCluster().ensureAtLeastNumDataNodes(routingTableWatcher.numIndexingCopies + 1);
        final List<String> nodesWithUnpromotableOnly = internalCluster().startDataOnlyNodes(
            additionalNumberOfNodesWithUnpromotableShards,
            Settings.builder().put("node.attr." + TestPlugin.NODE_ATTR_UNPROMOTABLE_ONLY, "true").build()
        );
        installMockTransportVerifications(routingTableWatcher);
        getMasterNodePlugin().numIndexingCopies = routingTableWatcher.numIndexingCopies;

        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (nodesWithUnpromotableOnly.contains(connection.getNode().getName())) {
                    assertThat(action, not(containsString("[r]")));
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        try {
            // verify the correct number of shard copies of each role as the routing table evolves
            masterClusterService.addListener(routingTableWatcher);
            createIndex(INDEX_NAME, routingTableWatcher.getIndexSettings());
            ensureGreen(INDEX_NAME);
            indexRandom(randomBoolean(), INDEX_NAME, randomIntBetween(50, 100));
        } finally {
            masterClusterService.removeListener(routingTableWatcher);
        }
    }

}
