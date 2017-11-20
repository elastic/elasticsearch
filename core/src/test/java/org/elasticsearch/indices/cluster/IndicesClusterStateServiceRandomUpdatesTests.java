/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class IndicesClusterStateServiceRandomUpdatesTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterStateChanges cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRandomClusterStateUpdates() {
        // we have an IndicesClusterStateService per node in the cluster
        final Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap = new HashMap<>();
        ClusterState state = randomInitialClusterState(clusterStateServiceMap, MockIndicesService::new);
        // each of the following iterations represents a new cluster state update processed on all nodes
        for (int i = 0; i < 30; i++) {
            logger.info("Iteration {}", i);
            final ClusterState previousState = state;

            // calculate new cluster state
            for (int j = 0; j < randomInt(3); j++) { // multiple iterations to simulate batching of cluster states
                try {
                    state = randomlyUpdateClusterState(state, clusterStateServiceMap, MockIndicesService::new);
                } catch (AssertionError error) {
                    ClusterState finalState = state;
                    logger.error((org.apache.logging.log4j.util.Supplier<?>) () ->
                        new ParameterizedMessage("failed to random change state. last good state: \n{}", finalState), error);
                    throw error;
                }
            }

            // apply cluster state to nodes (incl. master)
            for (DiscoveryNode node : state.nodes()) {
                IndicesClusterStateService indicesClusterStateService = clusterStateServiceMap.get(node);
                ClusterState localState = adaptClusterStateToLocalNode(state, node);
                ClusterState previousLocalState = adaptClusterStateToLocalNode(previousState, node);
                final ClusterChangedEvent event = new ClusterChangedEvent("simulated change " + i, localState, previousLocalState);
                try {
                    indicesClusterStateService.applyClusterState(event);
                } catch (AssertionError error) {
                    logger.error((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
                            "failed to apply change on [{}].\n ***  Previous state ***\n{}\n ***  New state ***\n{}",
                            node, event.previousState(), event.state()), error);
                    throw error;
                }

                // check that cluster state has been properly applied to node
                assertClusterStateMatchesNodeState(localState, indicesClusterStateService);
            }
        }

        // TODO: check if we can go to green by starting all shards and finishing all iterations
        logger.info("Final cluster state: {}", state);
    }

    /**
     * This test ensures that when a node joins a brand new cluster (different cluster UUID),
     * different from the cluster it was previously a part of, the in-memory index data structures
     * are all removed but the on disk contents of those indices remain so that they can later be
     * imported as dangling indices.  Normally, the first cluster state update that the node
     * receives from the new cluster would contain a cluster block that would cause all in-memory
     * structures to be removed (see {@link IndicesClusterStateService#applyClusterState(ClusterChangedEvent)}),
     * but in the case where the node joined and was a few cluster state updates behind, it would
     * not have received the cluster block, in which case we still need to remove the in-memory
     * structures while ensuring the data remains on disk.  This test executes this particular
     * scenario.
     */
    public void testJoiningNewClusterOnlyRemovesInMemoryIndexStructures() {
        // a cluster state derived from the initial state that includes a created index
        String name = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        ShardRoutingState[] replicaStates = new ShardRoutingState[randomIntBetween(0, 3)];
        Arrays.fill(replicaStates, ShardRoutingState.INITIALIZING);
        ClusterState stateWithIndex = ClusterStateCreationUtils.state(name, randomBoolean(), ShardRoutingState.INITIALIZING, replicaStates);

        // the initial state which is derived from the newly created cluster state but doesn't contain the index
        ClusterState initialState = ClusterState.builder(stateWithIndex)
            .metaData(MetaData.builder(stateWithIndex.metaData()).remove(name))
            .routingTable(RoutingTable.builder().build())
            .build();

        // pick a data node to simulate the adding an index cluster state change event on, that has shards assigned to it
        DiscoveryNode node = stateWithIndex.nodes().get(
            randomFrom(stateWithIndex.routingTable().index(name).shardsWithState(INITIALIZING)).currentNodeId());

        // simulate the cluster state change on the node
        ClusterState localState = adaptClusterStateToLocalNode(stateWithIndex, node);
        ClusterState previousLocalState = adaptClusterStateToLocalNode(initialState, node);
        IndicesClusterStateService indicesCSSvc = createIndicesClusterStateService(node, RecordingIndicesService::new);
        indicesCSSvc.start();
        indicesCSSvc.applyClusterState(new ClusterChangedEvent("cluster state change that adds the index", localState, previousLocalState));

        // create a new empty cluster state with a brand new cluster UUID
        ClusterState newClusterState = ClusterState.builder(initialState)
            .metaData(MetaData.builder(initialState.metaData()).clusterUUID(UUIDs.randomBase64UUID()))
            .build();

        // simulate the cluster state change on the node
        localState = adaptClusterStateToLocalNode(newClusterState, node);
        previousLocalState = adaptClusterStateToLocalNode(stateWithIndex, node);
        indicesCSSvc.applyClusterState(new ClusterChangedEvent(
            "cluster state change with a new cluster UUID (and doesn't contain the index)", localState, previousLocalState));

        // check that in memory data structures have been removed once the new cluster state is applied,
        // but the persistent data is still there
        RecordingIndicesService indicesService = (RecordingIndicesService) indicesCSSvc.indicesService;
        for (IndexMetaData indexMetaData : stateWithIndex.metaData()) {
            Index index = indexMetaData.getIndex();
            assertNull(indicesService.indexService(index));
            assertFalse(indicesService.isDeleted(index));
        }
    }

    public ClusterState randomInitialClusterState(Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
                                                  Supplier<MockIndicesService> indicesServiceSupplier) {
        List<DiscoveryNode> allNodes = new ArrayList<>();
        DiscoveryNode localNode = createNode(DiscoveryNode.Role.MASTER); // local node is the master
        allNodes.add(localNode);
        // at least two nodes that have the data role so that we can allocate shards
        allNodes.add(createNode(DiscoveryNode.Role.DATA));
        allNodes.add(createNode(DiscoveryNode.Role.DATA));
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            allNodes.add(createNode());
        }
        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[allNodes.size()]));
        // add nodes to clusterStateServiceMap
        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
        return state;
    }

    private void updateNodes(ClusterState state, Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
                             Supplier<MockIndicesService> indicesServiceSupplier) {
        for (DiscoveryNode node : state.nodes()) {
            clusterStateServiceMap.computeIfAbsent(node, discoveryNode -> {
                IndicesClusterStateService ics = createIndicesClusterStateService(discoveryNode, indicesServiceSupplier);
                ics.start();
                return ics;
            });
        }

        for (Iterator<Entry<DiscoveryNode, IndicesClusterStateService>> it = clusterStateServiceMap.entrySet().iterator(); it.hasNext(); ) {
            DiscoveryNode node = it.next().getKey();
            if (state.nodes().nodeExists(node) == false) {
                it.remove();
            }
        }
    }

    public ClusterState randomlyUpdateClusterState(ClusterState state,
                                                   Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
                                                   Supplier<MockIndicesService> indicesServiceSupplier) {
        // randomly remove no_master blocks
        if (randomBoolean() && state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)) {
            state = ClusterState.builder(state).blocks(
                ClusterBlocks.builder().blocks(state.blocks()).removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)).build();
        }

        // randomly add no_master blocks
        if (rarely() && state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID) == false) {
            ClusterBlock block = randomBoolean() ? DiscoverySettings.NO_MASTER_BLOCK_ALL : DiscoverySettings.NO_MASTER_BLOCK_WRITES;
            state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(block)).build();
        }

        // if no_master block is in place, make no other cluster state changes
        if (state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)) {
            return state;
        }

        // randomly create new indices (until we have 200 max)
        for (int i = 0; i < randomInt(5); i++) {
            if (state.metaData().indices().size() > 200) {
                break;
            }
            String name = "index_" + randomAlphaOfLength(15).toLowerCase(Locale.ROOT);
            Settings.Builder settingsBuilder = Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                .put(SETTING_NUMBER_OF_REPLICAS, randomInt(2));
            CreateIndexRequest request = new CreateIndexRequest(name, settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metaData().hasIndex(name));
        }

        // randomly delete indices
        Set<String> indicesToDelete = new HashSet<>();
        int numberOfIndicesToDelete = randomInt(Math.min(2, state.metaData().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToDelete, state.metaData().indices().keys().toArray(String.class))) {
            indicesToDelete.add(state.metaData().index(index).getIndex().getName());
        }
        if (indicesToDelete.isEmpty() == false) {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indicesToDelete.toArray(new String[indicesToDelete.size()]));
            state = cluster.deleteIndices(state, deleteRequest);
            for (String index : indicesToDelete) {
                assertFalse(state.metaData().hasIndex(index));
            }
        }

        // randomly close indices
        int numberOfIndicesToClose = randomInt(Math.min(1, state.metaData().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToClose, state.metaData().indices().keys().toArray(String.class))) {
            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(state.metaData().index(index).getIndex().getName());
            state = cluster.closeIndices(state, closeIndexRequest);
        }

        // randomly open indices
        int numberOfIndicesToOpen = randomInt(Math.min(1, state.metaData().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToOpen, state.metaData().indices().keys().toArray(String.class))) {
            OpenIndexRequest openIndexRequest = new OpenIndexRequest(state.metaData().index(index).getIndex().getName());
            state = cluster.openIndices(state, openIndexRequest);
        }

        // randomly update settings
        Set<String> indicesToUpdate = new HashSet<>();
        boolean containsClosedIndex = false;
        int numberOfIndicesToUpdate = randomInt(Math.min(2, state.metaData().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToUpdate, state.metaData().indices().keys().toArray(String.class))) {
            indicesToUpdate.add(state.metaData().index(index).getIndex().getName());
            if (state.metaData().index(index).getState() == IndexMetaData.State.CLOSE) {
                containsClosedIndex = true;
            }
        }
        if (indicesToUpdate.isEmpty() == false) {
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(
                indicesToUpdate.toArray(new String[indicesToUpdate.size()]));
            Settings.Builder settings = Settings.builder();
            if (containsClosedIndex == false) {
                settings.put(SETTING_NUMBER_OF_REPLICAS, randomInt(2));
            }
            settings.put("index.refresh_interval", randomIntBetween(1, 5) + "s");
            updateSettingsRequest.settings(settings.build());
            state = cluster.updateSettings(state, updateSettingsRequest);
        }

        // randomly reroute
        if (rarely()) {
            state = cluster.reroute(state, new ClusterRerouteRequest());
        }

        // randomly start and fail allocated shards
        List<ShardRouting> startedShards = new ArrayList<>();
        List<FailedShard> failedShards = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            IndicesClusterStateService indicesClusterStateService = clusterStateServiceMap.get(node);
            MockIndicesService indicesService = (MockIndicesService) indicesClusterStateService.indicesService;
            for (MockIndexService indexService : indicesService) {
                for (MockIndexShard indexShard : indexService) {
                    ShardRouting persistedShardRouting = indexShard.routingEntry();
                    if (persistedShardRouting.initializing() && randomBoolean()) {
                        startedShards.add(persistedShardRouting);
                    } else if (rarely()) {
                        failedShards.add(new FailedShard(persistedShardRouting, "fake shard failure", new Exception()));
                    }
                }
            }
        }
        state = cluster.applyFailedShards(state, failedShards);
        state = cluster.applyStartedShards(state, startedShards);

        // randomly add and remove nodes (except current master)
        if (rarely()) {
            if (randomBoolean()) {
                // add node
                if (state.nodes().getSize() < 10) {
                    DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).add(createNode()).build();
                    state = ClusterState.builder(state).nodes(newNodes).build();
                    state = cluster.reroute(state, new ClusterRerouteRequest()); // always reroute after node leave
                    updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                }
            } else {
                // remove node
                if (state.nodes().getDataNodes().size() > 3) {
                    DiscoveryNode discoveryNode = randomFrom(state.nodes().getNodes().values().toArray(DiscoveryNode.class));
                    if (discoveryNode.equals(state.nodes().getMasterNode()) == false) {
                        DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).remove(discoveryNode.getId()).build();
                        state = ClusterState.builder(state).nodes(newNodes).build();
                        state = cluster.deassociateDeadNodes(state, true, "removed and added a node");
                        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                    }
                    if (randomBoolean()) {
                        // and add it back
                        DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).add(discoveryNode).build();
                        state = ClusterState.builder(state).nodes(newNodes).build();
                        state = cluster.reroute(state, new ClusterRerouteRequest());
                        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                    }
                }
            }
        }

        // TODO: go masterless?

        return state;
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(DiscoveryNode.Role... mustHaveRoles) {
        Set<DiscoveryNode.Role> roles = new HashSet<>(randomSubsetOf(Sets.newHashSet(DiscoveryNode.Role.values())));
        for (DiscoveryNode.Role mustHaveRole : mustHaveRoles) {
            roles.add(mustHaveRole);
        }
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles,
            Version.CURRENT);
    }

    private static ClusterState adaptClusterStateToLocalNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private IndicesClusterStateService createIndicesClusterStateService(DiscoveryNode discoveryNode,
                                                                        final Supplier<MockIndicesService> indicesServiceSupplier) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(mock(ExecutorService.class));
        final MockIndicesService indicesService = indicesServiceSupplier.get();
        final Settings settings = Settings.builder().put("node.name", discoveryNode.getName()).build();
        final TransportService transportService = new TransportService(settings, null, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()), null);
        final ClusterService clusterService = mock(ClusterService.class);
        final RepositoriesService repositoriesService = new RepositoriesService(settings, clusterService,
            transportService, null);
        final PeerRecoveryTargetService recoveryTargetService = new PeerRecoveryTargetService(settings, threadPool,
            transportService, null, clusterService);
        final ShardStateAction shardStateAction = mock(ShardStateAction.class);
        final PrimaryReplicaSyncer primaryReplicaSyncer = mock(PrimaryReplicaSyncer.class);
        return new IndicesClusterStateService(
                settings,
                indicesService,
                clusterService,
                threadPool,
                recoveryTargetService,
                shardStateAction,
                null,
                repositoriesService,
                null,
                null,
                null,
                null,
                primaryReplicaSyncer,
                s -> {});
    }

    private class RecordingIndicesService extends MockIndicesService {
        private Set<Index> deletedIndices = Collections.emptySet();

        @Override
        public synchronized void removeIndex(Index index, IndexRemovalReason reason, String extraInfo) {
            super.removeIndex(index, reason, extraInfo);
            if (reason == IndexRemovalReason.DELETED) {
                Set<Index> newSet = Sets.newHashSet(deletedIndices);
                newSet.add(index);
                deletedIndices = Collections.unmodifiableSet(newSet);
            }
        }

        public synchronized boolean isDeleted(Index index) {
            return deletedIndices.contains(index);
        }
    }

}
