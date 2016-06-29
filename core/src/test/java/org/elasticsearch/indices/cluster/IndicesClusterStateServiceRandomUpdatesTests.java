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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation.FailedShard;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesClusterStateServiceRandomUpdatesTests extends AbstractIndicesClusterStateServiceTestCase {

    private final ClusterStateChanges cluster = new ClusterStateChanges();

    public void testRandomClusterStateUpdates() {
        // we have an IndicesClusterStateService per node in the cluster
        final Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap = new HashMap<>();
        ClusterState state = randomInitialClusterState(clusterStateServiceMap);

        // each of the following iterations represents a new cluster state update processed on all nodes
        for (int i = 0; i < 30; i++) {
            logger.info("Iteration {}", i);
            final ClusterState previousState = state;

            // calculate new cluster state
            for (int j = 0; j < randomInt(3); j++) { // multiple iterations to simulate batching of cluster states
                state = randomlyUpdateClusterState(state, clusterStateServiceMap);
            }

            // apply cluster state to nodes (incl. master)
            for (DiscoveryNode node : state.nodes()) {
                IndicesClusterStateService indicesClusterStateService = clusterStateServiceMap.get(node);
                ClusterState localState = adaptClusterStateToLocalNode(state, node);
                ClusterState previousLocalState = adaptClusterStateToLocalNode(previousState, node);
                indicesClusterStateService.clusterChanged(new ClusterChangedEvent("simulated change " + i, localState, previousLocalState));

                // check that cluster state has been properly applied to node
                assertClusterStateMatchesNodeState(localState, indicesClusterStateService);
            }
        }

        // TODO: check if we can go to green by starting all shards and finishing all iterations
        logger.info("Final cluster state: {}", state.prettyPrint());
    }

    public ClusterState randomInitialClusterState(Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap) {
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
        updateNodes(state, clusterStateServiceMap);
        return state;
    }

    private void updateNodes(ClusterState state, Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap) {
        for (DiscoveryNode node : state.nodes()) {
            clusterStateServiceMap.computeIfAbsent(node, discoveryNode -> {
                IndicesClusterStateService ics = createIndicesClusterStateService();
                ics.start();
                return ics;
            });
        }

        for (Iterator<Entry<DiscoveryNode, IndicesClusterStateService>> it = clusterStateServiceMap.entrySet().iterator(); it.hasNext(); ) {
            DiscoveryNode node = it.next().getKey();
            if (state.nodes().nodeExists(node.getId()) == false) {
                it.remove();
            }
        }
    }

    public ClusterState randomlyUpdateClusterState(ClusterState state,
                                                   Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap) {
        // randomly create new indices (until we have 200 max)
        for (int i = 0; i < randomInt(5); i++) {
            if (state.metaData().indices().size() > 200) {
                break;
            }
            String name = "index_" + randomAsciiOfLength(15).toLowerCase(Locale.ROOT);
            CreateIndexRequest request = new CreateIndexRequest(name, Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                .put(SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                .build());
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
                    DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).put(createNode()).build();
                    state = ClusterState.builder(state).nodes(newNodes).build();
                    state = cluster.reroute(state, new ClusterRerouteRequest()); // always reroute after node leave
                    updateNodes(state, clusterStateServiceMap);
                }
            } else {
                // remove node
                if (state.nodes().getDataNodes().size() > 3) {
                    DiscoveryNode discoveryNode = randomFrom(state.nodes().getNodes().values().toArray(DiscoveryNode.class));
                    if (discoveryNode.equals(state.nodes().getMasterNode()) == false) {
                        DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).remove(discoveryNode.getId()).build();
                        state = ClusterState.builder(state).nodes(newNodes).build();
                        state = cluster.reroute(state, new ClusterRerouteRequest()); // always reroute after node join
                        updateNodes(state, clusterStateServiceMap);
                    }
                }
            }
        }

        // TODO: go masterless?

        return state;
    }

    protected DiscoveryNode createNode(DiscoveryNode.Role... mustHaveRoles) {
        Set<DiscoveryNode.Role> roles = new HashSet<>(randomSubsetOf(Sets.newHashSet(DiscoveryNode.Role.values())));
        for (DiscoveryNode.Role mustHaveRole : mustHaveRoles) {
            roles.add(mustHaveRole);
        }
        return new DiscoveryNode("node_" + randomAsciiOfLength(8), DummyTransportAddress.INSTANCE, Collections.emptyMap(), roles,
            Version.CURRENT);
    }

    private static ClusterState adaptClusterStateToLocalNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private IndicesClusterStateService createIndicesClusterStateService() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final Executor executor = mock(Executor.class);
        when(threadPool.generic()).thenReturn(executor);
        final MockIndicesService indicesService = new MockIndicesService();
        final TransportService transportService = new TransportService(Settings.EMPTY, null, threadPool);
        final ClusterService clusterService = mock(ClusterService.class);
        final RepositoriesService repositoriesService = new RepositoriesService(Settings.EMPTY, clusterService,
            transportService, null, null);
        final RecoveryTargetService recoveryTargetService = new RecoveryTargetService(Settings.EMPTY, threadPool,
            transportService, null, clusterService);
        final ShardStateAction shardStateAction = mock(ShardStateAction.class);
        return new IndicesClusterStateService(Settings.EMPTY, indicesService, clusterService,
            threadPool, recoveryTargetService, shardStateAction, null, repositoriesService, null, null, null, null, null);
    }

}
