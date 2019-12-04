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

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GatewayMetaStatePersistedStateTests extends ESTestCase {
    private NodeEnvironment nodeEnvironment;
    private ClusterName clusterName;
    private Settings settings;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        nodeEnvironment = newNodeEnvironment();
        localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        clusterName = new ClusterName(randomAlphaOfLength(10));
        settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).build();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        nodeEnvironment.close();
        super.tearDown();
    }

    private CoordinationState.PersistedState newGatewayPersistedState() {
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode);
        gateway.start(settings, nodeEnvironment, xContentRegistry());
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState, not(instanceOf(InMemoryPersistedState.class)));
        return persistedState;
    }

    private CoordinationState.PersistedState maybeNew(CoordinationState.PersistedState persistedState) {
        if (randomBoolean()) {
            return newGatewayPersistedState();
        }
        return persistedState;
    }

    public void testInitialState() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();
        ClusterState state = gateway.getLastAcceptedState();
        assertThat(state.getClusterName(), equalTo(clusterName));
        assertTrue(MetaData.isGlobalStateEquals(state.metaData(), MetaData.EMPTY_META_DATA));
        assertThat(state.getVersion(), equalTo(Manifest.empty().getClusterStateVersion()));
        assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

        long currentTerm = gateway.getCurrentTerm();
        assertThat(currentTerm, equalTo(Manifest.empty().getCurrentTerm()));
    }

    public void testSetCurrentTerm() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long currentTerm = randomNonNegativeLong();
            gateway.setCurrentTerm(currentTerm);
            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
        }
    }

    private ClusterState createClusterState(long version, MetaData metaData) {
        return createClusterState(version, metaData, localNode,
            RoutingTable.EMPTY_ROUTING_TABLE);
    }

    private ClusterState createClusterState(long version, MetaData metaData, DiscoveryNode localNode, RoutingTable routingTable) {
        return ClusterState.builder(clusterName).
                nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()).
                version(version).
                metaData(metaData).
                routingTable(routingTable).
                build();
    }

    private CoordinationMetaData createCoordinationMetaData(long term) {
        CoordinationMetaData.Builder builder = CoordinationMetaData.builder();
        builder.term(term);
        builder.lastAcceptedConfiguration(
                new CoordinationMetaData.VotingConfiguration(
                        Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        builder.lastCommittedConfiguration(
                new CoordinationMetaData.VotingConfiguration(
                        Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            builder.addVotingConfigExclusion(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return builder.build();
    }

    private IndexMetaData createIndexMetaData(String indexName, int numberOfShards, long version) {
        return IndexMetaData.builder(indexName).settings(
                Settings.builder()
                        .put(IndexMetaData.SETTING_INDEX_UUID, indexName)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
        ).version(version).build();
    }

    private void assertClusterStateEqual(ClusterState expected, ClusterState actual) {
        assertThat(actual.version(), equalTo(expected.version()));
        assertTrue(MetaData.isGlobalStateEquals(actual.metaData(), expected.metaData()));
        assertEquals(expected.metaData().indices().size(), actual.metaData().indices().size());
        for (IndexMetaData indexMetaData : expected.metaData()) {
            assertThat(actual.metaData().index(indexMetaData.getIndex()), equalTo(indexMetaData));
        }
    }

    public void testSetLastAcceptedState() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();
        final long term = randomNonNegativeLong();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long version = randomNonNegativeLong();
            final String indexName = randomAlphaOfLength(10);
            final IndexMetaData indexMetaData = createIndexMetaData(indexName, randomIntBetween(1,5), randomNonNegativeLong());
            final MetaData metaData = MetaData.builder().
                    persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                    coordinationMetaData(createCoordinationMetaData(term)).
                    put(indexMetaData, false).
                    build();
            ClusterState state = createClusterState(version, metaData);

            gateway.setLastAcceptedState(state);
            gateway = maybeNew(gateway);

            ClusterState lastAcceptedState = gateway.getLastAcceptedState();
            assertClusterStateEqual(state, lastAcceptedState);
        }
    }

    public void testSetLastAcceptedStateTermChanged() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        final String indexName = randomAlphaOfLength(10);
        final int numberOfShards = randomIntBetween(1, 5);
        final long version = randomNonNegativeLong();
        final long term = randomNonNegativeLong();
        final IndexMetaData indexMetaData = createIndexMetaData(indexName, numberOfShards, version);
        final ClusterState state = createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(createCoordinationMetaData(term)).put(indexMetaData, false).build());
        gateway.setLastAcceptedState(state);

        gateway = maybeNew(gateway);
        final long newTerm = randomValueOtherThan(term, ESTestCase::randomNonNegativeLong);
        final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1,5));
        final IndexMetaData newIndexMetaData = createIndexMetaData(indexName, newNumberOfShards, version);
        final ClusterState newClusterState = createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(createCoordinationMetaData(newTerm)).put(newIndexMetaData, false).build());
        gateway.setLastAcceptedState(newClusterState);

        gateway = maybeNew(gateway);
        assertThat(gateway.getLastAcceptedState().metaData().index(indexName), equalTo(newIndexMetaData));
    }

    public void testCurrentTermAndTermAreDifferent() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        long currentTerm = randomNonNegativeLong();
        long term  = randomValueOtherThan(currentTerm, ESTestCase::randomNonNegativeLong);

        gateway.setCurrentTerm(currentTerm);
        gateway.setLastAcceptedState(createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(CoordinationMetaData.builder().term(term).build()).build()));

        gateway = maybeNew(gateway);
        assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
        assertThat(gateway.getLastAcceptedState().coordinationMetaData().term(), equalTo(term));
    }

    public void testMarkAcceptedConfigAsCommitted() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        //generate random coordinationMetaData with different lastAcceptedConfiguration and lastCommittedConfiguration
        CoordinationMetaData coordinationMetaData;
        do {
            coordinationMetaData = createCoordinationMetaData(randomNonNegativeLong());
        } while (coordinationMetaData.getLastAcceptedConfiguration().equals(coordinationMetaData.getLastCommittedConfiguration()));

        ClusterState state = createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(coordinationMetaData)
                    .clusterUUID(randomAlphaOfLength(10)).build());
        gateway.setLastAcceptedState(state);

        gateway = maybeNew(gateway);
        assertThat(gateway.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(gateway.getLastAcceptedState().getLastCommittedConfiguration())));
        gateway.markLastAcceptedStateAsCommitted();

        CoordinationMetaData expectedCoordinationMetaData = CoordinationMetaData.builder(coordinationMetaData)
                .lastCommittedConfiguration(coordinationMetaData.getLastAcceptedConfiguration()).build();
        ClusterState expectedClusterState =
                ClusterState.builder(state).metaData(MetaData.builder().coordinationMetaData(expectedCoordinationMetaData)
                    .clusterUUID(state.metaData().clusterUUID()).clusterUUIDCommitted(true).build()).build();

        gateway = maybeNew(gateway);
        assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
        gateway.markLastAcceptedStateAsCommitted();

        gateway = maybeNew(gateway);
        assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
    }

    public void testMarkAcceptedConfigAsCommittedOnDataOnlyNode() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        Settings settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).put(
            Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_NAME_SETTING.getKey(), "test").build();
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode);
        final TransportService transportService = mock(TransportService.class);
        TestThreadPool threadPool = new TestThreadPool("testMarkAcceptedConfigAsCommittedOnDataOnlyNode");
        when(transportService.getThreadPool()).thenReturn(threadPool);
        gateway.start(settings, transportService, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            new MetaStateService(nodeEnvironment, xContentRegistry()), null, null);
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState, instanceOf(GatewayMetaState.DataOnlyNodePersistedState.class));

        //generate random coordinationMetaData with different lastAcceptedConfiguration and lastCommittedConfiguration
        CoordinationMetaData coordinationMetaData;
        do {
            coordinationMetaData = createCoordinationMetaData(randomNonNegativeLong());
        } while (coordinationMetaData.getLastAcceptedConfiguration().equals(coordinationMetaData.getLastCommittedConfiguration()));

        IndexMetaData testIndex1 = createIndexMetaData("test1", 1, 1L);
        IndexMetaData testIndex2 = createIndexMetaData("test2", 1, 1L);
        ShardRouting firstRouting = TestShardRouting.newShardRouting(
            new ShardId(testIndex1.getIndex(), 0), localNode.getId(), null, true, ShardRoutingState.STARTED);
        ShardRouting secondRouting = TestShardRouting.newShardRouting(
            new ShardId(testIndex2.getIndex(), 0), localNode.getId(), null, true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable1 = IndexRoutingTable.builder(firstRouting.index())
            .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build()).build();
        IndexRoutingTable indexRoutingTable2 = IndexRoutingTable.builder(secondRouting.index())
            .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build()).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable1).add(indexRoutingTable2).build();
        ClusterState state = createClusterState(randomNonNegativeLong(),
            MetaData.builder().coordinationMetaData(coordinationMetaData)
                .clusterUUID(randomAlphaOfLength(10))
                .put(testIndex1, false).put(testIndex2, false).build(), localNode, routingTable);
        persistedState.setLastAcceptedState(state);

        assertThat(newGatewayPersistedState().getLastAcceptedState().getLastAcceptedConfiguration().getNodeIds().size(),
            equalTo(0));
        assertThat(newGatewayPersistedState().getLastAcceptedState().getLastCommittedConfiguration().getNodeIds().size(),
            equalTo(0));

        assertThat(persistedState.getLastAcceptedState().getLastAcceptedConfiguration(),
            not(equalTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration())));
        assertTrue(persistedState.getLastAcceptedState().metaData().hasIndex("test1"));
        assertTrue(persistedState.getLastAcceptedState().metaData().hasIndex("test2"));
        persistedState.markLastAcceptedStateAsCommitted();
        assertThat(persistedState.getLastAcceptedState().getLastAcceptedConfiguration(),
            equalTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration()));
        assertTrue(persistedState.getLastAcceptedState().metaData().hasIndex("test1"));
        assertTrue(persistedState.getLastAcceptedState().metaData().hasIndex("test2"));

        CoordinationMetaData expectedCoordinationMetaData = CoordinationMetaData.builder(coordinationMetaData)
            .lastCommittedConfiguration(coordinationMetaData.getLastAcceptedConfiguration()).build();
        ClusterState expectedClusterState =
            ClusterState.builder(state).metaData(MetaData.builder().coordinationMetaData(expectedCoordinationMetaData)
                .clusterUUID(state.metaData().clusterUUID()).clusterUUIDCommitted(true)
                .put(testIndex1, false).put(testIndex2, false).build())
                .build();

        assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());
        // load from disk again and check if persisted state matches current state
        // use assertBusy as state is written out asynchronously
        assertBusy(() -> assertClusterStateEqual(expectedClusterState, newGatewayPersistedState().getLastAcceptedState()));
        assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());

        // trigger two concurrent updates and check if the latest one is correctly applied
        state = ClusterState.builder(state).incrementVersion().routingTable(RoutingTable.builder().add(indexRoutingTable1).build()).build();
        persistedState.setLastAcceptedState(state);
        persistedState.markLastAcceptedStateAsCommitted();

        state = ClusterState.builder(state).incrementVersion().routingTable(RoutingTable.builder().add(indexRoutingTable2).build()).build();
        persistedState.setLastAcceptedState(state);
        persistedState.markLastAcceptedStateAsCommitted();

        ClusterState expectedClusterState2 =
            ClusterState.builder(state)
                .version(state.version())
                .stateUUID(state.stateUUID())
                .metaData(MetaData.builder().coordinationMetaData(expectedCoordinationMetaData)
                .clusterUUID(state.metaData().clusterUUID()).clusterUUIDCommitted(true)
                .put(testIndex2, false).build()).build();
        assertBusy(() -> assertClusterStateEqual(expectedClusterState2, newGatewayPersistedState().getLastAcceptedState()));

        persistedState.close();

        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
