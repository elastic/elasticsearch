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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
        assertThat(persistedState, instanceOf(GatewayMetaState.LucenePersistedState.class));
        return persistedState;
    }

    private CoordinationState.PersistedState maybeNew(CoordinationState.PersistedState persistedState) throws IOException {
        if (randomBoolean()) {
            persistedState.close();
            return newGatewayPersistedState();
        }
        return persistedState;
    }

    public void testInitialState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            ClusterState state = gateway.getLastAcceptedState();
            assertThat(state.getClusterName(), equalTo(clusterName));
            assertTrue(MetaData.isGlobalStateEquals(state.metaData(), MetaData.EMPTY_META_DATA));
            assertThat(state.getVersion(), equalTo(Manifest.empty().getClusterStateVersion()));
            assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

            long currentTerm = gateway.getCurrentTerm();
            assertThat(currentTerm, equalTo(Manifest.empty().getCurrentTerm()));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testSetCurrentTerm() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long currentTerm = randomNonNegativeLong();
                gateway.setCurrentTerm(currentTerm);
                gateway = maybeNew(gateway);
                assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
            }
        } finally {
            IOUtils.close(gateway);
        }
    }

    private ClusterState createClusterState(long version, MetaData metaData) {
        return ClusterState.builder(clusterName).
            nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()).
            version(version).
            metaData(metaData).
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
        for (IndexMetaData indexMetaData : expected.metaData()) {
            assertThat(actual.metaData().index(indexMetaData.getIndex()), equalTo(indexMetaData));
        }
    }

    public void testSetLastAcceptedState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            final long term = randomNonNegativeLong();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long version = randomNonNegativeLong();
                final String indexName = randomAlphaOfLength(10);
                final IndexMetaData indexMetaData = createIndexMetaData(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
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
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testSetLastAcceptedStateTermChanged() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            final String indexName = randomAlphaOfLength(10);
            final int numberOfShards = randomIntBetween(1, 5);
            final long version = randomNonNegativeLong();
            final long term = randomValueOtherThan(Long.MAX_VALUE, ESTestCase::randomNonNegativeLong);
            final IndexMetaData indexMetaData = createIndexMetaData(indexName, numberOfShards, version);
            final ClusterState state = createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(createCoordinationMetaData(term)).put(indexMetaData, false).build());
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            final long newTerm = randomLongBetween(term + 1, Long.MAX_VALUE);
            final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1, 5));
            final IndexMetaData newIndexMetaData = createIndexMetaData(indexName, newNumberOfShards, version);
            final ClusterState newClusterState = createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(createCoordinationMetaData(newTerm)).put(newIndexMetaData, false).build());
            gateway.setLastAcceptedState(newClusterState);

            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().metaData().index(indexName), equalTo(newIndexMetaData));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testCurrentTermAndTermAreDifferent() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            long currentTerm = randomNonNegativeLong();
            long term = randomValueOtherThan(currentTerm, ESTestCase::randomNonNegativeLong);

            gateway.setCurrentTerm(currentTerm);
            gateway.setLastAcceptedState(createClusterState(randomNonNegativeLong(),
                MetaData.builder().coordinationMetaData(CoordinationMetaData.builder().term(term).build()).build()));

            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
            assertThat(gateway.getLastAcceptedState().coordinationMetaData().term(), equalTo(term));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testMarkAcceptedConfigAsCommitted() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            // generate random coordinationMetaData with different lastAcceptedConfiguration and lastCommittedConfiguration
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
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testStatePersistedOnLoad() throws IOException {
        // open LucenePersistedState to make sure that cluster state is written out to each data path
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
        final ClusterState state = createClusterState(randomNonNegativeLong(),
            MetaData.builder().clusterUUID(randomAlphaOfLength(10)).build());
        try (GatewayMetaState.LucenePersistedState ignored = new GatewayMetaState.LucenePersistedState(
            persistedClusterStateService, 42L, state)) {

        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.toString()).build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService =
                    new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(42L));
                assertClusterStateEqual(state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metaData(onDiskState.metaData).build());
            }
        }
    }

    public void testDataOnlyNodePersistence() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        Settings settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).put(
            Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_NAME_SETTING.getKey(), "test").build();
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode);
        final TransportService transportService = mock(TransportService.class);
        TestThreadPool threadPool = new TestThreadPool("testMarkAcceptedConfigAsCommittedOnDataOnlyNode");
        when(transportService.getThreadPool()).thenReturn(threadPool);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
        gateway.start(settings, transportService, clusterService,
            new MetaStateService(nodeEnvironment, xContentRegistry()), null, null, persistedClusterStateService);
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState, instanceOf(GatewayMetaState.AsyncLucenePersistedState.class));

        //generate random coordinationMetaData with different lastAcceptedConfiguration and lastCommittedConfiguration
        CoordinationMetaData coordinationMetaData;
        do {
            coordinationMetaData = createCoordinationMetaData(randomNonNegativeLong());
        } while (coordinationMetaData.getLastAcceptedConfiguration().equals(coordinationMetaData.getLastCommittedConfiguration()));

        ClusterState state = createClusterState(randomNonNegativeLong(),
            MetaData.builder().coordinationMetaData(coordinationMetaData)
                .clusterUUID(randomAlphaOfLength(10)).build());
        persistedState.setLastAcceptedState(state);
        assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

        assertThat(persistedState.getLastAcceptedState().getLastAcceptedConfiguration(),
            not(equalTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration())));
        CoordinationMetaData persistedCoordinationMetaData =
            persistedClusterStateService.loadBestOnDiskState().metaData.coordinationMetaData();
        assertThat(persistedCoordinationMetaData.getLastAcceptedConfiguration(),
            equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration));
        assertThat(persistedCoordinationMetaData.getLastCommittedConfiguration(),
            equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration));

        persistedState.markLastAcceptedStateAsCommitted();
        assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

        CoordinationMetaData expectedCoordinationMetaData = CoordinationMetaData.builder(coordinationMetaData)
            .lastCommittedConfiguration(coordinationMetaData.getLastAcceptedConfiguration()).build();
        ClusterState expectedClusterState =
            ClusterState.builder(state).metaData(MetaData.builder().coordinationMetaData(expectedCoordinationMetaData)
                .clusterUUID(state.metaData().clusterUUID()).clusterUUIDCommitted(true).build()).build();

        assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());
        persistedCoordinationMetaData = persistedClusterStateService.loadBestOnDiskState().metaData.coordinationMetaData();
        assertThat(persistedCoordinationMetaData.getLastAcceptedConfiguration(),
            equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration));
        assertThat(persistedCoordinationMetaData.getLastCommittedConfiguration(),
            equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration));
        assertTrue(persistedClusterStateService.loadBestOnDiskState().metaData.clusterUUIDCommitted());

        // generate a series of updates and check if batching works
        final String indexName = randomAlphaOfLength(10);
        long currentTerm = state.term();
        for (int i = 0; i < 1000; i++) {
            if (rarely()) {
                // bump term
                currentTerm = currentTerm + (rarely() ? randomIntBetween(1, 5) : 0L);
                persistedState.setCurrentTerm(currentTerm);
            } else {
                // update cluster state
                final int numberOfShards = randomIntBetween(1, 5);
                final long term = Math.min(state.term() + (rarely() ? randomIntBetween(1, 5) : 0L), currentTerm);
                final IndexMetaData indexMetaData = createIndexMetaData(indexName, numberOfShards, i);
                state = createClusterState(state.version() + 1,
                    MetaData.builder().coordinationMetaData(createCoordinationMetaData(term)).put(indexMetaData, false).build());
                persistedState.setLastAcceptedState(state);
            }
        }
        assertEquals(currentTerm, persistedState.getCurrentTerm());
        assertClusterStateEqual(state, persistedState.getLastAcceptedState());
        assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

        gateway.close();

        try (CoordinationState.PersistedState reloadedPersistedState = newGatewayPersistedState()) {
            assertEquals(currentTerm, reloadedPersistedState.getCurrentTerm());
            assertClusterStateEqual(GatewayMetaState.AsyncLucenePersistedState.resetVotingConfiguration(state),
                reloadedPersistedState.getLastAcceptedState());
            assertNotNull(reloadedPersistedState.getLastAcceptedState().metaData().index(indexName));
        }

        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testStatePersistenceWithIOIssues() throws IOException {
        final AtomicReference<Double> ioExceptionRate = new AtomicReference<>(0.01d);
        final List<MockDirectoryWrapper> list = new ArrayList<>();
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L) {
                @Override
                Directory createDirectory(Path path) {
                    final MockDirectoryWrapper wrapper = newMockFSDirectory(path);
                    wrapper.setAllowRandomFileNotFoundException(randomBoolean());
                    wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                    wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
                    list.add(wrapper);
                    return wrapper;
                }
            };
        ClusterState state = createClusterState(randomNonNegativeLong(),
            MetaData.builder().clusterUUID(randomAlphaOfLength(10)).build());
        long currentTerm = 42L;
        try (GatewayMetaState.LucenePersistedState persistedState = new GatewayMetaState.LucenePersistedState(
            persistedClusterStateService, currentTerm, state)) {

            try {
                if (randomBoolean()) {
                    final ClusterState newState = createClusterState(randomNonNegativeLong(),
                        MetaData.builder().clusterUUID(randomAlphaOfLength(10)).build());
                    persistedState.setLastAcceptedState(newState);
                    state = newState;
                } else {
                    final long newTerm = currentTerm + 1;
                    persistedState.setCurrentTerm(newTerm);
                    currentTerm = newTerm;
                }
            } catch (IOError | Exception e) {
                assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
            }

            ioExceptionRate.set(0.0d);
            for (MockDirectoryWrapper wrapper : list) {
                wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
            }

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                if (randomBoolean()) {
                    final long version = randomNonNegativeLong();
                    final String indexName = randomAlphaOfLength(10);
                    final IndexMetaData indexMetaData = createIndexMetaData(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                    final MetaData metaData = MetaData.builder().
                        persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                        coordinationMetaData(createCoordinationMetaData(1L)).
                        put(indexMetaData, false).
                        build();
                    state = createClusterState(version, metaData);
                    persistedState.setLastAcceptedState(state);
                } else {
                    currentTerm += 1;
                    persistedState.setCurrentTerm(currentTerm);
                }
            }

            assertEquals(state, persistedState.getLastAcceptedState());
            assertEquals(currentTerm, persistedState.getCurrentTerm());

        } catch (IOError | Exception e) {
            if (ioExceptionRate.get() == 0.0d) {
                throw e;
            }
            assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
            return;
        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.toString()).build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService =
                    new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(currentTerm));
                assertClusterStateEqual(state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metaData(onDiskState.metaData).build());
            }
        }
    }

}
