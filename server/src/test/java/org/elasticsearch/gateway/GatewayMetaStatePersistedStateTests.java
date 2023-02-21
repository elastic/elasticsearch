/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.NodeRoles.nonMasterNode;
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
        localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );
        clusterName = new ClusterName(randomAlphaOfLength(10));
        final Settings.Builder settingsBuilder = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value());
        if (randomBoolean()) {
            settingsBuilder.put(
                PersistedClusterStateService.DOCUMENT_PAGE_SIZE.getKey(),
                ByteSizeValue.ofBytes(randomLongBetween(1, 1024))
            );
        }
        settings = settingsBuilder.build();
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
            assertTrue(Metadata.isGlobalStateEquals(state.metadata(), Metadata.EMPTY_METADATA));
            assertThat(state.getVersion(), equalTo(Manifest.empty().clusterStateVersion()));
            assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

            long currentTerm = gateway.getCurrentTerm();
            assertThat(currentTerm, equalTo(Manifest.empty().currentTerm()));
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

    private ClusterState createClusterState(long version, Metadata metadata) {
        return ClusterState.builder(clusterName)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .version(version)
            .metadata(metadata)
            .build();
    }

    private CoordinationMetadata createCoordinationMetadata(long term) {
        CoordinationMetadata.Builder builder = CoordinationMetadata.builder();
        builder.term(term);
        builder.lastAcceptedConfiguration(
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
        );
        builder.lastCommittedConfiguration(
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
        );
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            builder.addVotingConfigExclusion(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, int numberOfShards, long version) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexName)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            )
            .version(version)
            .build();
    }

    private void assertClusterStateEqual(ClusterState expected, ClusterState actual) {
        assertThat(actual.version(), equalTo(expected.version()));
        assertTrue(Metadata.isGlobalStateEquals(actual.metadata(), expected.metadata()));
        for (IndexMetadata indexMetadata : expected.metadata()) {
            assertThat(actual.metadata().index(indexMetadata.getIndex()), equalTo(indexMetadata));
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
                final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                final Metadata metadata = Metadata.builder()
                    .persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build())
                    .coordinationMetadata(createCoordinationMetadata(term))
                    .put(indexMetadata, false)
                    .build();
                ClusterState state = createClusterState(version, metadata);

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
            final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, version);
            final ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build()
            );
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            final long newTerm = randomLongBetween(term + 1, Long.MAX_VALUE);
            final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1, 5));
            final IndexMetadata newIndexMetadata = createIndexMetadata(indexName, newNumberOfShards, version);
            final ClusterState newClusterState = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(newTerm)).put(newIndexMetadata, false).build()
            );
            gateway.setLastAcceptedState(newClusterState);

            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().metadata().index(indexName), equalTo(newIndexMetadata));
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
            gateway.setLastAcceptedState(
                createClusterState(
                    randomNonNegativeLong(),
                    Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).build()
                )
            );

            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
            assertThat(gateway.getLastAcceptedState().coordinationMetadata().term(), equalTo(term));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testMarkAcceptedConfigAsCommitted() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            // generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
            CoordinationMetadata coordinationMetadata;
            do {
                coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
            } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

            ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(randomAlphaOfLength(10)).build()
            );
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            assertThat(
                gateway.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(gateway.getLastAcceptedState().getLastCommittedConfiguration()))
            );
            gateway.markLastAcceptedStateAsCommitted();

            CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration())
                .build();
            ClusterState expectedClusterState = ClusterState.builder(state)
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(expectedCoordinationMetadata)
                        .clusterUUID(state.metadata().clusterUUID())
                        .clusterUUIDCommitted(true)
                        .build()
                )
                .build();

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
        final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
        final ClusterState state = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build()
        );

        // noinspection EmptyTryBlock
        try (
            GatewayMetaState.LucenePersistedState ignored = new GatewayMetaState.LucenePersistedState(
                persistedClusterStateService,
                42L,
                state
            )
        ) {

        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.toString())
                .build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService = new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                );
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(42L));
                assertClusterStateEqual(
                    state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metadata(onDiskState.metadata)
                        .build()
                );
            }
        }
    }

    @TestIssueLogging(value = "org.elasticsearch.gateway:TRACE", issueUrl = "https://github.com/elastic/elasticsearch/issues/87952")
    public void testDataOnlyNodePersistence() throws Exception {
        final List<Closeable> cleanup = new ArrayList<>(2);

        try {
            DiscoveryNode localNode = new DiscoveryNode(
                "node1",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            Settings settings = Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value())
                .put(nonMasterNode())
                .put(Node.NODE_NAME_SETTING.getKey(), "test")
                .build();
            final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode);
            cleanup.add(gateway);
            final TransportService transportService = mock(TransportService.class);
            TestThreadPool threadPool = new TestThreadPool("testMarkAcceptedConfigAsCommittedOnDataOnlyNode");
            cleanup.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            when(transportService.getThreadPool()).thenReturn(threadPool);
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getClusterSettings()).thenReturn(
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            );
            gateway.start(
                settings,
                transportService,
                clusterService,
                new MetaStateService(nodeEnvironment, xContentRegistry()),
                null,
                null,
                persistedClusterStateService
            );
            final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
            assertThat(persistedState, instanceOf(GatewayMetaState.AsyncPersistedState.class));

            // generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
            CoordinationMetadata coordinationMetadata;
            do {
                coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
            } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

            ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(randomAlphaOfLength(10)).build()
            );
            persistedState.setCurrentTerm(state.term());
            persistedState.setLastAcceptedState(state);
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()), 30, TimeUnit.SECONDS);

            assertThat(
                persistedState.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration()))
            );
            CoordinationMetadata persistedCoordinationMetadata = persistedClusterStateService.loadBestOnDiskState(false).metadata
                .coordinationMetadata();
            assertThat(
                persistedCoordinationMetadata.getLastAcceptedConfiguration(),
                equalTo(GatewayMetaState.AsyncPersistedState.staleStateConfiguration)
            );
            assertThat(
                persistedCoordinationMetadata.getLastCommittedConfiguration(),
                equalTo(GatewayMetaState.AsyncPersistedState.staleStateConfiguration)
            );

            persistedState.markLastAcceptedStateAsCommitted();
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()), 30, TimeUnit.SECONDS);

            CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration())
                .build();
            ClusterState expectedClusterState = ClusterState.builder(state)
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(expectedCoordinationMetadata)
                        .clusterUUID(state.metadata().clusterUUID())
                        .clusterUUIDCommitted(true)
                        .build()
                )
                .build();

            assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());
            persistedCoordinationMetadata = persistedClusterStateService.loadBestOnDiskState(false).metadata.coordinationMetadata();
            assertThat(
                persistedCoordinationMetadata.getLastAcceptedConfiguration(),
                equalTo(GatewayMetaState.AsyncPersistedState.staleStateConfiguration)
            );
            assertThat(
                persistedCoordinationMetadata.getLastCommittedConfiguration(),
                equalTo(GatewayMetaState.AsyncPersistedState.staleStateConfiguration)
            );
            assertTrue(persistedClusterStateService.loadBestOnDiskState(false).metadata.clusterUUIDCommitted());

            // generate a series of updates and check if batching works
            final String indexName = randomAlphaOfLength(10);
            long currentTerm = state.term();
            boolean wroteState = false;
            final int iterations = randomIntBetween(1, 1000);
            for (int i = 0; i < iterations; i++) {
                final boolean mustWriteState = wroteState == false && i == iterations - 1;
                if (rarely() && mustWriteState == false) {
                    // bump term
                    currentTerm = currentTerm + (rarely() ? randomIntBetween(1, 5) : 0L);
                    persistedState.setCurrentTerm(currentTerm);
                } else {
                    // update cluster state
                    final int numberOfShards = randomIntBetween(1, 5);
                    final long term = Math.min(state.term() + (rarely() ? randomIntBetween(1, 5) : 0L), currentTerm);
                    final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, i);
                    state = createClusterState(
                        state.version() + 1,
                        Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build()
                    );
                    persistedState.setLastAcceptedState(state);
                    wroteState = true;
                }
            }
            assertTrue(wroteState); // must write it at least once
            assertEquals(currentTerm, persistedState.getCurrentTerm());
            assertClusterStateEqual(state, persistedState.getLastAcceptedState());
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()), 30, TimeUnit.SECONDS);

            gateway.close();
            assertTrue(cleanup.remove(gateway));

            try (CoordinationState.PersistedState reloadedPersistedState = newGatewayPersistedState()) {
                assertEquals(currentTerm, reloadedPersistedState.getCurrentTerm());
                assertClusterStateEqual(
                    GatewayMetaState.AsyncPersistedState.resetVotingConfiguration(state),
                    reloadedPersistedState.getLastAcceptedState()
                );
                assertNotNull(reloadedPersistedState.getLastAcceptedState().metadata().index(indexName));
            }
        } finally {
            IOUtils.close(cleanup);
        }
    }

    public void testStatePersistenceWithIOIssues() throws IOException {
        final AtomicReference<Double> ioExceptionRate = new AtomicReference<>(0.01d);
        final List<MockDirectoryWrapper> list = new ArrayList<>();
        final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        ) {
            @Override
            Directory createDirectory(Path path) {
                final MockDirectoryWrapper wrapper = newMockFSDirectory(path);
                wrapper.setAllowRandomFileNotFoundException(randomBoolean());
                wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
                list.add(wrapper);
                return wrapper;
            }

            @Override
            CheckedBiConsumer<Path, DirectoryReader, IOException> getAssertOnCommit() {
                // IO issues might prevent reloading the state to verify that it round-trips
                return null;
            }
        };
        ClusterState state = createClusterState(randomNonNegativeLong(), Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());
        long currentTerm = 42L;
        try (
            GatewayMetaState.LucenePersistedState persistedState = new GatewayMetaState.LucenePersistedState(
                persistedClusterStateService,
                currentTerm,
                state
            )
        ) {

            try {
                if (randomBoolean()) {
                    final ClusterState newState = createClusterState(
                        randomNonNegativeLong(),
                        Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build()
                    );
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

            for (int i = between(1, 5); 0 <= i; i--) {
                if (randomBoolean()) {
                    final long version = randomNonNegativeLong();
                    final String indexName = randomAlphaOfLength(10);
                    final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                    final Metadata metadata = Metadata.builder()
                        .persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build())
                        .coordinationMetadata(createCoordinationMetadata(1L))
                        .put(indexMetadata, false)
                        .build();
                    state = createClusterState(version, metadata);
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
                .put(Environment.PATH_DATA_SETTING.getKey(), path.toString())
                .build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService = new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                ) {
                    @Override
                    CheckedBiConsumer<Path, DirectoryReader, IOException> getAssertOnCommit() {
                        // IO issues might prevent reloading the state to verify that it round-trips
                        return null;
                    }
                };
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(currentTerm));
                assertClusterStateEqual(
                    state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metadata(onDiskState.metadata)
                        .build()
                );
            }
        }
    }

    public void testStatePersistenceWithFatalError() throws IOException {
        long currentTerm = 42L;
        ClusterState state = createClusterState(randomNonNegativeLong(), Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());

        final AtomicBoolean throwError = new AtomicBoolean();
        final AtomicBoolean openedFile = new AtomicBoolean();
        PathUtilsForTesting.installMock(new FilterFileSystemProvider("throwerror://", PathUtils.getDefaultFileSystem()) {
            @Override
            public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
                if (throwError.get() && path.getFileName().toString().startsWith("pending_segments_")) {
                    openedFile.set(true);
                    if (randomBoolean()) {
                        throw randomBoolean() ? new TestTragicError() : new TestComedicError();
                    }
                }
                return super.newOutputStream(path, options);
            }
        }.getFileSystem(null));

        final Path[] dataPaths;
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) { // a new node environment is needed to pick up the fake filesystem
            dataPaths = nodeEnvironment.nodeDataPaths();
            try (
                GatewayMetaState.LucenePersistedState persistedState = new GatewayMetaState.LucenePersistedState(
                    new PersistedClusterStateService(
                        nodeEnvironment,
                        xContentRegistry(),
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                        () -> 0L
                    ),
                    currentTerm,
                    state
                )
            ) {
                throwError.set(true);
                for (int i = between(1, 5); 0 <= i; i--) {
                    if (randomBoolean()) {
                        final ClusterState newState = createClusterState(
                            randomNonNegativeLong(),
                            Metadata.builder()
                                .clusterUUID(randomAlphaOfLength(10))
                                .coordinationMetadata(CoordinationMetadata.builder().term(currentTerm).build())
                                .build()
                        );
                        try {
                            persistedState.setLastAcceptedState(newState);
                            state = newState;
                        } catch (TestTragicError | TestComedicError e) {
                            logger.info("--> test error", e);
                            // ok
                        }
                    } else {
                        final long newTerm = currentTerm + 1;
                        try {
                            persistedState.setCurrentTerm(newTerm);
                            currentTerm = newTerm;
                        } catch (TestTragicError | TestComedicError e) {
                            logger.info("--> test error", e);
                            // ok
                        }
                    }
                }

                assertEquals(state, persistedState.getLastAcceptedState());
                assertEquals(currentTerm, persistedState.getCurrentTerm());
            }
        } finally {
            PathUtilsForTesting.teardown();
        }
        assertTrue(openedFile.get()); // make sure we had opportunity to throw an error

        nodeEnvironment.close();

        for (Path path : dataPaths) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.toString())
                .build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService = new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                );
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse("loaded state from " + path, onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(currentTerm));
                assertClusterStateEqual(
                    state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metadata(onDiskState.metadata)
                        .build()
                );
            }
        }
    }

    /**
     * An {@link Error} which is tragic.
     */
    private static final class TestTragicError extends VirtualMachineError {
        TestTragicError() {
            super("test tragic error");
        }
    }

    /**
     * An {@link Error} which is not tragic.
     */
    private static final class TestComedicError extends Error {
        TestComedicError() {
            super("test comedic error");
        }
    }
}
