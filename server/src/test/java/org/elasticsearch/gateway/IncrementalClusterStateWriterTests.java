/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IncrementalClusterStateWriterTests extends ESAllocationTestCase {

    private ClusterState clusterStateWithUnassignedIndex(IndexMetadata indexMetadata, boolean masterEligible) {
        Metadata metadata = Metadata.builder()
            .put(indexMetadata, false)
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(generateDiscoveryNodes(masterEligible))
            .build();
    }

    private ClusterState clusterStateWithAssignedIndex(IndexMetadata indexMetadata, boolean masterEligible) {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
            .build());

        ClusterState oldClusterState = clusterStateWithUnassignedIndex(indexMetadata, masterEligible);
        RoutingTable routingTable = strategy.reroute(oldClusterState, "reroute").routingTable();

        Metadata metadataNewClusterState = Metadata.builder()
            .put(oldClusterState.metadata().index("test"), false)
            .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metadata(metadataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithNonReplicatedClosedIndex(IndexMetadata indexMetadata, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetadata, masterEligible);

        Metadata metadataNewClusterState = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).state(IndexMetadata.State.CLOSE)
                .numberOfShards(5).numberOfReplicas(2))
            .version(oldClusterState.metadata().version() + 1)
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metadataNewClusterState.index("test"))
            .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metadata(metadataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithReplicatedClosedIndex(IndexMetadata indexMetadata, boolean masterEligible, boolean assigned) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetadata, masterEligible);

        Metadata metadataNewClusterState = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)
                .put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true))
                .state(IndexMetadata.State.CLOSE)
                .numberOfShards(5).numberOfReplicas(2))
            .version(oldClusterState.metadata().version() + 1)
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metadataNewClusterState.index("test"))
            .build();

        oldClusterState = ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metadata(metadataNewClusterState).build();
        if (assigned) {
            AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
                .build());

            routingTable = strategy.reroute(oldClusterState, "reroute").routingTable();
        }

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metadata(metadataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private DiscoveryNodes.Builder generateDiscoveryNodes(boolean masterEligible) {
        Set<DiscoveryNodeRole> dataOnlyRoles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return DiscoveryNodes.builder().add(newNode("node1", masterEligible ? MASTER_DATA_ROLES : dataOnlyRoles))
            .add(newNode("master_node", MASTER_DATA_ROLES)).localNodeId("node1").masterNodeId(masterEligible ? "node1" : "master_node");
    }

    private IndexMetadata createIndexMetadata(String name) {
        return IndexMetadata.builder(name).
            settings(settings(Version.CURRENT)).
            numberOfShards(5).
            numberOfReplicas(2).
            build();
    }

    public void testGetRelevantIndicesWithUnassignedShardsOnMasterEligibleNode() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithUnassignedIndex(indexMetadata, true));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesWithUnassignedShardsOnDataOnlyNode() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithUnassignedIndex(indexMetadata, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesWithAssignedShards() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        boolean masterEligible = randomBoolean();
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithAssignedIndex(indexMetadata, masterEligible));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndicesForNonReplicatedClosedIndexOnDataOnlyNode() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithNonReplicatedClosedIndex(indexMetadata, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesForReplicatedClosedButUnassignedIndexOnDataOnlyNode() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithReplicatedClosedIndex(indexMetadata, false, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesForReplicatedClosedAndAssignedIndexOnDataOnlyNode() {
        IndexMetadata indexMetadata = createIndexMetadata("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithReplicatedClosedIndex(indexMetadata, false, true));
        assertThat(indices.size(), equalTo(1));
    }

    public void testResolveStatesToBeWritten() throws WriteStateException {
        Map<Index, Long> indices = new HashMap<>();
        Set<Index> relevantIndices = new HashSet<>();

        IndexMetadata removedIndex = createIndexMetadata("removed_index");
        indices.put(removedIndex.getIndex(), 1L);

        IndexMetadata versionChangedIndex = createIndexMetadata("version_changed_index");
        indices.put(versionChangedIndex.getIndex(), 2L);
        relevantIndices.add(versionChangedIndex.getIndex());

        IndexMetadata notChangedIndex = createIndexMetadata("not_changed_index");
        indices.put(notChangedIndex.getIndex(), 3L);
        relevantIndices.add(notChangedIndex.getIndex());

        IndexMetadata newIndex = createIndexMetadata("new_index");
        relevantIndices.add(newIndex.getIndex());

        Metadata oldMetadata = Metadata.builder()
            .put(removedIndex, false)
            .put(versionChangedIndex, false)
            .put(notChangedIndex, false)
            .build();

        Metadata newMetadata = Metadata.builder()
            .put(versionChangedIndex, true)
            .put(notChangedIndex, false)
            .put(newIndex, false)
            .build();

        IndexMetadata newVersionChangedIndex = newMetadata.index(versionChangedIndex.getIndex());

        List<IncrementalClusterStateWriter.IndexMetadataAction> actions =
            IncrementalClusterStateWriter.resolveIndexMetadataActions(indices, relevantIndices, oldMetadata, newMetadata);

        assertThat(actions, hasSize(3));

        boolean keptPreviousGeneration = false;
        boolean wroteNewIndex = false;
        boolean wroteChangedIndex = false;

        for (IncrementalClusterStateWriter.IndexMetadataAction action : actions) {
            if (action instanceof IncrementalClusterStateWriter.KeepPreviousGeneration) {
                assertThat(action.getIndex(), equalTo(notChangedIndex.getIndex()));
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = mock(IncrementalClusterStateWriter.AtomicClusterStateWriter.class);
                assertThat(action.execute(writer), equalTo(3L));
                verify(writer, times(1)).incrementIndicesSkipped();
                verifyNoMoreInteractions(writer);
                keptPreviousGeneration = true;
            }
            if (action instanceof IncrementalClusterStateWriter.WriteNewIndexMetadata) {
                assertThat(action.getIndex(), equalTo(newIndex.getIndex()));
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = mock(IncrementalClusterStateWriter.AtomicClusterStateWriter.class);
                when(writer.writeIndex("freshly created", newIndex)).thenReturn(0L);
                assertThat(action.execute(writer), equalTo(0L));
                verify(writer, times(1)).incrementIndicesWritten();
                wroteNewIndex = true;
            }
            if (action instanceof IncrementalClusterStateWriter.WriteChangedIndexMetadata) {
                assertThat(action.getIndex(), equalTo(newVersionChangedIndex.getIndex()));
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = mock(IncrementalClusterStateWriter.AtomicClusterStateWriter.class);
                when(writer.writeIndex(anyString(), eq(newVersionChangedIndex))).thenReturn(3L);
                assertThat(action.execute(writer), equalTo(3L));
                ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
                verify(writer).writeIndex(reason.capture(), eq(newVersionChangedIndex));
                verify(writer, times(1)).incrementIndicesWritten();
                assertThat(reason.getValue(), containsString(Long.toString(versionChangedIndex.getVersion())));
                assertThat(reason.getValue(), containsString(Long.toString(newVersionChangedIndex.getVersion())));
                wroteChangedIndex = true;
            }
        }

        assertTrue(keptPreviousGeneration);
        assertTrue(wroteNewIndex);
        assertTrue(wroteChangedIndex);
    }

    private static class MetaStateServiceWithFailures extends MetaStateService {
        private final int invertedFailRate;
        private boolean failRandomly;

        private <T> MetadataStateFormat<T> wrap(MetadataStateFormat<T> format) {
            return new MetadataStateFormat<T>(format.getPrefix()) {
                @Override
                public void toXContent(XContentBuilder builder, T state) throws IOException {
                    format.toXContent(builder, state);
                }

                @Override
                public T fromXContent(XContentParser parser) throws IOException {
                    return format.fromXContent(parser);
                }

                @Override
                protected Directory newDirectory(Path dir) {
                    MockDirectoryWrapper mock = newMockFSDirectory(dir);
                    if (failRandomly) {
                        MockDirectoryWrapper.Failure fail = new MockDirectoryWrapper.Failure() {
                            @Override
                            public void eval(MockDirectoryWrapper dir) throws IOException {
                                int r = randomIntBetween(0, invertedFailRate);
                                if (r == 0) {
                                    throw new MockDirectoryWrapper.FakeIOException();
                                }
                            }
                        };
                        mock.failOn(fail);
                    }
                    closeAfterSuite(mock);
                    return mock;
                }
            };
        }

        MetaStateServiceWithFailures(int invertedFailRate, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
            super(nodeEnv, namedXContentRegistry);
            METADATA_FORMAT = wrap(Metadata.FORMAT);
            INDEX_METADATA_FORMAT = wrap(IndexMetadata.FORMAT);
            MANIFEST_FORMAT = wrap(Manifest.FORMAT);
            failRandomly = false;
            this.invertedFailRate = invertedFailRate;
        }

        void failRandomly() {
            failRandomly = true;
        }

        void noFailures() {
            failRandomly = false;
        }
    }

    private boolean metadataEquals(Metadata md1, Metadata md2) {
        boolean equals = Metadata.isGlobalStateEquals(md1, md2);

        for (IndexMetadata imd : md1) {
            IndexMetadata imd2 = md2.index(imd.getIndex());
            equals = equals && imd.equals(imd2);
        }

        for (IndexMetadata imd : md2) {
            IndexMetadata imd2 = md1.index(imd.getIndex());
            equals = equals && imd.equals(imd2);
        }
        return equals;
    }

    private static Metadata randomMetadataForTx() {
        int settingNo = randomIntBetween(0, 10);
        Metadata.Builder builder = Metadata.builder()
            .persistentSettings(Settings.builder().put("setting" + settingNo, randomAlphaOfLength(5)).build());
        int numOfIndices = randomIntBetween(0, 3);

        for (int i = 0; i < numOfIndices; i++) {
            int indexNo = randomIntBetween(0, 50);
            IndexMetadata indexMetadata = IndexMetadata.builder("index" + indexNo).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, "index" + indexNo)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            ).build();
            builder.put(indexMetadata, false);
        }
        return builder.build();
    }

    public void testAtomicityWithFailures() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateServiceWithFailures metaStateService =
                new MetaStateServiceWithFailures(randomIntBetween(100, 1000), env, xContentRegistry());

            // We only guarantee atomicity of writes, if there is initial Manifest file
            Manifest manifest = Manifest.empty();
            Metadata metadata = Metadata.EMPTY_METADATA;
            metaStateService.writeManifestAndCleanup("startup", Manifest.empty());
            long currentTerm = randomNonNegativeLong();
            long clusterStateVersion = randomNonNegativeLong();

            metaStateService.failRandomly();
            Set<Metadata> possibleMetadata = new HashSet<>();
            possibleMetadata.add(metadata);

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer =
                    new IncrementalClusterStateWriter.AtomicClusterStateWriter(metaStateService, manifest);
                metadata = randomMetadataForTx();
                Map<Index, Long> indexGenerations = new HashMap<>();

                try {
                    long globalGeneration = writer.writeGlobalState("global", metadata);

                    for (IndexMetadata indexMetadata : metadata) {
                        long generation = writer.writeIndex("index", indexMetadata);
                        indexGenerations.put(indexMetadata.getIndex(), generation);
                    }

                    Manifest newManifest = new Manifest(currentTerm, clusterStateVersion, globalGeneration, indexGenerations);
                    writer.writeManifestAndCleanup("manifest", newManifest);
                    possibleMetadata.clear();
                    possibleMetadata.add(metadata);
                    manifest = newManifest;
                } catch (WriteStateException e) {
                    if (e.isDirty()) {
                        possibleMetadata.add(metadata);
                        /*
                         * If dirty WriteStateException occurred, it's only safe to proceed if there is subsequent
                         * successful write of metadata and Manifest. We prefer to break here, not to over complicate test logic.
                         * See also MetadataStateFormat#testFailRandomlyAndReadAnyState, that does not break.
                         */
                        break;
                    }
                }
            }

            metaStateService.noFailures();

            Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
            Metadata loadedMetadata = manifestAndMetadata.v2();

            assertTrue(possibleMetadata.stream().anyMatch(md -> metadataEquals(md, loadedMetadata)));
        }
    }

    @TestLogging(value = "org.elasticsearch.gateway:WARN", reason = "to ensure that we log gateway events on WARN level")
    public void testSlowLogging() throws WriteStateException, IllegalAccessException {
        final long slowWriteLoggingThresholdMillis;
        final Settings settings;
        if (randomBoolean()) {
            slowWriteLoggingThresholdMillis = PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.get(Settings.EMPTY).millis();
            settings = Settings.EMPTY;
        } else {
            slowWriteLoggingThresholdMillis = randomLongBetween(2, 100000);
            settings = Settings.builder()
                .put(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(), slowWriteLoggingThresholdMillis + "ms")
                .build();
        }

        final DiscoveryNode localNode = newNode("node");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        final long startTimeMillis = randomLongBetween(0L, Long.MAX_VALUE - slowWriteLoggingThresholdMillis * 10);
        final AtomicLong currentTime = new AtomicLong(startTimeMillis);
        final AtomicLong writeDurationMillis = new AtomicLong(slowWriteLoggingThresholdMillis);

        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IncrementalClusterStateWriter incrementalClusterStateWriter
            = new IncrementalClusterStateWriter(settings, clusterSettings, mock(MetaStateService.class),
            new Manifest(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), Collections.emptyMap()),
            clusterState, () -> currentTime.getAndAdd(writeDurationMillis.get()));

        assertExpectedLogs(clusterState, incrementalClusterStateWriter, new MockLogAppender.SeenEventExpectation(
            "should see warning at threshold",
            IncrementalClusterStateWriter.class.getCanonicalName(),
            Level.WARN,
            "writing cluster state took [*] which is above the warn threshold of [*]; " +
                "wrote metadata for [0] indices and skipped [0] unchanged indices"));

        writeDurationMillis.set(randomLongBetween(slowWriteLoggingThresholdMillis, slowWriteLoggingThresholdMillis * 2));
        assertExpectedLogs(clusterState, incrementalClusterStateWriter, new MockLogAppender.SeenEventExpectation(
            "should see warning above threshold",
            IncrementalClusterStateWriter.class.getCanonicalName(),
            Level.WARN,
            "writing cluster state took [*] which is above the warn threshold of [*]; " +
                "wrote metadata for [0] indices and skipped [0] unchanged indices"));

        writeDurationMillis.set(randomLongBetween(1, slowWriteLoggingThresholdMillis - 1));
        assertExpectedLogs(clusterState, incrementalClusterStateWriter, new MockLogAppender.UnseenEventExpectation(
            "should not see warning below threshold",
            IncrementalClusterStateWriter.class.getCanonicalName(),
            Level.WARN,
            "*"));

        clusterSettings.applySettings(Settings.builder()
            .put(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(), writeDurationMillis.get() + "ms")
            .build());
        assertExpectedLogs(clusterState, incrementalClusterStateWriter, new MockLogAppender.SeenEventExpectation(
            "should see warning at reduced threshold",
            IncrementalClusterStateWriter.class.getCanonicalName(),
            Level.WARN,
            "writing cluster state took [*] which is above the warn threshold of [*]; " +
                "wrote metadata for [0] indices and skipped [0] unchanged indices"));

        assertThat(currentTime.get(), lessThan(startTimeMillis + 10 * slowWriteLoggingThresholdMillis)); // ensure no overflow
    }

    private void assertExpectedLogs(ClusterState clusterState, IncrementalClusterStateWriter incrementalClusterStateWriter,
                                    MockLogAppender.LoggingExpectation expectation) throws IllegalAccessException, WriteStateException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(expectation);
        Logger classLogger = LogManager.getLogger(IncrementalClusterStateWriter.class);
        Loggers.addAppender(classLogger, mockAppender);

        try {
            incrementalClusterStateWriter.updateClusterState(clusterState);
        } finally {
            Loggers.removeAppender(classLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }
}
