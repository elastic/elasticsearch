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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.collect.Tuple;
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

    private ClusterState clusterStateWithUnassignedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        MetaData metaData = MetaData.builder()
            .put(indexMetaData, false)
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();

        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData)
            .routingTable(routingTable)
            .nodes(generateDiscoveryNodes(masterEligible))
            .build();
    }

    private ClusterState clusterStateWithAssignedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
            .build());

        ClusterState oldClusterState = clusterStateWithUnassignedIndex(indexMetaData, masterEligible);
        RoutingTable routingTable = strategy.reroute(oldClusterState, "reroute").routingTable();

        MetaData metaDataNewClusterState = MetaData.builder()
            .put(oldClusterState.metaData().index("test"), false)
            .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithNonReplicatedClosedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.CLOSE)
                .numberOfShards(5).numberOfReplicas(2))
            .version(oldClusterState.metaData().version() + 1)
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metaDataNewClusterState.index("test"))
            .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithReplicatedClosedIndex(IndexMetaData indexMetaData, boolean masterEligible, boolean assigned) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)
                .put(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true))
                .state(IndexMetaData.State.CLOSE)
                .numberOfShards(5).numberOfReplicas(2))
            .version(oldClusterState.metaData().version() + 1)
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRecovery(metaDataNewClusterState.index("test"))
            .build();

        oldClusterState = ClusterState.builder(oldClusterState).routingTable(routingTable)
            .metaData(metaDataNewClusterState).build();
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
            .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private DiscoveryNodes.Builder generateDiscoveryNodes(boolean masterEligible) {
        Set<DiscoveryNodeRole> dataOnlyRoles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return DiscoveryNodes.builder().add(newNode("node1", masterEligible ? MASTER_DATA_ROLES : dataOnlyRoles))
            .add(newNode("master_node", MASTER_DATA_ROLES)).localNodeId("node1").masterNodeId(masterEligible ? "node1" : "master_node");
    }

    private IndexMetaData createIndexMetaData(String name) {
        return IndexMetaData.builder(name).
            settings(settings(Version.CURRENT)).
            numberOfShards(5).
            numberOfReplicas(2).
            build();
    }

    public void testGetRelevantIndicesWithUnassignedShardsOnMasterEligibleNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithUnassignedIndex(indexMetaData, true));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesWithUnassignedShardsOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithUnassignedIndex(indexMetaData, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesWithAssignedShards() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        boolean masterEligible = randomBoolean();
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(clusterStateWithAssignedIndex(indexMetaData, masterEligible));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndicesForNonReplicatedClosedIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithNonReplicatedClosedIndex(indexMetaData, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesForReplicatedClosedButUnassignedIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithReplicatedClosedIndex(indexMetaData, false, false));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesForReplicatedClosedAndAssignedIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = IncrementalClusterStateWriter.getRelevantIndices(
            clusterStateWithReplicatedClosedIndex(indexMetaData, false, true));
        assertThat(indices.size(), equalTo(1));
    }

    public void testResolveStatesToBeWritten() throws WriteStateException {
        Map<Index, Long> indices = new HashMap<>();
        Set<Index> relevantIndices = new HashSet<>();

        IndexMetaData removedIndex = createIndexMetaData("removed_index");
        indices.put(removedIndex.getIndex(), 1L);

        IndexMetaData versionChangedIndex = createIndexMetaData("version_changed_index");
        indices.put(versionChangedIndex.getIndex(), 2L);
        relevantIndices.add(versionChangedIndex.getIndex());

        IndexMetaData notChangedIndex = createIndexMetaData("not_changed_index");
        indices.put(notChangedIndex.getIndex(), 3L);
        relevantIndices.add(notChangedIndex.getIndex());

        IndexMetaData newIndex = createIndexMetaData("new_index");
        relevantIndices.add(newIndex.getIndex());

        MetaData oldMetaData = MetaData.builder()
            .put(removedIndex, false)
            .put(versionChangedIndex, false)
            .put(notChangedIndex, false)
            .build();

        MetaData newMetaData = MetaData.builder()
            .put(versionChangedIndex, true)
            .put(notChangedIndex, false)
            .put(newIndex, false)
            .build();

        IndexMetaData newVersionChangedIndex = newMetaData.index(versionChangedIndex.getIndex());

        List<IncrementalClusterStateWriter.IndexMetaDataAction> actions =
            IncrementalClusterStateWriter.resolveIndexMetaDataActions(indices, relevantIndices, oldMetaData, newMetaData);

        assertThat(actions, hasSize(3));

        boolean keptPreviousGeneration = false;
        boolean wroteNewIndex = false;
        boolean wroteChangedIndex = false;

        for (IncrementalClusterStateWriter.IndexMetaDataAction action : actions) {
            if (action instanceof IncrementalClusterStateWriter.KeepPreviousGeneration) {
                assertThat(action.getIndex(), equalTo(notChangedIndex.getIndex()));
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = mock(IncrementalClusterStateWriter.AtomicClusterStateWriter.class);
                assertThat(action.execute(writer), equalTo(3L));
                verify(writer, times(1)).incrementIndicesSkipped();
                verifyNoMoreInteractions(writer);
                keptPreviousGeneration = true;
            }
            if (action instanceof IncrementalClusterStateWriter.WriteNewIndexMetaData) {
                assertThat(action.getIndex(), equalTo(newIndex.getIndex()));
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = mock(IncrementalClusterStateWriter.AtomicClusterStateWriter.class);
                when(writer.writeIndex("freshly created", newIndex)).thenReturn(0L);
                assertThat(action.execute(writer), equalTo(0L));
                verify(writer, times(1)).incrementIndicesWritten();
                wroteNewIndex = true;
            }
            if (action instanceof IncrementalClusterStateWriter.WriteChangedIndexMetaData) {
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

        private <T> MetaDataStateFormat<T> wrap(MetaDataStateFormat<T> format) {
            return new MetaDataStateFormat<T>(format.getPrefix()) {
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
            META_DATA_FORMAT = wrap(MetaData.FORMAT);
            INDEX_META_DATA_FORMAT = wrap(IndexMetaData.FORMAT);
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

    private boolean metaDataEquals(MetaData md1, MetaData md2) {
        boolean equals = MetaData.isGlobalStateEquals(md1, md2);

        for (IndexMetaData imd : md1) {
            IndexMetaData imd2 = md2.index(imd.getIndex());
            equals = equals && imd.equals(imd2);
        }

        for (IndexMetaData imd : md2) {
            IndexMetaData imd2 = md1.index(imd.getIndex());
            equals = equals && imd.equals(imd2);
        }
        return equals;
    }

    private static MetaData randomMetaDataForTx() {
        int settingNo = randomIntBetween(0, 10);
        MetaData.Builder builder = MetaData.builder()
            .persistentSettings(Settings.builder().put("setting" + settingNo, randomAlphaOfLength(5)).build());
        int numOfIndices = randomIntBetween(0, 3);

        for (int i = 0; i < numOfIndices; i++) {
            int indexNo = randomIntBetween(0, 50);
            IndexMetaData indexMetaData = IndexMetaData.builder("index" + indexNo).settings(
                Settings.builder()
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index" + indexNo)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            ).build();
            builder.put(indexMetaData, false);
        }
        return builder.build();
    }

    public void testAtomicityWithFailures() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateServiceWithFailures metaStateService =
                new MetaStateServiceWithFailures(randomIntBetween(100, 1000), env, xContentRegistry());

            // We only guarantee atomicity of writes, if there is initial Manifest file
            Manifest manifest = Manifest.empty();
            MetaData metaData = MetaData.EMPTY_META_DATA;
            metaStateService.writeManifestAndCleanup("startup", Manifest.empty());
            long currentTerm = randomNonNegativeLong();
            long clusterStateVersion = randomNonNegativeLong();

            metaStateService.failRandomly();
            Set<MetaData> possibleMetaData = new HashSet<>();
            possibleMetaData.add(metaData);

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                IncrementalClusterStateWriter.AtomicClusterStateWriter writer =
                    new IncrementalClusterStateWriter.AtomicClusterStateWriter(metaStateService, manifest);
                metaData = randomMetaDataForTx();
                Map<Index, Long> indexGenerations = new HashMap<>();

                try {
                    long globalGeneration = writer.writeGlobalState("global", metaData);

                    for (IndexMetaData indexMetaData : metaData) {
                        long generation = writer.writeIndex("index", indexMetaData);
                        indexGenerations.put(indexMetaData.getIndex(), generation);
                    }

                    Manifest newManifest = new Manifest(currentTerm, clusterStateVersion, globalGeneration, indexGenerations);
                    writer.writeManifestAndCleanup("manifest", newManifest);
                    possibleMetaData.clear();
                    possibleMetaData.add(metaData);
                    manifest = newManifest;
                } catch (WriteStateException e) {
                    if (e.isDirty()) {
                        possibleMetaData.add(metaData);
                        /*
                         * If dirty WriteStateException occurred, it's only safe to proceed if there is subsequent
                         * successful write of metadata and Manifest. We prefer to break here, not to over complicate test logic.
                         * See also MetaDataStateFormat#testFailRandomlyAndReadAnyState, that does not break.
                         */
                        break;
                    }
                }
            }

            metaStateService.noFailures();

            Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
            MetaData loadedMetaData = manifestAndMetaData.v2();

            assertTrue(possibleMetaData.stream().anyMatch(md -> metaDataEquals(md, loadedMetaData)));
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
