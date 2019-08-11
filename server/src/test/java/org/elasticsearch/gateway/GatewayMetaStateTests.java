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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.test.TestCustomMetaData;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class GatewayMetaStateTests extends ESAllocationTestCase {

    private ClusterState noIndexClusterState(boolean masterEligible) {
        MetaData metaData = MetaData.builder().build();
        RoutingTable routingTable = RoutingTable.builder().build();

        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
    }

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

    private ClusterState clusterStateWithClosedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.CLOSE)
                        .numberOfShards(5).numberOfReplicas(2))
                .version(oldClusterState.metaData().version() + 1)
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaDataNewClusterState.index("test"))
                .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
                .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithJustOpenedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithClosedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.OPEN)
                        .numberOfShards(5).numberOfReplicas(2))
                .version(oldClusterState.metaData().version() + 1)
                .build();

        return ClusterState.builder(oldClusterState)
                .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private DiscoveryNodes.Builder generateDiscoveryNodes(boolean masterEligible) {
        Set<DiscoveryNodeRole> dataOnlyRoles = Set.of(DiscoveryNodeRole.DATA_ROLE);
        return DiscoveryNodes.builder().add(newNode("node1", masterEligible ? MASTER_DATA_ROLES : dataOnlyRoles))
                .add(newNode("master_node", MASTER_DATA_ROLES)).localNodeId("node1").masterNodeId(masterEligible ? "node1" : "master_node");
    }

    private Set<Index> randomPrevWrittenIndices(IndexMetaData indexMetaData) {
        if (randomBoolean()) {
            return Collections.singleton(indexMetaData.getIndex());
        } else {
            return Collections.emptySet();
        }
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
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithUnassignedIndex(indexMetaData, true),
                noIndexClusterState(true),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndicesWithUnassignedShardsOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithUnassignedIndex(indexMetaData, false),
                noIndexClusterState(false),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesWithAssignedShards() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        boolean masterEligible = randomBoolean();
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithAssignedIndex(indexMetaData, masterEligible),
                clusterStateWithUnassignedIndex(indexMetaData, masterEligible),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndicesForClosedPrevWrittenIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithClosedIndex(indexMetaData, false),
                clusterStateWithAssignedIndex(indexMetaData, false),
                Collections.singleton(indexMetaData.getIndex()));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndicesForClosedPrevNotWrittenIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithJustOpenedIndex(indexMetaData, false),
                clusterStateWithClosedIndex(indexMetaData, false),
                Collections.emptySet());
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndicesForWasClosedPrevWrittenIndexOnDataOnlyNode() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithJustOpenedIndex(indexMetaData, false),
                clusterStateWithClosedIndex(indexMetaData, false),
                Collections.singleton(indexMetaData.getIndex()));
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

        List<GatewayMetaState.IndexMetaDataAction> actions =
                GatewayMetaState.resolveIndexMetaDataActions(indices, relevantIndices, oldMetaData, newMetaData);

        assertThat(actions, hasSize(3));

        for (GatewayMetaState.IndexMetaDataAction action : actions) {
            if (action instanceof GatewayMetaState.KeepPreviousGeneration) {
                assertThat(action.getIndex(), equalTo(notChangedIndex.getIndex()));
                GatewayMetaState.AtomicClusterStateWriter writer = mock(GatewayMetaState.AtomicClusterStateWriter.class);
                assertThat(action.execute(writer), equalTo(3L));
                verifyZeroInteractions(writer);
            }
            if (action instanceof GatewayMetaState.WriteNewIndexMetaData) {
                assertThat(action.getIndex(), equalTo(newIndex.getIndex()));
                GatewayMetaState.AtomicClusterStateWriter writer = mock(GatewayMetaState.AtomicClusterStateWriter.class);
                when(writer.writeIndex("freshly created", newIndex)).thenReturn(0L);
                assertThat(action.execute(writer), equalTo(0L));
            }
            if (action instanceof GatewayMetaState.WriteChangedIndexMetaData) {
                assertThat(action.getIndex(), equalTo(newVersionChangedIndex.getIndex()));
                GatewayMetaState.AtomicClusterStateWriter writer = mock(GatewayMetaState.AtomicClusterStateWriter.class);
                when(writer.writeIndex(anyString(), eq(newVersionChangedIndex))).thenReturn(3L);
                assertThat(action.execute(writer), equalTo(3L));
                ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
                verify(writer).writeIndex(reason.capture(), eq(newVersionChangedIndex));
                assertThat(reason.getValue(), containsString(Long.toString(versionChangedIndex.getVersion())));
                assertThat(reason.getValue(), containsString(Long.toString(newVersionChangedIndex.getVersion())));
            }
        }
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
                GatewayMetaState.AtomicClusterStateWriter writer =
                        new GatewayMetaState.AtomicClusterStateWriter(metaStateService, manifest);
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

    public void testAddCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
    }

    public void testRemoveCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.remove(CustomMetaData1.TYPE);
                return customs;
            }),
            Collections.emptyList()
        );
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNull(upgrade.custom(CustomMetaData1.TYPE));
    }

    public void testUpdateCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );

        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
    }


    public void testUpdateTemplateMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                templates -> {
                    templates.put("added_test_template", IndexTemplateMetaData.builder("added_test_template")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false))).build());
                    return templates;
                }
            ));

        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetaDataUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList(), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade == metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataValidation() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(
            customs -> {
                throw new IllegalStateException("custom meta data too old");
            }
        ), Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testMultipleCustomMetaDataUpgrade() throws Exception {
        final MetaData metaData;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metaData = randomMetaData(new CustomMetaData1("data1"), new CustomMetaData2("data2"));
                break;
            case 1:
                metaData = randomMetaData(randomBoolean() ? new CustomMetaData1("data1") : new CustomMetaData2("data2"));
                break;
            case 2:
                metaData = randomMetaData();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Arrays.asList(
                customs -> {
                    customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                    return customs;
                },
                customs -> {
                    customs.put(CustomMetaData2.TYPE, new CustomMetaData1("modified_data2"));
                    return customs;
                }
            ), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
        assertNotNull(upgrade.custom(CustomMetaData2.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData2.TYPE)).getData(), equalTo("modified_data2"));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testIndexMetaDataUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList(), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(true), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertFalse(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataNoChange() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(HashMap::new),
            Collections.singletonList(HashMap::new));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade == metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testIndexTemplateValidation() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                customs -> {
                    throw new IllegalStateException("template is incompatible");
                }));
        String message = expectThrows(IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader)).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }


    public void testMultipleIndexTemplateUpgrade() throws Exception {
        final MetaData metaData;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metaData = randomMetaDataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metaData = randomMetaDataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metaData = randomMetaData();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Arrays.asList(
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template1", IndexTemplateMetaData.builder("template1")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                        .build());
                    return indexTemplateMetaDatas;

                },
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template2", IndexTemplateMetaData.builder("template2")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    return indexTemplateMetaDatas;

                }
            ));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    private static class MockMetaDataIndexUpgradeService extends MetaDataIndexUpgradeService {
        private final boolean upgrade;

        MockMetaDataIndexUpgradeService(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData, Version minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetaData.builder(indexMetaData).build() : indexMetaData;
        }
    }

    private static class CustomMetaData1 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_1";

        protected CustomMetaData1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetaData2 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_2";

        protected CustomMetaData2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static MetaData randomMetaData(TestCustomMetaData... customMetaDatas) {
        MetaData.Builder builder = MetaData.builder();
        for (TestCustomMetaData customMetaData : customMetaDatas) {
            builder.putCustom(customMetaData.getWriteableName(), customMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static MetaData randomMetaDataWithIndexTemplates(String... templates) {
        MetaData.Builder builder = MetaData.builder();
        for (String template : templates) {
            IndexTemplateMetaData templateMetaData = IndexTemplateMetaData.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                .build();
            builder.put(templateMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }
}
