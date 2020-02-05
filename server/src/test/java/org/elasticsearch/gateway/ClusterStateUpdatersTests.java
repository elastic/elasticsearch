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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.MetaData.CLUSTER_READ_ONLY_BLOCK;
import static org.elasticsearch.gateway.ClusterStateUpdaters.addStateNotRecoveredBlock;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.ClusterStateUpdaters.mixCurrentStateAndRecoveredState;
import static org.elasticsearch.gateway.ClusterStateUpdaters.recoverClusterBlocks;
import static org.elasticsearch.gateway.ClusterStateUpdaters.removeStateNotRecoveredBlock;
import static org.elasticsearch.gateway.ClusterStateUpdaters.setLocalNode;
import static org.elasticsearch.gateway.ClusterStateUpdaters.updateRoutingTable;
import static org.elasticsearch.gateway.ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ClusterStateUpdatersTests extends ESTestCase {

    public void testUpgradePersistentSettings() {
        runUpgradeSettings(MetaData.Builder::persistentSettings, MetaData::persistentSettings);
    }

    public void testUpgradeTransientSettings() {
        runUpgradeSettings(MetaData.Builder::transientSettings, MetaData::transientSettings);
    }

    private void runUpgradeSettings(final BiConsumer<MetaData.Builder, Settings> applySettingsToBuilder,
                                    final Function<MetaData, Settings> metaDataSettings) {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Set<Setting<?>> settingsSet =
                Stream.concat(
                        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                        Stream.of(oldSetting, newSetting)).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(
                Settings.EMPTY,
                settingsSet,
                Collections.singleton(new SettingUpgrader<String>() {

                    @Override
                    public Setting<String> getSetting() {
                        return oldSetting;
                    }

                    @Override
                    public String getKey(final String key) {
                        return "foo.new";
                    }

                    @Override
                    public String getValue(final String value) {
                        return "new." + value;
                    }

                }));
        final ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, null);
        final MetaData.Builder builder = MetaData.builder();
        final Settings settings = Settings.builder().put("foo.old", randomAlphaOfLength(8)).build();
        applySettingsToBuilder.accept(builder, settings);
        final ClusterState initialState = ClusterState.builder(clusterService.getClusterName()).metaData(builder.build()).build();
        final ClusterState state = upgradeAndArchiveUnknownOrInvalidSettings(initialState, clusterService.getClusterSettings());

        assertFalse(oldSetting.exists(metaDataSettings.apply(state.metaData())));
        assertTrue(newSetting.exists(metaDataSettings.apply(state.metaData())));
        assertThat(newSetting.get(metaDataSettings.apply(state.metaData())), equalTo("new." + oldSetting.get(settings)));
    }

    private IndexMetaData createIndexMetaData(final String name, final Settings settings) {
        return IndexMetaData.builder(name).settings(
                Settings.builder()
                        .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(settings))
                .build();
    }

    private static void assertMetaDataEquals(final ClusterState state1, final ClusterState state2) {
        assertTrue(MetaData.isGlobalStateEquals(state1.metaData(), state2.metaData()));
        assertThat(state1.metaData().indices().size(), equalTo(state2.metaData().indices().size()));
        for (final IndexMetaData indexMetaData : state1.metaData()) {
            assertThat(indexMetaData, equalTo(state2.metaData().index(indexMetaData.getIndex())));
        }
    }

    public void testRecoverClusterBlocks() {
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        final Settings.Builder transientSettings = Settings.builder();
        final Settings.Builder persistentSettings = Settings.builder();

        if (randomBoolean()) {
            persistentSettings.put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), true);
        } else {
            transientSettings.put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), true);
        }

        if (randomBoolean()) {
            persistentSettings.put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        } else {
            transientSettings.put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        }

        final IndexMetaData indexMetaData = createIndexMetaData("test",
                Settings.builder().put(IndexMetaData.INDEX_BLOCKS_READ_SETTING.getKey(), true).build());
        metaDataBuilder.put(indexMetaData, false);
        final MetaData metaData =
                metaDataBuilder.transientSettings(transientSettings.build()).persistentSettings(persistentSettings.build()).build();

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(metaData).build();
        final ClusterState newState = recoverClusterBlocks(initialState);

        assertMetaDataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        assertTrue(newState.blocks().hasGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertTrue(newState.blocks().hasIndexBlock("test", IndexMetaData.INDEX_READ_BLOCK));
    }

    public void testRemoveStateNotRecoveredBlock() {
        final MetaData.Builder metaDataBuilder = MetaData.builder()
                .persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetaData indexMetaData = createIndexMetaData("test", Settings.EMPTY);
        metaDataBuilder.put(indexMetaData, false);

        final ClusterState initialState = ClusterState
                .builder(ClusterState.EMPTY_STATE)
                .metaData(metaDataBuilder)
                .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
                .build();
        assertTrue(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = removeStateNotRecoveredBlock(initialState);

        assertMetaDataEquals(initialState, newState);
        assertFalse(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testAddStateNotRecoveredBlock() {
        final MetaData.Builder metaDataBuilder = MetaData.builder()
                .persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetaData indexMetaData = createIndexMetaData("test", Settings.EMPTY);
        metaDataBuilder.put(indexMetaData, false);

        final ClusterState initialState = ClusterState
                .builder(ClusterState.EMPTY_STATE)
                .metaData(metaDataBuilder)
                .build();
        assertFalse(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = addStateNotRecoveredBlock(initialState);

        assertMetaDataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testUpdateRoutingTable() {
        final int numOfShards = randomIntBetween(1, 10);

        final IndexMetaData metaData = createIndexMetaData("test",
                Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numOfShards)
                        .build());
        final Index index = metaData.getIndex();
        final ClusterState initialState = ClusterState
                .builder(ClusterState.EMPTY_STATE)
                .metaData(MetaData.builder().put(metaData, false).build())
                .build();
        assertFalse(initialState.routingTable().hasIndex(index));

        {
            final ClusterState newState = updateRoutingTable(initialState);
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
        {
            final ClusterState newState = updateRoutingTable(ClusterState.builder(initialState)
                .metaData(MetaData.builder(initialState.metaData())
                    .put(IndexMetaData.builder(initialState.metaData().index("test"))
                        .state(IndexMetaData.State.CLOSE))
                    .build())
                .build());
            assertFalse(newState.routingTable().hasIndex(index));
        }
        {
            final ClusterState newState = updateRoutingTable(ClusterState.builder(initialState)
                .metaData(MetaData.builder(initialState.metaData())
                    .put(IndexMetaData.builder(initialState.metaData().index("test"))
                        .state(IndexMetaData.State.CLOSE)
                        .settings(Settings.builder()
                            .put(initialState.metaData().index("test").getSettings())
                            .put(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
                            .build())
                    ).build())
                .build());
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
    }

    public void testMixCurrentAndRecoveredState() {
        final ClusterState currentState = ClusterState
                .builder(ClusterState.EMPTY_STATE)
                .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
                .build();
        final IndexMetaData indexMetaData = createIndexMetaData("test", Settings.EMPTY);
        final MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test", "test").build())
                .put(indexMetaData, false)
                .build();
        final ClusterState recoveredState = ClusterState
                .builder(ClusterState.EMPTY_STATE)
                .blocks(ClusterBlocks.builder().addGlobalBlock(CLUSTER_READ_ONLY_BLOCK).build())
                .metaData(metaData)
                .build();
        assertThat(recoveredState.metaData().clusterUUID(), equalTo(MetaData.UNKNOWN_CLUSTER_UUID));

        final ClusterState updatedState = mixCurrentStateAndRecoveredState(currentState, recoveredState);

        assertThat(updatedState.metaData().clusterUUID(), not(equalTo(MetaData.UNKNOWN_CLUSTER_UUID)));
        assertFalse(MetaData.isGlobalStateEquals(metaData, updatedState.metaData()));
        assertThat(updatedState.metaData().index("test"), equalTo(indexMetaData));
        assertTrue(updatedState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertTrue(updatedState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    public void testSetLocalNode() {
        final IndexMetaData indexMetaData = createIndexMetaData("test", Settings.EMPTY);
        final MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test", "test").build())
                .put(indexMetaData, false)
                .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metaData(metaData)
                .build();
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);

        final ClusterState updatedState = setLocalNode(initialState, localNode);

        assertMetaDataEquals(initialState, updatedState);
        assertThat(updatedState.nodes().getLocalNode(), equalTo(localNode));
        assertThat(updatedState.nodes().getSize(), is(1));
    }

    public void testDoNotHideStateIfRecovered() {
        final IndexMetaData indexMetaData = createIndexMetaData("test", Settings.EMPTY);
        final MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test", "test").build())
                .put(indexMetaData, false)
                .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metaData(metaData)
                .build();
        assertMetaDataEquals(initialState, hideStateIfNotRecovered(initialState));
    }

    public void testHideStateIfNotRecovered() {
        final IndexMetaData indexMetaData = createIndexMetaData("test",
                Settings.builder().put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), true).build());
        final String clusterUUID = UUIDs.randomBase64UUID();
        final CoordinationMetaData coordinationMetaData = new CoordinationMetaData(randomLong(),
                new CoordinationMetaData.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
                new CoordinationMetaData.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
                Arrays.stream(generateRandomStringArray(5, 5, false))
                        .map(id -> new CoordinationMetaData.VotingConfigExclusion(id, id))
                        .collect(Collectors.toSet()));
        final MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), true).build())
                .transientSettings(Settings.builder().put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build())
                .clusterUUID(clusterUUID)
                .coordinationMetaData(coordinationMetaData)
                .put(indexMetaData, false)
                .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metaData(metaData)
                .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
                .build();
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        final ClusterState updatedState = Function.<ClusterState>identity()
                .andThen(state -> setLocalNode(state, localNode))
                .andThen(ClusterStateUpdaters::recoverClusterBlocks)
                .apply(initialState);

        final ClusterState hiddenState = hideStateIfNotRecovered(updatedState);

        assertTrue(MetaData.isGlobalStateEquals(hiddenState.metaData(),
                MetaData.builder().coordinationMetaData(coordinationMetaData).clusterUUID(clusterUUID).build()));
        assertThat(hiddenState.metaData().indices().size(), is(0));
        assertTrue(hiddenState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertFalse(hiddenState.blocks().hasIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_READ_ONLY_BLOCK));
    }

}
