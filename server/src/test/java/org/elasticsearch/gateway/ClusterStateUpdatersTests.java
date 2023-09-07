/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.Metadata.CLUSTER_READ_ONLY_BLOCK;
import static org.elasticsearch.gateway.ClusterStateUpdaters.addStateNotRecoveredBlock;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.ClusterStateUpdaters.mixCurrentStateAndRecoveredState;
import static org.elasticsearch.gateway.ClusterStateUpdaters.recoverClusterBlocks;
import static org.elasticsearch.gateway.ClusterStateUpdaters.removeStateNotRecoveredBlock;
import static org.elasticsearch.gateway.ClusterStateUpdaters.setLocalNode;
import static org.elasticsearch.gateway.ClusterStateUpdaters.updateRoutingTable;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ClusterStateUpdatersTests extends ESTestCase {

    private IndexMetadata createIndexMetadata(final String name, final Settings settings) {
        return IndexMetadata.builder(name)
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).put(settings)
            )
            .build();
    }

    private static void assertMetadataEquals(final ClusterState state1, final ClusterState state2) {
        assertTrue(Metadata.isGlobalStateEquals(state1.metadata(), state2.metadata()));
        assertThat(state1.metadata().indices().size(), equalTo(state2.metadata().indices().size()));
        for (final IndexMetadata indexMetadata : state1.metadata()) {
            assertThat(indexMetadata, equalTo(state2.metadata().index(indexMetadata.getIndex())));
        }
    }

    public void testRecoverClusterBlocks() {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        final Settings.Builder transientSettings = Settings.builder();
        final Settings.Builder persistentSettings = Settings.builder();

        if (randomBoolean()) {
            persistentSettings.put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true);
        } else {
            transientSettings.put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true);
        }

        if (randomBoolean()) {
            persistentSettings.put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        } else {
            transientSettings.put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        }

        final IndexMetadata indexMetadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true).build()
        );
        metadataBuilder.put(indexMetadata, false);
        final Metadata metadata = metadataBuilder.transientSettings(transientSettings.build())
            .persistentSettings(persistentSettings.build())
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final ClusterState newState = recoverClusterBlocks(initialState);

        assertMetadataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        assertTrue(newState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertTrue(newState.blocks().hasIndexBlock("test", IndexMetadata.INDEX_READ_BLOCK));
    }

    public void testRemoveStateNotRecoveredBlock() {
        final Metadata.Builder metadataBuilder = Metadata.builder().persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        metadataBuilder.put(indexMetadata, false);

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadataBuilder)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        assertTrue(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = removeStateNotRecoveredBlock(initialState);

        assertMetadataEquals(initialState, newState);
        assertFalse(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testAddStateNotRecoveredBlock() {
        final Metadata.Builder metadataBuilder = Metadata.builder().persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        metadataBuilder.put(indexMetadata, false);

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadataBuilder).build();
        assertFalse(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = addStateNotRecoveredBlock(initialState);

        assertMetadataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testUpdateRoutingTable() {
        final int numOfShards = randomIntBetween(1, 10);

        final IndexMetadata metadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards).build()
        );
        final Index index = metadata.getIndex();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(metadata, false).build())
            .build();
        assertFalse(initialState.routingTable().hasIndex(index));

        {
            final ClusterState newState = updateRoutingTable(initialState, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .metadata(
                        Metadata.builder(initialState.metadata())
                            .put(IndexMetadata.builder(initialState.metadata().index("test")).state(IndexMetadata.State.CLOSE))
                            .build()
                    )
                    .build(),
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            );
            assertFalse(newState.routingTable().hasIndex(index));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .metadata(
                        Metadata.builder(initialState.metadata())
                            .put(
                                IndexMetadata.builder(initialState.metadata().index("test"))
                                    .state(IndexMetadata.State.CLOSE)
                                    .settings(
                                        Settings.builder()
                                            .put(initialState.metadata().index("test").getSettings())
                                            .put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
                                            .build()
                                    )
                            )
                            .build()
                    )
                    .build(),
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            );
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
    }

    public void testMixCurrentAndRecoveredState() {
        final ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState recoveredState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(CLUSTER_READ_ONLY_BLOCK).build())
            .metadata(metadata)
            .build();
        assertThat(recoveredState.metadata().clusterUUID(), equalTo(Metadata.UNKNOWN_CLUSTER_UUID));

        final ClusterState updatedState = mixCurrentStateAndRecoveredState(currentState, recoveredState);

        assertThat(updatedState.metadata().clusterUUID(), not(equalTo(Metadata.UNKNOWN_CLUSTER_UUID)));
        assertFalse(Metadata.isGlobalStateEquals(metadata, updatedState.metadata()));
        assertThat(updatedState.metadata().index("test"), equalTo(indexMetadata));
        assertTrue(updatedState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertTrue(updatedState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    public void testSetLocalNode() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE)).build();

        final ClusterState updatedState = setLocalNode(initialState, localNode, TransportVersion.current());

        assertMetadataEquals(initialState, updatedState);
        assertThat(updatedState.nodes().getLocalNode(), equalTo(localNode));
        assertThat(updatedState.nodes().getSize(), is(1));
    }

    public void testDoNotHideStateIfRecovered() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        assertMetadataEquals(initialState, hideStateIfNotRecovered(initialState));
    }

    public void testHideStateIfNotRecovered() {
        final IndexMetadata indexMetadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true).build()
        );
        final String clusterUUID = UUIDs.randomBase64UUID();
        final CoordinationMetadata coordinationMetadata = new CoordinationMetadata(
            randomLong(),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
            Arrays.stream(generateRandomStringArray(5, 5, false))
                .map(id -> new CoordinationMetadata.VotingConfigExclusion(id, id))
                .collect(Collectors.toSet())
        );
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .transientSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build())
            .clusterUUID(clusterUUID)
            .coordinationMetadata(coordinationMetadata)
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadata)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .build();
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE)).build();
        final ClusterState updatedState = Function.<ClusterState>identity()
            .andThen(state -> setLocalNode(state, localNode, TransportVersion.current()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(initialState);

        final ClusterState hiddenState = hideStateIfNotRecovered(updatedState);

        assertTrue(
            Metadata.isGlobalStateEquals(
                hiddenState.metadata(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(clusterUUID).build()
            )
        );
        assertThat(hiddenState.metadata().indices().size(), is(0));
        assertTrue(hiddenState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertFalse(hiddenState.blocks().hasIndexBlock(indexMetadata.getIndex().getName(), IndexMetadata.INDEX_READ_ONLY_BLOCK));
    }

}
