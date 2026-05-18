/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
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
        assertThat(state1.metadata().getTotalNumberOfIndices(), equalTo(state2.metadata().getTotalNumberOfIndices()));
        for (var projectMetadata : state1.metadata().projects().values()) {
            for (final IndexMetadata indexMetadata : projectMetadata) {
                assertThat(state2.metadata().hasProject(projectMetadata.id()), is(true));
                assertThat(indexMetadata, equalTo(state2.metadata().getProject(projectMetadata.id()).index(indexMetadata.getIndex())));
            }
        }
    }

    public void testRecoverClusterBlocks() {
        final Metadata.Builder metadataBuilder = Metadata.builder(Metadata.EMPTY_METADATA);
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
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false).build();
        metadataBuilder.put(projectMetadata);
        final Metadata metadata = metadataBuilder.transientSettings(transientSettings.build())
            .persistentSettings(persistentSettings.build())
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final ClusterState newState = recoverClusterBlocks(initialState);

        assertMetadataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        assertTrue(newState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertTrue(newState.blocks().hasIndexBlock(projectMetadata.id(), "test", IndexMetadata.INDEX_READ_BLOCK));
    }

    public void testRemoveStateNotRecoveredBlock() {
        final Metadata.Builder metadataBuilder = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false).build();
        metadataBuilder.put(projectMetadata);

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
        final Metadata.Builder metadataBuilder = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false).build();
        metadataBuilder.put(projectMetadata);

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
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).put(metadata, false).build();
        final Index index = metadata.getIndex();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(projectMetadata).build())
            .build();
        assertFalse(initialState.routingTable(projectId).hasIndex(index));
        assertFalse(initialState.routingTable(Metadata.DEFAULT_PROJECT_ID).hasIndex(index));

        {
            final ClusterState newState = updateRoutingTable(initialState, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            assertTrue(newState.routingTable(projectId).hasIndex(index));
            assertThat(newState.routingTable(projectId).allShards(index.getName()).size(), is(numOfShards));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(projectMetadata)
                            .put(IndexMetadata.builder(projectMetadata.index("test")).state(IndexMetadata.State.CLOSE))
                    )
                    .build(),
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            );
            assertFalse(newState.routingTable(projectId).hasIndex(index));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(projectMetadata)
                            .put(
                                IndexMetadata.builder(projectMetadata.index("test"))
                                    .state(IndexMetadata.State.CLOSE)
                                    .settings(
                                        Settings.builder()
                                            .put(projectMetadata.index("test").getSettings())
                                            .put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
                                            .build()
                                    )
                            )
                    )
                    .build(),
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            );
            assertTrue(newState.routingTable(projectId).hasIndex(index));
            assertThat(newState.routingTable(projectId).allShards(index.getName()).size(), is(numOfShards));
        }
    }

    public void testMixCurrentAndRecoveredState() {
        final ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();
        final ClusterState recoveredState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(CLUSTER_READ_ONLY_BLOCK).build())
            .metadata(metadata)
            .build();
        assertThat(recoveredState.metadata().clusterUUID(), equalTo(Metadata.UNKNOWN_CLUSTER_UUID));

        final ClusterState updatedState = mixCurrentStateAndRecoveredState(currentState, recoveredState);

        assertThat(updatedState.metadata().clusterUUID(), not(equalTo(Metadata.UNKNOWN_CLUSTER_UUID)));
        assertFalse(Metadata.isGlobalStateEquals(metadata, updatedState.metadata()));
        assertThat(updatedState.metadata().getProject(projectId).index("test"), equalTo(indexMetadata));
        assertTrue(updatedState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertTrue(updatedState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    public void testSetLocalNode() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false))
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE)).build();

        final ClusterState updatedState = setLocalNode(initialState, localNode, CompatibilityVersionsUtils.staticCurrent());

        assertMetadataEquals(initialState, updatedState);
        assertThat(updatedState.nodes().getLocalNode(), equalTo(localNode));
        assertThat(updatedState.nodes().getSize(), is(1));
    }

    public void testDoNotHideStateIfRecovered() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false))
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
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .transientSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build())
            .clusterUUID(clusterUUID)
            .coordinationMetadata(coordinationMetadata)
            .put(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadata)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .build();
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE)).build();
        final ClusterState updatedState = Function.<ClusterState>identity()
            .andThen(state -> setLocalNode(state, localNode, CompatibilityVersionsUtils.staticCurrent()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(initialState);

        final ClusterState hiddenState = hideStateIfNotRecovered(updatedState);

        assertTrue(
            Metadata.isGlobalStateEquals(
                hiddenState.metadata(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(clusterUUID).build()
            )
        );
        assertThat(hiddenState.metadata().getTotalNumberOfIndices(), is(0));
        // Hidden cluster state hides all projects except the default which is built by default
        assertThat(hiddenState.metadata().hasProject(Metadata.DEFAULT_PROJECT_ID), is(true));
        if (Metadata.DEFAULT_PROJECT_ID.equals(projectId) == false) {
            assertThat(hiddenState.metadata().hasProject(projectId), is(false));
        }
        assertTrue(hiddenState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertFalse(hiddenState.blocks().hasIndexBlock(projectId, indexMetadata.getIndex().getName(), IndexMetadata.INDEX_READ_ONLY_BLOCK));
    }

}
