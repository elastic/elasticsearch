/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.shards.ShardCounts;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Set;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateServiceTests.addClosedIndex;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateServiceTests.addOpenedIndex;
import static org.elasticsearch.cluster.shards.ShardCounts.forDataNodeCount;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardLimitValidatorTests extends ESTestCase {

    public void testOverShardLimit() {
        int nodesInCluster = randomIntBetween(1, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);

        String group = randomFrom(ShardLimitValidator.VALID_GROUPS);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getShardsPerNode(),
            group
        );

        int shardsToAdd = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(
            shardsToAdd,
            state,
            counts.getShardsPerNode(),
            nodesInCluster,
            group
        );

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        assertTrue(errorMessage.isPresent());
        assertEquals(
            "this action would add ["
                + totalShards
                + "] shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum "
                + group
                + " shards open",
            errorMessage.get()
        );
        assertFalse(
            ShardLimitValidator.canAddShardsToCluster(
                counts.getFailingIndexShards(),
                counts.getFailingIndexReplicas(),
                state,
                ShardLimitValidator.FROZEN_GROUP.equals(group)
            )
        );
    }

    public void testUnderShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        // Calculate the counts for a cluster 1 node smaller than we have to ensure we have headroom
        ShardCounts counts = forDataNodeCount(nodesInCluster - 1);

        String group = randomFrom(ShardLimitValidator.VALID_GROUPS);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getShardsPerNode(),
            group
        );

        int existingShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int shardsToAdd = randomIntBetween(1, (counts.getShardsPerNode() * nodesInCluster) - existingShards);
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(
            shardsToAdd,
            state,
            counts.getShardsPerNode(),
            nodesInCluster,
            group
        );

        assertFalse(errorMessage.isPresent());
        assertTrue(ShardLimitValidator.canAddShardsToCluster(shardsToAdd, 0, state, ShardLimitValidator.FROZEN_GROUP.equals(group)));
    }

    public void testValidateShardLimitOpenIndices() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        final String group = randomFrom(ShardLimitValidator.VALID_GROUPS);
        final ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas(),
            group
        );

        Index[] indices = getIndices(state);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), group);
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state, indices)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + totalShards
                + "] shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum "
                + group
                + " shards open;",
            exception.getMessage()
        );
    }

    public void testValidateShardLimitUpdateReplicas() {
        final int nodesInCluster = randomIntBetween(2, 90);
        final int shardsPerNode = randomIntBetween(1, 10);
        final String group = randomFrom(ShardLimitValidator.VALID_GROUPS);
        ClusterState state = createClusterStateForReplicaUpdate(nodesInCluster, shardsPerNode, group);

        final Index[] indices = getIndices(state);
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(shardsPerNode, group);
        shardLimitValidator.validateShardLimitOnReplicaUpdate(state, indices, nodesInCluster - 1);

        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimitOnReplicaUpdate(state, indices, nodesInCluster)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + (shardsPerNode * 2)
                + "] shards, but this cluster currently has ["
                + (shardsPerNode * (nodesInCluster - 1))
                + "]/["
                + shardsPerNode * nodesInCluster
                + "] maximum "
                + group
                + " shards open;",
            exception.getMessage()
        );
    }

    public Index[] getIndices(ClusterState state) {
        return state.metadata()
            .indices()
            .values()
            .stream()
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(Index.EMPTY_ARRAY);
    }

    private ClusterState createClusterStateForReplicaUpdate(int nodesInCluster, int shardsPerNode, String group) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        state = addOpenedIndex(randomAlphaOfLengthBetween(5, 15), shardsPerNode, nodesInCluster - 2, state);
        if (group.equals(ShardLimitValidator.FROZEN_GROUP)) {
            state = ClusterState.builder(state).metadata(freezeMetadata(Metadata.builder(state.metadata()), state.metadata())).build();
        }
        return state;
    }

    public static ClusterState createClusterForShardLimitTest(
        int nodesInCluster,
        int shardsInIndex,
        int replicas,
        int maxShardsPerNode,
        String group
    ) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);

        Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        if (ShardLimitValidator.FROZEN_GROUP.equals(group) || randomBoolean()) {
            settings.put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), group);
        }
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(settings)
            .creationDate(randomLong())
            .numberOfShards(shardsInIndex)
            .numberOfReplicas(replicas);
        Metadata.Builder metadata = Metadata.builder().put(indexMetadata);
        Settings.Builder clusterSettings = Settings.builder()
            .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), maxShardsPerNode)
            .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode);
        if (randomBoolean()) {
            metadata.transientSettings(Settings.EMPTY);
            metadata.persistentSettings(clusterSettings.build());
        } else {
            metadata.persistentSettings(Settings.EMPTY);
            metadata.transientSettings(clusterSettings.build());
        }

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(nodes).build();
    }

    public static ClusterState createClusterForShardLimitTest(
        int nodesInCluster,
        int openIndexShards,
        int openIndexReplicas,
        int closedIndexShards,
        int closedIndexReplicas,
        String group
    ) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        state = addOpenedIndex(randomAlphaOfLengthBetween(5, 15), openIndexShards, openIndexReplicas, state);
        state = addClosedIndex(randomAlphaOfLengthBetween(5, 15), closedIndexShards, closedIndexReplicas, state);

        final Metadata.Builder metadata = Metadata.builder(state.metadata());
        if (randomBoolean()) {
            metadata.persistentSettings(Settings.EMPTY);
        } else {
            metadata.transientSettings(Settings.EMPTY);
        }
        if (ShardLimitValidator.FROZEN_GROUP.equals(group)) {
            freezeMetadata(metadata, state.metadata());
        }
        return ClusterState.builder(state).metadata(metadata).nodes(nodes).build();
    }

    public static DiscoveryNodes createDiscoveryNodes(int nodesInCluster, String group) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), createNode(group));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());
        return nodes;
    }

    private static DiscoveryNode createNode(String group) {
        DiscoveryNode mock = mock(DiscoveryNode.class);
        if (ShardLimitValidator.FROZEN_GROUP.equals(group)) {
            when(mock.getRoles()).thenReturn(
                randomBoolean() ? DiscoveryNodeRole.BUILT_IN_ROLES : Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
            );
        } else {
            when(mock.getRoles()).thenReturn(
                randomBoolean()
                    ? DiscoveryNodeRole.BUILT_IN_ROLES
                    : Set.of(
                        randomFrom(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                            DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                            DiscoveryNodeRole.DATA_COLD_NODE_ROLE
                        )
                    )
            );
        }
        return mock;
    }

    private static Metadata.Builder freezeMetadata(Metadata.Builder builder, Metadata metadata) {
        metadata.indices()
            .values()
            .stream()
            .map(
                imd -> IndexMetadata.builder(imd)
                    .settings(
                        Settings.builder()
                            .put(imd.getSettings())
                            .put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), ShardLimitValidator.FROZEN_GROUP)
                    )
            )
            .forEach(builder::put);
        return builder;
    }

    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode, String group) {
        Setting<?> setting = ShardLimitValidator.FROZEN_GROUP.equals(group)
            ? ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN
            : SETTING_CLUSTER_MAX_SHARDS_PER_NODE;

        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);
        Settings limitOnlySettings = Settings.builder().put(setting.getKey(), maxShardsPerNode).build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        return new ShardLimitValidator(limitOnlySettings, clusterService);
    }

    /**
     * Creates a {@link ShardLimitValidator} for testing with the given setting and a mocked cluster service.
     *
     * @param maxShardsPerNode the value to use for the max shards per node setting
     * @return a test instance
     */
    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode) {
        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);
        Settings limitOnlySettings = Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        return new ShardLimitValidator(limitOnlySettings, clusterService);
    }

    /**
     * Creates a {@link ShardLimitValidator} for testing with the given setting and a given cluster service.
     *
     * @param maxShardsPerNode the value to use for the max shards per node setting
     * @param clusterService   the cluster service to use
     * @return a test instance
     */
    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode, ClusterService clusterService) {
        Settings limitOnlySettings = Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build();

        return new ShardLimitValidator(limitOnlySettings, clusterService);
    }
}
