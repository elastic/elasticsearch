/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.shards.ShardCounts;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ShardLimitValidator.LimitGroup;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateServiceTests.addClosedIndex;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateServiceTests.addOpenedIndex;
import static org.elasticsearch.cluster.shards.ShardCounts.forDataNodeCount;
import static org.elasticsearch.cluster.shards.ShardCounts.forIndexNodeCount;
import static org.elasticsearch.cluster.shards.ShardCounts.forSearchNodeCount;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardLimitValidatorTests extends ESTestCase {

    public void testOverShardLimit() {
        testOverShardLimit(LimitGroup.NORMAL, between(1, 90));
        testOverShardLimit(LimitGroup.FROZEN, between(1, 90));
        testOverShardLimit(LimitGroup.INDEX, between(1, 40));
        testOverShardLimit(LimitGroup.SEARCH, between(2, 40));
    }

    private void testOverShardLimit(LimitGroup group, int nodesInCluster) {
        final ShardCounts counts = computeShardCounts(group, nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            randomProjectIdOrDefault(),
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getShardsPerNode(),
            group
        );

        ShardLimitValidator.Result shardLimitsResult = group.checkShardLimit(
            counts.getShardsPerNode(),
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas(),
            state.nodes(),
            state.metadata()
        );

        int totalShards = computeTotalNewShards(group, counts);
        int currentOpenShards = computeCurrentOpenShards(group, counts);
        int maxShards = counts.getShardsPerNode() * nodesInCluster;

        assertFalse(shardLimitsResult.canAddShards());
        assertEquals(
            "this action would add ["
                + totalShards
                + "] shards, but this cluster currently has ["
                + currentOpenShards
                + "]/["
                + maxShards
                + "] maximum "
                + group
                + " shards open; for more information, see "
                + ReferenceDocs.MAX_SHARDS_PER_NODE,
            ShardLimitValidator.errorMessageFrom(shardLimitsResult)
        );
        assertEquals(shardLimitsResult.maxShardsInCluster(), maxShards);
        assertEquals(shardLimitsResult.totalShardsToAdd(), totalShards);
        shardLimitsResult.currentUsedShards()
            .ifPresentOrElse(v -> assertEquals(currentOpenShards, v.intValue()), () -> fail("currentUsedShard should be defined"));
        assertEquals(shardLimitsResult.group(), group);
    }

    public void testUnderShardLimit() {
        testUnderShardLimit(LimitGroup.NORMAL, between(10, 90));
        testUnderShardLimit(LimitGroup.FROZEN, between(10, 90));
        testUnderShardLimit(LimitGroup.INDEX, between(10, 40));
        testUnderShardLimit(LimitGroup.SEARCH, between(10, 40));
    }

    private void testUnderShardLimit(LimitGroup group, int nodesInCluster) {
        // Calculate the counts for a cluster with maximum of 60% of occupancy
        ShardCounts counts = computeShardCounts(group, (int) (nodesInCluster * 0.6));
        ClusterState state = createClusterForShardLimitTest(
            randomProjectIdOrDefault(),
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getShardsPerNode(),
            group
        );

        final var underLimitShardCounts = computeUnderLimitShardCounts(group, counts, nodesInCluster);

        ShardLimitValidator.Result shardLimitsResult = group.checkShardLimit(
            counts.getShardsPerNode(),
            underLimitShardCounts.shards(),
            underLimitShardCounts.replicas(),
            state.nodes(),
            state.metadata()
        );
        logger.info("--> underLimitShardCounts: " + underLimitShardCounts);

        assertTrue(shardLimitsResult.canAddShards());
        assertEquals(shardLimitsResult.maxShardsInCluster(), counts.getShardsPerNode() * nodesInCluster);
        assertEquals(shardLimitsResult.totalShardsToAdd(), underLimitShardCounts.totalNewShards());
        assertFalse(shardLimitsResult.currentUsedShards().isPresent());
        assertEquals(shardLimitsResult.group(), group);
    }

    public void testValidateShardLimitOpenIndices() {
        doTestValidateShardLimitOpenIndices(LimitGroup.NORMAL, between(2, 90));
        doTestValidateShardLimitOpenIndices(LimitGroup.FROZEN, between(2, 90));
        doTestValidateShardLimitOpenIndices(LimitGroup.INDEX, between(2, 40));
        doTestValidateShardLimitOpenIndices(LimitGroup.SEARCH, between(2, 40));
    }

    private void doTestValidateShardLimitOpenIndices(LimitGroup group, int nodesInCluster) {
        ShardCounts counts = computeShardCounts(group, nodesInCluster);
        final ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas(),
            group
        );

        Index[] indices = getIndices(state);

        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), group);
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state.nodes(), state.metadata(), indices)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + computeTotalNewShards(group, counts)
                + "] shards, but this cluster currently has ["
                + computeCurrentOpenShards(group, counts)
                + "]/["
                + maxShards
                + "] maximum "
                + group
                + " shards open; for more information, see "
                + ReferenceDocs.MAX_SHARDS_PER_NODE
                + ";",
            exception.getMessage()
        );
    }

    public void testValidateShardLimitUpdateReplicas() {
        doTestValidateShardLimitUpdateReplicas(LimitGroup.NORMAL);
        doTestValidateShardLimitUpdateReplicas(LimitGroup.FROZEN);
        doTestValidateShardLimitUpdateReplicas(LimitGroup.SEARCH);
    }

    public void doTestValidateShardLimitUpdateReplicas(LimitGroup group) {
        final int nodesInCluster = randomIntBetween(2, 90);
        final int shardsPerNode = randomIntBetween(1, 10);
        int originalReplicas = nodesInCluster - 2;
        ClusterState state = createClusterStateForReplicaUpdate(nodesInCluster, shardsPerNode, originalReplicas, group);

        final Index[] indices = getIndices(state);
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(shardsPerNode, group);

        final int updatedValidReplicas = switch (group) {
            case NORMAL, FROZEN -> nodesInCluster - 1;
            case INDEX, SEARCH -> between(nodesInCluster - 1, nodesInCluster);
        };
        shardLimitValidator.validateShardLimitOnReplicaUpdate(state.nodes(), state.metadata(), indices, updatedValidReplicas);

        final int updatedInvalidReplicas = switch (group) {
            case NORMAL, FROZEN -> nodesInCluster;
            case INDEX, SEARCH -> nodesInCluster + 1;
        };
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimitOnReplicaUpdate(state.nodes(), state.metadata(), indices, updatedInvalidReplicas)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + (shardsPerNode * (updatedInvalidReplicas - originalReplicas))
                + "] shards, but this cluster currently has ["
                + computeTotalShards(group, shardsPerNode, originalReplicas)
                + "]/["
                + shardsPerNode * nodesInCluster
                + "] maximum "
                + group
                + " shards open; for more information, see "
                + ReferenceDocs.MAX_SHARDS_PER_NODE
                + ";",
            exception.getMessage()
        );
    }

    private ShardCounts computeShardCounts(LimitGroup group, int nodesInCluster) {
        final ShardCounts shardCounts = switch (group) {
            case NORMAL, FROZEN -> forDataNodeCount(nodesInCluster);
            case INDEX -> forIndexNodeCount(nodesInCluster);
            case SEARCH -> forSearchNodeCount(nodesInCluster);
        };
        logger.info("--> group [{}], nodesInCluster: [{}] shardCounts: [{}]", group, nodesInCluster, shardCounts);
        return shardCounts;
    }

    private static int computeTotalNewShards(LimitGroup group, ShardCounts counts) {
        return computeTotalShards(group, counts.getFailingIndexShards(), counts.getFailingIndexReplicas());
    }

    private static int computeCurrentOpenShards(LimitGroup group, ShardCounts counts) {
        return computeTotalShards(group, counts.getFirstIndexShards(), counts.getFirstIndexReplicas());
    }

    private static int computeTotalShards(LimitGroup group, int shards, int replicas) {
        return switch (group) {
            case NORMAL, FROZEN -> shards * (1 + replicas);
            case INDEX -> shards;
            case SEARCH -> shards * replicas;
        };
    }

    private record UnderLimitShardCounts(int shards, int replicas, int totalNewShards) {}

    private static UnderLimitShardCounts computeUnderLimitShardCounts(LimitGroup group, ShardCounts counts, int nodesInCluster) {
        int maxShardsInCluster = counts.getShardsPerNode() * nodesInCluster;
        return switch (group) {
            case NORMAL, FROZEN -> {
                int existingShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
                int availableRoom = maxShardsInCluster - existingShards;
                int replicas = randomIntBetween(0, 3);
                int shards = randomIntBetween(1, Math.max(availableRoom / (replicas + 1), 1));
                yield new UnderLimitShardCounts(shards, replicas, shards * (replicas + 1));
            }
            case INDEX -> {
                int existingShards = counts.getFirstIndexShards();
                int availableRoom = maxShardsInCluster - existingShards;
                int replicas = randomIntBetween(0, 1);
                int shards = randomIntBetween(1, availableRoom);
                yield new UnderLimitShardCounts(shards, replicas, shards);
            }
            case SEARCH -> {
                int existingShards = counts.getFirstIndexShards() * counts.getFirstIndexReplicas();
                int availableRoom = maxShardsInCluster - existingShards;
                int replicas = randomIntBetween(1, nodesInCluster);
                int shards = randomIntBetween(1, Math.max(availableRoom / replicas, 1));
                yield new UnderLimitShardCounts(shards, replicas, shards * replicas);
            }
        };
    }

    public Index[] getIndices(ClusterState state) {
        return state.metadata().getProject().indices().values().stream().map(IndexMetadata::getIndex).toList().toArray(Index.EMPTY_ARRAY);
    }

    private ClusterState createClusterStateForReplicaUpdate(int nodesInCluster, int shardsPerNode, int replicas, LimitGroup group) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        state = addOpenedIndex(Metadata.DEFAULT_PROJECT_ID, randomAlphaOfLengthBetween(5, 15), shardsPerNode, replicas, state);
        if (group == LimitGroup.FROZEN) {
            state = ClusterState.builder(state).metadata(freezeMetadata(Metadata.builder(state.metadata()), state.metadata())).build();
        }
        return state;
    }

    private static ClusterState createClusterForShardLimitTest(
        final ProjectId projectId,
        int nodesInCluster,
        int shardsInIndex,
        int replicas,
        int maxShardsPerNode,
        LimitGroup group
    ) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);

        Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (LimitGroup.FROZEN == group || randomBoolean()) {
            settings.put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), group.groupName());
        }
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(settings)
            .creationDate(randomLong())
            .numberOfShards(shardsInIndex)
            .numberOfReplicas(replicas);
        Metadata.Builder metadata = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata));
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
        LimitGroup group
    ) {
        DiscoveryNodes nodes = createDiscoveryNodes(nodesInCluster, group);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        state = addOpenedIndex(Metadata.DEFAULT_PROJECT_ID, randomAlphaOfLengthBetween(5, 15), openIndexShards, openIndexReplicas, state);
        state = addClosedIndex(
            Metadata.DEFAULT_PROJECT_ID,
            randomAlphaOfLengthBetween(5, 15),
            closedIndexShards,
            closedIndexReplicas,
            state
        );

        final Metadata.Builder metadata = Metadata.builder(state.metadata());
        if (randomBoolean()) {
            metadata.persistentSettings(Settings.EMPTY);
        } else {
            metadata.transientSettings(Settings.EMPTY);
        }
        if (LimitGroup.FROZEN == group) {
            freezeMetadata(metadata, state.metadata());
        }
        return ClusterState.builder(state).metadata(metadata).nodes(nodes).build();
    }

    public static DiscoveryNodes createDiscoveryNodes(int nodesInCluster, LimitGroup group) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            Set<DiscoveryNodeRole> roles;
            roles = switch (group) {
                case FROZEN -> randomBoolean() ? DiscoveryNodeRole.roles() : Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
                case NORMAL -> randomBoolean()
                    ? DiscoveryNodeRole.roles()
                    : Set.of(
                        randomFrom(
                            DiscoveryNodeRole.DATA_ROLE,
                            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                            DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                            DiscoveryNodeRole.DATA_COLD_NODE_ROLE
                        )
                    );
                case INDEX -> Set.of(DiscoveryNodeRole.INDEX_ROLE);
                case SEARCH -> Set.of(DiscoveryNodeRole.SEARCH_ROLE);
            };
            builder.add(DiscoveryNodeUtils.builder(randomAlphaOfLengthBetween(5, 15)).roles(roles).build());
        }

        if (group == LimitGroup.INDEX) {
            // Also add search nodes for index limit group and they should not affect the result of the index group
            IntStream.range(0, nodesInCluster + 1)
                .forEach(
                    i -> builder.add(
                        DiscoveryNodeUtils.builder(randomAlphaOfLength(16) + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build()
                    )
                );
        } else if (group == LimitGroup.SEARCH) {
            // Also add index nodes for search limit group and they should not affect the result of the search group
            IntStream.range(0, nodesInCluster + 1)
                .forEach(
                    i -> builder.add(
                        DiscoveryNodeUtils.builder(randomAlphaOfLength(16) + i).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build()
                    )
                );
        }

        return builder.build();
    }

    private static Metadata.Builder freezeMetadata(Metadata.Builder builder, Metadata metadata) {
        metadata.getProject()
            .indices()
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

    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode, LimitGroup group) {
        Setting<?> setting = LimitGroup.FROZEN == group
            ? ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN
            : SETTING_CLUSTER_MAX_SHARDS_PER_NODE;

        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);

        final Settings.Builder settingsBuilder = Settings.builder();
        if (group == LimitGroup.INDEX || group == LimitGroup.SEARCH) {
            settingsBuilder.put("stateless.enabled", true);
        }
        Settings limitOnlySettings = settingsBuilder.put(setting.getKey(), maxShardsPerNode).build();
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
