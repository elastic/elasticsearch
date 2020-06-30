/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.shards.ShardCounts;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
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

        Settings clusterSettings = Settings.builder().build();

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas()
        );

        int shardsToAdd = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode());

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        assertTrue(errorMessage.isPresent());
        assertEquals("this action would add [" + totalShards + "] total shards, but this cluster currently has [" + currentShards
            + "]/[" + maxShards + "] maximum shards open", errorMessage.get());
    }

    public void testUnderShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        // Calculate the counts for a cluster 1 node smaller than we have to ensure we have headroom
        ShardCounts counts = forDataNodeCount(nodesInCluster - 1);

        Settings clusterSettings = Settings.builder().build();

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas()
        );

        int existingShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int shardsToAdd = randomIntBetween(1, (counts.getShardsPerNode() * nodesInCluster) - existingShards);
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode());

        assertFalse(errorMessage.isPresent());
    }

    public void testValidateShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(), counts.getFailingIndexReplicas());

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(IndexMetadata.class))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode());
        ValidationException exception = expectThrows(ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state, indices));
        assertEquals("Validation Failed: 1: this action would add [" + totalShards + "] total shards, but this cluster currently has [" +
            currentShards + "]/[" + maxShards + "] maximum shards open;", exception.getMessage());
    }

    public static ClusterState createClusterForShardLimitTest(int nodesInCluster, int shardsInIndex, int replicas) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());

        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(randomLong())
            .numberOfShards(shardsInIndex)
            .numberOfReplicas(replicas);
        Metadata.Builder metadata = Metadata.builder().put(indexMetadata);
        if (randomBoolean()) {
            metadata.transientSettings(Settings.EMPTY);
        } else {
            metadata.persistentSettings(Settings.EMPTY);
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(nodes)
            .build();
    }

    public static ClusterState createClusterForShardLimitTest(int nodesInCluster, int openIndexShards, int openIndexReplicas,
                                                              int closedIndexShards, int closedIndexReplicas) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        state = addOpenedIndex(randomAlphaOfLengthBetween(5, 15), openIndexShards, openIndexReplicas, state);
        state = addClosedIndex(randomAlphaOfLengthBetween(5, 15), closedIndexShards, closedIndexReplicas, state);

        final Metadata.Builder metadata = Metadata.builder(state.metadata());
        if (randomBoolean()) {
            metadata.persistentSettings(Settings.EMPTY);
        } else {
            metadata.transientSettings(Settings.EMPTY);
        }
        return ClusterState.builder(state).metadata(metadata).nodes(nodes).build();
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
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

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
