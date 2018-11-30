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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.shards.ClusterShardLimitIT;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.shards.ClusterShardLimitIT.ShardCounts.forDataNodeCount;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataIndexStateServiceTests extends ESTestCase {

    public void testValidateShardLimit() {
        int nodesInCluster = randomIntBetween(2,100);
        ClusterShardLimitIT.ShardCounts counts = forDataNodeCount(nodesInCluster);
        Settings clusterSettings = Settings.builder()
            .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), counts.getShardsPerNode())
            .build();
        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(), counts.getFailingIndexReplicas(), clusterSettings);

        Index[] indices = Arrays.stream(state.metaData().indices().values().toArray(IndexMetaData.class))
            .map(IndexMetaData::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ValidationException exception = expectThrows(ValidationException.class,
            () -> MetaDataIndexStateService.validateShardLimit(state, indices));
        assertEquals("Validation Failed: 1: this action would add [" + totalShards + "] total shards, but this cluster currently has [" +
            currentShards + "]/[" + maxShards + "] maximum shards open;", exception.getMessage());
    }

    public static ClusterState createClusterForShardLimitTest(int nodesInCluster, int openIndexShards, int openIndexReplicas,
                                                              int closedIndexShards, int closedIndexReplicas, Settings clusterSettings) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5,15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());

        IndexMetaData.Builder openIndexMetaData = IndexMetaData.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(randomLong())
            .numberOfShards(openIndexShards)
            .numberOfReplicas(openIndexReplicas);
        IndexMetaData.Builder closedIndexMetaData = IndexMetaData.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(randomLong())
            .state(IndexMetaData.State.CLOSE)
            .numberOfShards(closedIndexShards)
            .numberOfReplicas(closedIndexReplicas);
        MetaData.Builder metaData = MetaData.builder().put(openIndexMetaData).put(closedIndexMetaData);
        if (randomBoolean()) {
            metaData.persistentSettings(clusterSettings);
        } else {
            metaData.transientSettings(clusterSettings);
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(nodes)
            .build();
    }
}
