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

package org.elasticsearch.bwcompat;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStateBackwardsCompatTests extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testClusterState() throws Exception {
        createIndex("test");

        // connect to each node with a custom TransportClient, issue a ClusterStateRequest to test serialization
        for (NodeInfo n : clusterNodes()) {
            try (TransportClient tc = newTransportClient()) {
                tc.addTransportAddress(n.getNode().address());
                ClusterStateResponse response = tc.admin().cluster().prepareState().execute().actionGet();

                assertThat(response.getState().status(), equalTo(ClusterState.ClusterStateStatus.UNKNOWN));
                assertNotNull(response.getClusterName());
                assertTrue(response.getState().getMetaData().hasIndex("test"));
            }
        }
    }

    @Test
    public void testClusterStateWithBlocks() {
        createIndex("test-blocks");

        Map<String, ClusterBlock> blocks = new HashMap<>();
        blocks.put(SETTING_BLOCKS_READ, IndexMetaData.INDEX_READ_BLOCK);
        blocks.put(SETTING_BLOCKS_WRITE, IndexMetaData.INDEX_WRITE_BLOCK);
        blocks.put(SETTING_BLOCKS_METADATA, IndexMetaData.INDEX_METADATA_BLOCK);

        for (Map.Entry<String, ClusterBlock> block : blocks.entrySet()) {
            try {
                enableIndexBlock("test-blocks", block.getKey());

                for (NodeInfo n : clusterNodes()) {
                    try (TransportClient tc = newTransportClient()) {
                        tc.addTransportAddress(n.getNode().address());

                        ClusterStateResponse response = tc.admin().cluster().prepareState().setIndices("test-blocks")
                                .setBlocks(true).setNodes(false).execute().actionGet();

                        ClusterBlocks clusterBlocks = response.getState().blocks();
                        assertNotNull(clusterBlocks);
                        assertTrue(clusterBlocks.hasIndexBlock("test-blocks", block.getValue()));

                        for (ClusterBlockLevel level : block.getValue().levels()) {
                            assertTrue(clusterBlocks.indexBlocked(level, "test-blocks"));
                        }

                        IndexMetaData indexMetaData = response.getState().getMetaData().getIndices().get("test-blocks");
                        assertNotNull(indexMetaData);
                        assertTrue(indexMetaData.settings().getAsBoolean(block.getKey(), null));
                    }
                }
            } finally {
                disableIndexBlock("test-blocks", block.getKey());
            }
        }
    }

    private NodesInfoResponse clusterNodes() {
        return client().admin().cluster().prepareNodesInfo().execute().actionGet();
    }

    private TransportClient newTransportClient() {
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true)
                .put("node.name", "transport_client_" + getTestName()).build();
        return new TransportClient(settings);
    }
}
