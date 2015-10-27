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
package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTests extends ESTestCase {

    public void testSupersedes() {
        final DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("node2", DummyTransportAddress.INSTANCE, Version.CURRENT);
        final DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).put(node2).build();
        ClusterState noMaster1 = ClusterState.builder(ClusterName.DEFAULT).version(randomInt(5)).nodes(nodes).build();
        ClusterState noMaster2 = ClusterState.builder(ClusterName.DEFAULT).version(randomInt(5)).nodes(nodes).build();
        ClusterState withMaster1a = ClusterState.builder(ClusterName.DEFAULT).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes).masterNodeId(node1.id())).build();
        ClusterState withMaster1b = ClusterState.builder(ClusterName.DEFAULT).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes).masterNodeId(node1.id())).build();
        ClusterState withMaster2 = ClusterState.builder(ClusterName.DEFAULT).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes).masterNodeId(node2.id())).build();

        // states with no master should never supersede anything
        assertFalse(noMaster1.supersedes(noMaster2));
        assertFalse(noMaster1.supersedes(withMaster1a));

        // states should never supersede states from another master
        assertFalse(withMaster1a.supersedes(withMaster2));
        assertFalse(withMaster1a.supersedes(noMaster1));

        // state from the same master compare by version
        assertThat(withMaster1a.supersedes(withMaster1b), equalTo(withMaster1a.version() > withMaster1b.version()));

    }

    public void testToXContent() throws IOException {
        ClusterState clusterState = newClusterState();
        assertNotNull(clusterState);

        ShardRouting testShard0 = clusterState.getRoutingTable().getIndicesRouting().get("test").shard(0).primaryShard();
        assertNotNull(testShard0);

        ShardRouting testShard1 = clusterState.getRoutingTable().getIndicesRouting().get("test").shard(1).primaryShard();
        assertNotNull(testShard1);

        for (ToXContent.Params params : Arrays.asList(ToXContent.EMPTY_PARAMS, toParams("keyed", Boolean.TRUE.toString()))) {
            assertEquals("{\n" +
                    "  \"version\" : 0,\n" +
                    "  \"state_uuid\" : \"_dummy_state_uuid_\",\n" +
                    "  \"master_node\" : \"_dummy_node_id_\",\n" +
                    "  \"blocks\" : {\n" +
                    // by default, global cluster blocks are placed as objects named with the block's id
                    "    \"global\" : {\n" +
                    "      \"2\" : {\n" +
                    "        \"description\" : \"no master\",\n" +
                    "        \"retryable\" : true,\n" +
                    "        \"disable_state_persistence\" : true,\n" +
                    "        \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                    "      }\n" +
                    "    },\n" +
                    // by default, index blocks are placed as objects named with the index's name
                    "    \"indices\" : {\n" +
                    "      \"test\" : {\n" +
                    // and then each block is placed as an object named with the block's id
                    "        \"8\" : {\n" +
                    "          \"description\" : \"index write (api)\",\n" +
                    "          \"retryable\" : false,\n" +
                    "          \"levels\" : [ \"write\" ]\n" +
                    "        },\n" +
                    "        \"9\" : {\n" +
                    "          \"description\" : \"index metadata (api)\",\n" +
                    "          \"retryable\" : false,\n" +
                    "          \"levels\" : [ \"metadata_read\", \"metadata_write\" ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    // by default, nodes are placed as objects named with the node's id
                    "  \"nodes\" : {\n" +
                    "    \"_dummy_node_id_\" : {\n" +
                    "      \"name\" : \"_dummy_node_\",\n" +
                    "      \"transport_address\" : \"_dummy_addr_\",\n" +
                    "      \"attributes\" : { }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"metadata\" : {\n" +
                    "    \"cluster_uuid\" : \"_dummy_cluster_uuid_\",\n" +
                    // by default, templates are placed as objects named with the template's name
                    "    \"templates\" : {\n" +
                    "      \"foo\" : {\n" +
                    "        \"template\" : \"bar\",\n" +
                    "        \"order\" : 1,\n" +
                    "        \"settings\" : {\n" +
                    "          \"setting2\" : \"value2\",\n" +
                    "          \"setting1\" : \"value1\"\n" +
                    "        },\n" +
                    "        \"mappings\" : { }\n" +
                    "      }\n" +
                    "    },\n" +
                    // by default, indices are placed as objects named with the index's name
                    "    \"indices\" : {\n" +
                    "      \"test\" : {\n" +
                    "        \"state\" : \"open\",\n" +
                    "        \"settings\" : {\n" +
                    "          \"index\" : {\n" +
                    "            \"creation_date\" : \"1\",\n" +
                    "            \"number_of_shards\" : \"2\",\n" +
                    "            \"number_of_replicas\" : \"0\",\n" +
                    "            \"version\" : {\n" +
                    "              \"created\" : \"1070099\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        \"mappings\" : { },\n" +
                    "        \"aliases\" : [ ]\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"routing_table\" : {\n" +
                    // same as above
                    "    \"indices\" : {\n" +
                    "      \"test\" : {\n" +
                    "        \"shards\" : {\n" +
                    "          \"1\" : [ {\n" +
                    "            \"state\" : \"STARTED\",\n" +
                    "            \"primary\" : true,\n" +
                    "            \"node\" : \"_dummy_node_id_\",\n" +
                    "            \"relocating_node\" : null,\n" +
                    "            \"shard\" : 1,\n" +
                    "            \"index\" : \"test\",\n" +
                    "            \"version\" : 1,\n" +
                    "            \"allocation_id\" : {\n" +
                    "              \"id\" : \"" + testShard1.allocationId().getId() + "\"\n" +
                    "            }\n" +
                    "          } ],\n" +
                    "          \"0\" : [ {\n" +
                    "            \"state\" : \"STARTED\",\n" +
                    "            \"primary\" : true,\n" +
                    "            \"node\" : \"_dummy_node_id_\",\n" +
                    "            \"relocating_node\" : null,\n" +
                    "            \"shard\" : 0,\n" +
                    "            \"index\" : \"test\",\n" +
                    "            \"version\" : 1,\n" +
                    "            \"allocation_id\" : {\n" +
                    "              \"id\" : \"" + testShard0.allocationId().getId() + "\"\n" +
                    "            }\n" +
                    "          } ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"routing_nodes\" : {\n" +
                    "    \"unassigned\" : [ ],\n" +
                    // by default, nodes are placed as objects named with the node's id
                    "    \"nodes\" : {\n" +
                    "      \"_dummy_node_id_\" : [ {\n" +
                    "        \"state\" : \"STARTED\",\n" +
                    "        \"primary\" : true,\n" +
                    "        \"node\" : \"_dummy_node_id_\",\n" +
                    "        \"relocating_node\" : null,\n" +
                    "        \"shard\" : 1,\n" +
                    "        \"index\" : \"test\",\n" +
                    "        \"version\" : 1,\n" +
                    "        \"allocation_id\" : {\n" +
                    "          \"id\" : \"" + testShard1.allocationId().getId() + "\"\n" +
                    "        }\n" +
                    "      }, {\n" +
                    "        \"state\" : \"STARTED\",\n" +
                    "        \"primary\" : true,\n" +
                    "        \"node\" : \"_dummy_node_id_\",\n" +
                    "        \"relocating_node\" : null,\n" +
                    "        \"shard\" : 0,\n" +
                    "        \"index\" : \"test\",\n" +
                    "        \"version\" : 1,\n" +
                    "        \"allocation_id\" : {\n" +
                    "          \"id\" : \"" + testShard0.allocationId().getId() + "\"\n" +
                    "        }\n" +
                    "      } ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", buildClusterState(clusterState, params));
        }
    }

    public void testToXContentWithKeyedSetToFalse() throws IOException {
        ClusterState clusterState = newClusterState();
        assertNotNull(clusterState);

        ShardRouting testShard0 = clusterState.getRoutingTable().getIndicesRouting().get("test").shard(0).primaryShard();
        assertNotNull(testShard0);

        ShardRouting testShard1 = clusterState.getRoutingTable().getIndicesRouting().get("test").shard(1).primaryShard();
        assertNotNull(testShard1);

        assertEquals("{\n" +
                "  \"version\" : 0,\n" +
                "  \"state_uuid\" : \"_dummy_state_uuid_\",\n" +
                "  \"master_node\" : \"_dummy_node_id_\",\n" +
                "  \"blocks\" : {\n" +
                // with keyed=false, the global cluster blocks are placed in an array of blocks,
                // each block id is moved to a sub field "id"
                "    \"global\" : [ {\n" +
                "      \"id\" : \"2\",\n" +
                "      \"description\" : \"no master\",\n" +
                "      \"retryable\" : true,\n" +
                "      \"disable_state_persistence\" : true,\n" +
                "      \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                "    } ],\n" +
                "    \"indices\" : [ {\n" +
                "      \"index\" : \"test\",\n" +
                "      \"blocks\" : [ {\n" +
                "        \"id\" : \"8\",\n" +
                "        \"description\" : \"index write (api)\",\n" +
                "        \"retryable\" : false,\n" +
                "        \"levels\" : [ \"write\" ]\n" +
                "      }, {\n" +
                "        \"id\" : \"9\",\n" +
                "        \"description\" : \"index metadata (api)\",\n" +
                "        \"retryable\" : false,\n" +
                "        \"levels\" : [ \"metadata_read\", \"metadata_write\" ]\n" +
                "      } ]\n" +
                "    } ]\n" +
                "  },\n" +
                // with keyed=false, the nodes are placed in an array,
                // each node id is moved to a sub field "id"
                "  \"nodes\" : [ {\n" +
                "    \"id\" : \"_dummy_node_id_\",\n" +
                "    \"name\" : \"_dummy_node_\",\n" +
                "    \"transport_address\" : \"_dummy_addr_\",\n" +
                "    \"attributes\" : { }\n" +
                "  } ],\n" +
                "  \"metadata\" : {\n" +
                "    \"cluster_uuid\" : \"_dummy_cluster_uuid_\",\n" +
                // with keyed=false, the templates are placed in an array,
                // each template name is moved to a sub field "name"
                "    \"templates\" : [ {\n" +
                "      \"name\" : \"foo\",\n" +
                "      \"template\" : \"bar\",\n" +
                "      \"order\" : 1,\n" +
                "      \"settings\" : {\n" +
                "        \"setting2\" : \"value2\",\n" +
                "        \"setting1\" : \"value1\"\n" +
                "      },\n" +
                "      \"mappings\" : { }\n" +
                "    } ],\n" +
                // with keyed=false, the indices are placed in an array,
                // each index name is moved to a sub field "index"
                "    \"indices\" : [ {\n" +
                "      \"index\" : \"test\",\n" +
                "      \"state\" : \"open\",\n" +
                "      \"settings\" : {\n" +
                "        \"index\" : {\n" +
                "          \"creation_date\" : \"1\",\n" +
                "          \"number_of_shards\" : \"2\",\n" +
                "          \"number_of_replicas\" : \"0\",\n" +
                "          \"version\" : {\n" +
                "            \"created\" : \"1070099\"\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      \"mappings\" : { },\n" +
                "      \"aliases\" : [ ]\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"routing_table\" : {\n" +
                // with keyed=false, the indices are placed in an array,
                // each index name is moved to a sub field "index"
                "    \"indices\" : [ {\n" +
                "      \"index\" : \"test\",\n" +
                // with keyed=false, the shards are placed in an array,
                // each shard id is moved to a sub field "id"
                "      \"shards\" : [ {\n" +
                "        \"id\" : \"1\",\n" +
                // same as above
                "        \"shards\" : [ {\n" +
                "          \"state\" : \"STARTED\",\n" +
                "          \"primary\" : true,\n" +
                "          \"node\" : \"_dummy_node_id_\",\n" +
                "          \"relocating_node\" : null,\n" +
                "          \"shard\" : 1,\n" +
                "          \"index\" : \"test\",\n" +
                "          \"version\" : 1,\n" +
                "          \"allocation_id\" : {\n" +
                "            \"id\" : \"" + testShard1.allocationId().getId() + "\"\n" +
                "          }\n" +
                "        } ]\n" +
                "      }, {\n" +
                "        \"id\" : \"0\",\n" +
                "        \"shards\" : [ {\n" +
                "          \"state\" : \"STARTED\",\n" +
                "          \"primary\" : true,\n" +
                "          \"node\" : \"_dummy_node_id_\",\n" +
                "          \"relocating_node\" : null,\n" +
                "          \"shard\" : 0,\n" +
                "          \"index\" : \"test\",\n" +
                "          \"version\" : 1,\n" +
                "          \"allocation_id\" : {\n" +
                "            \"id\" : \"" + testShard0.allocationId().getId() + "\"\n" +
                "          }\n" +
                "        } ]\n" +
                "      } ]\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"routing_nodes\" : {\n" +
                "    \"unassigned\" : [ ],\n" +
                // with keyed=false, the nodes are placed in an array,
                // each node id is moved to a sub field "id"
                "    \"nodes\" : [ {\n" +
                "      \"id\" : \"_dummy_node_id_\",\n" +
                "      \"shards\" : [ {\n" +
                "        \"state\" : \"STARTED\",\n" +
                "        \"primary\" : true,\n" +
                "        \"node\" : \"_dummy_node_id_\",\n" +
                "        \"relocating_node\" : null,\n" +
                "        \"shard\" : 1,\n" +
                "        \"index\" : \"test\",\n" +
                "        \"version\" : 1,\n" +
                "        \"allocation_id\" : {\n" +
                "          \"id\" : \"" + testShard1.allocationId().getId() + "\"\n" +
                "        }\n" +
                "      }, {\n" +
                "        \"state\" : \"STARTED\",\n" +
                "        \"primary\" : true,\n" +
                "        \"node\" : \"_dummy_node_id_\",\n" +
                "        \"relocating_node\" : null,\n" +
                "        \"shard\" : 0,\n" +
                "        \"index\" : \"test\",\n" +
                "        \"version\" : 1,\n" +
                "        \"allocation_id\" : {\n" +
                "          \"id\" : \"" + testShard0.allocationId().getId() + "\"\n" +
                "        }\n" +
                "      } ]\n" +
                "    } ]\n" +
                "  }\n" +
                "}", buildClusterState(clusterState, toParams("keyed", Boolean.FALSE.toString())));
    }

    /**
     * Creates a fake ClusterState instance
     *
     * @return a fake ClusterState instance
     */
    private ClusterState newClusterState() {
        DiscoveryNode node = new DiscoveryNode("_dummy_node_", "_dummy_node_id_", DummyTransportAddress.INSTANCE, emptyMap(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node).build();

        MetaData metaData = MetaData.builder()
                .clusterUUID("_dummy_cluster_uuid_")
                .put(IndexMetaData.builder("test")
                        .settings(settings(Version.V_1_7_0))
                        .creationDate(1l)
                        .numberOfShards(2)
                        .numberOfReplicas(0))
                .put(IndexTemplateMetaData.builder("foo")
                                .template("bar")
                                .order(1)
                                .settings(settingsBuilder()
                                                .put("setting1", "value1")
                                                .put("setting2", "value2")
                                )
                ).build();

        RoutingTable routingTable = RoutingTable.builder()
                .add(IndexRoutingTable.builder("test")
                                .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 0))
                                        .addShard(TestShardRouting.newShardRouting("test", 0, "_dummy_node_id_", null, null, true, ShardRoutingState.STARTED, 1))
                                        .build())
                                .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 1))
                                                .addShard(TestShardRouting.newShardRouting("test", 1, "_dummy_node_id_", null, null, true, ShardRoutingState.STARTED, 1))
                                                .build()
                                )
                )
                .build();

        ClusterBlocks blocks = ClusterBlocks.builder()
                .addGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ALL)
                .addIndexBlock("test", IndexMetaData.INDEX_METADATA_BLOCK)
                .addIndexBlock("test", IndexMetaData.INDEX_WRITE_BLOCK)
                .build();

        return ClusterState.builder(ClusterName.DEFAULT)
                .stateUUID("_dummy_state_uuid_")
                .version(0)
                .nodes(DiscoveryNodes.builder(nodes)
                        .masterNodeId(node.id()))
                .blocks(blocks)
                .metaData(metaData)
                .routingTable(routingTable)
                .build();
    }

    private String buildClusterState(ClusterState clusterState, ToXContent.Params params) throws IOException {
        assertNotNull(clusterState);
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint()) {
            builder.startObject();
            clusterState.toXContent(builder, params);
            builder.endObject();
            return builder.string().trim();
        }
    }

    private ToXContent.Params toParams(String key, String value) {
        Map<String, String> params = new HashMap<>();
        params.put(key, value);
        return new ToXContent.MapParams(params);
    }
}
