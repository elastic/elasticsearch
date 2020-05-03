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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClusterRerouteResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        DiscoveryNode node0 = new DiscoveryNode("node0", new TransportAddress(TransportAddress.META_ADDRESS, 9000), Version.CURRENT);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().add(node0).masterNodeId(node0.getId()).build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(Settings.builder()
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), true)
                .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 10)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> openMapBuilder = ImmutableOpenMap.builder();
        openMapBuilder.put("index", indexMetadata);
        Metadata metadata = Metadata.builder().indices(openMapBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodes).metadata(metadata).build();

        RoutingExplanations routingExplanations = new RoutingExplanations();
        routingExplanations.add(new RerouteExplanation(new AllocateReplicaAllocationCommand("index", 0, "node0"), Decision.YES));
        ClusterRerouteResponse clusterRerouteResponse = new ClusterRerouteResponse(true, clusterState, routingExplanations);
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            clusterRerouteResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\n" +
                    "  \"acknowledged\" : true,\n" +
                    "  \"state\" : {\n" +
                    "    \"cluster_uuid\" : \"_na_\",\n" +
                    "    \"version\" : 0,\n" +
                    "    \"state_uuid\" : \"" + clusterState.stateUUID() + "\",\n" +
                    "    \"master_node\" : \"node0\",\n" +
                    "    \"blocks\" : { },\n" +
                    "    \"nodes\" : {\n" +
                    "      \"node0\" : {\n" +
                    "        \"name\" : \"\",\n" +
                    "        \"ephemeral_id\" : \"" + node0.getEphemeralId() + "\",\n" +
                    "        \"transport_address\" : \"0.0.0.0:9000\",\n" +
                    "        \"attributes\" : { }\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"metadata\" : {\n" +
                    "      \"cluster_uuid\" : \"_na_\",\n" +
                    "      \"cluster_uuid_committed\" : false,\n" +
                    "      \"cluster_coordination\" : {\n" +
                    "        \"term\" : 0,\n" +
                    "        \"last_committed_config\" : [ ],\n" +
                    "        \"last_accepted_config\" : [ ],\n" +
                    "        \"voting_config_exclusions\" : [ ]\n" +
                    "      },\n" +
                    "      \"templates\" : { },\n" +
                    "      \"indices\" : {\n" +
                    "        \"index\" : {\n" +
                    "          \"version\" : 1,\n" +
                    "          \"mapping_version\" : 1,\n" +
                    "          \"settings_version\" : 1,\n" +
                    "          \"aliases_version\" : 1,\n" +
                    "          \"routing_num_shards\" : 1,\n" +
                    "          \"state\" : \"open\",\n" +
                    "          \"settings\" : {\n" +
                    "            \"index\" : {\n" +
                    "              \"shard\" : {\n" +
                    "                \"check_on_startup\" : \"true\"\n" +
                    "              },\n" +
                    "              \"number_of_shards\" : \"1\",\n" +
                    "              \"number_of_replicas\" : \"0\",\n" +
                    "              \"version\" : {\n" +
                    "                \"created\" : \"" + Version.CURRENT.id + "\"\n" +
                    "              },\n" +
                    "              \"max_script_fields\" : \"10\"\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"mappings\" : { },\n" +
                    "          \"aliases\" : [ ],\n" +
                    "          \"primary_terms\" : {\n" +
                    "            \"0\" : 0\n" +
                    "          },\n" +
                    "          \"in_sync_allocations\" : {\n" +
                    "            \"0\" : [ ]\n" +
                    "          },\n" +
                    "          \"rollover_info\" : { }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"index-graveyard\" : {\n" +
                    "        \"tombstones\" : [ ]\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"routing_table\" : {\n" +
                    "      \"indices\" : { }\n" +
                    "    },\n" +
                    "    \"routing_nodes\" : {\n" +
                    "      \"unassigned\" : [ ],\n" +
                    "      \"nodes\" : {\n" +
                    "        \"node0\" : [ ]\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", Strings.toString(builder));

        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            Map<String, String> params = new HashMap<>();
            params.put("explain", "true");
            params.put("metric", "version,master_node");
            clusterRerouteResponse.toXContent(builder, new ToXContent.MapParams(params));
            assertEquals("{\n" +
                    "  \"acknowledged\" : true,\n" +
                    "  \"state\" : {\n" +
                    "    \"cluster_uuid\" : \"_na_\",\n" +
                    "    \"version\" : 0,\n" +
                    "    \"state_uuid\" : \"" + clusterState.stateUUID() + "\",\n" +
                    "    \"master_node\" : \"node0\"\n" +
                    "  },\n" +
                    "  \"explanations\" : [\n" +
                    "    {\n" +
                    "      \"command\" : \"allocate_replica\",\n" +
                    "      \"parameters\" : {\n" +
                    "        \"index\" : \"index\",\n" +
                    "        \"shard\" : 0,\n" +
                    "        \"node\" : \"node0\"\n" +
                    "      },\n" +
                    "      \"decisions\" : [\n" +
                    "        {\n" +
                    "          \"decider\" : null,\n" +
                    "          \"decision\" : \"YES\",\n" +
                    "          \"explanation\" : \"none\"\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}", Strings.toString(builder));
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            Map<String, String> params = new HashMap<>();
            params.put("metric", "metadata");
            params.put("settings_filter", "index.number*,index.version.created");
            clusterRerouteResponse.toXContent(builder, new ToXContent.MapParams(params));
            assertEquals("{\n" +
                    "  \"acknowledged\" : true,\n" +
                    "  \"state\" : {\n" +
                    "    \"cluster_uuid\" : \"_na_\",\n" +
                    "    \"metadata\" : {\n" +
                    "      \"cluster_uuid\" : \"_na_\",\n" +
                    "      \"cluster_uuid_committed\" : false,\n" +
                    "      \"cluster_coordination\" : {\n" +
                    "        \"term\" : 0,\n" +
                    "        \"last_committed_config\" : [ ],\n" +
                    "        \"last_accepted_config\" : [ ],\n" +
                    "        \"voting_config_exclusions\" : [ ]\n" +
                    "      },\n" +
                    "      \"templates\" : { },\n" +
                    "      \"indices\" : {\n" +
                    "        \"index\" : {\n" +
                    "          \"version\" : 1,\n" +
                    "          \"mapping_version\" : 1,\n" +
                    "          \"settings_version\" : 1,\n" +
                    "          \"aliases_version\" : 1,\n" +
                    "          \"routing_num_shards\" : 1,\n" +
                    "          \"state\" : \"open\",\n" +
                    "          \"settings\" : {\n" +
                    "            \"index\" : {\n" +
                    "              \"max_script_fields\" : \"10\",\n" +
                    "              \"shard\" : {\n" +
                    "                \"check_on_startup\" : \"true\"\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"mappings\" : { },\n" +
                    "          \"aliases\" : [ ],\n" +
                    "          \"primary_terms\" : {\n" +
                    "            \"0\" : 0\n" +
                    "          },\n" +
                    "          \"in_sync_allocations\" : {\n" +
                    "            \"0\" : [ ]\n" +
                    "          },\n" +
                    "          \"rollover_info\" : { }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"index-graveyard\" : {\n" +
                    "        \"tombstones\" : [ ]\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", Strings.toString(builder));
        }
    }
}
