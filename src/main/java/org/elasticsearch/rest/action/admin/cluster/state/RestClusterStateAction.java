/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.admin.cluster.state;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RestClusterStateAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestClusterStateAction(Settings settings, Client client, RestController controller,
                                  SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/state", this);

        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.filterNodes(request.paramAsBoolean("filter_nodes", clusterStateRequest.filterNodes()));
        clusterStateRequest.filterRoutingTable(request.paramAsBoolean("filter_routing_table", clusterStateRequest.filterRoutingTable()));
        clusterStateRequest.filterMetaData(request.paramAsBoolean("filter_metadata", clusterStateRequest.filterMetaData()));
        clusterStateRequest.filterBlocks(request.paramAsBoolean("filter_blocks", clusterStateRequest.filterBlocks()));
        clusterStateRequest.filteredIndices(RestActions.splitIndices(request.param("filter_indices", null)));
        clusterStateRequest.filteredIndexTemplates(request.paramAsStringArray("filter_index_templates", Strings.EMPTY_ARRAY));
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse response) {
                try {
                    ClusterState state = response.state();
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    builder.field("cluster_name", response.clusterName().value());

                    if (!clusterStateRequest.filterNodes()) {
                        builder.field("master_node", state.nodes().masterNodeId());
                    }

                    // blocks
                    if (!clusterStateRequest.filterBlocks()) {
                        builder.startObject("blocks");

                        if (!state.blocks().global().isEmpty()) {
                            builder.startObject("global");
                            for (ClusterBlock block : state.blocks().global()) {
                                block.toXContent(builder, request);
                            }
                            builder.endObject();
                        }

                        if (!state.blocks().indices().isEmpty()) {
                            builder.startObject("indices");
                            for (Map.Entry<String, ImmutableSet<ClusterBlock>> entry : state.blocks().indices().entrySet()) {
                                builder.startObject(entry.getKey());
                                for (ClusterBlock block : entry.getValue()) {
                                    block.toXContent(builder, request);
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }

                        builder.endObject();
                    }

                    // nodes
                    if (!clusterStateRequest.filterNodes()) {
                        builder.startObject("nodes");
                        for (DiscoveryNode node : state.nodes()) {
                            builder.startObject(node.id(), XContentBuilder.FieldCaseConversion.NONE);
                            builder.field("name", node.name());
                            builder.field("transport_address", node.address().toString());

                            builder.startObject("attributes");
                            for (Map.Entry<String, String> attr : node.attributes().entrySet()) {
                                builder.field(attr.getKey(), attr.getValue());
                            }
                            builder.endObject();

                            builder.endObject();
                        }
                        builder.endObject();
                    }

                    // meta data
                    if (!clusterStateRequest.filterMetaData()) {
                        builder.startObject("metadata");

                        builder.startObject("templates");
                        for (IndexTemplateMetaData templateMetaData : state.metaData().templates().values()) {
                            builder.startObject(templateMetaData.name(), XContentBuilder.FieldCaseConversion.NONE);

                            builder.field("template", templateMetaData.template());
                            builder.field("order", templateMetaData.order());

                            builder.startObject("settings");
                            Settings settings = settingsFilter.filterSettings(templateMetaData.settings());
                            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                                builder.field(entry.getKey(), entry.getValue());
                            }
                            builder.endObject();

                            builder.startObject("mappings");
                            for (Map.Entry<String, CompressedString> entry : templateMetaData.mappings().entrySet()) {
                                byte[] mappingSource = entry.getValue().uncompressed();
                                XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                                Map<String, Object> mapping = parser.map();
                                if (mapping.size() == 1 && mapping.containsKey(entry.getKey())) {
                                    // the type name is the root value, reduce it
                                    mapping = (Map<String, Object>) mapping.get(entry.getKey());
                                }
                                builder.field(entry.getKey());
                                builder.map(mapping);
                            }
                            builder.endObject();


                            builder.endObject();
                        }
                        builder.endObject();

                        builder.startObject("indices");
                        for (IndexMetaData indexMetaData : state.metaData()) {
                            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);

                            builder.field("state", indexMetaData.state().toString().toLowerCase());

                            builder.startObject("settings");
                            Settings settings = settingsFilter.filterSettings(indexMetaData.settings());
                            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                                builder.field(entry.getKey(), entry.getValue());
                            }
                            builder.endObject();

                            builder.startObject("mappings");
                            for (Map.Entry<String, MappingMetaData> entry : indexMetaData.mappings().entrySet()) {
                                byte[] mappingSource = entry.getValue().source().uncompressed();
                                XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                                Map<String, Object> mapping = parser.map();
                                if (mapping.size() == 1 && mapping.containsKey(entry.getKey())) {
                                    // the type name is the root value, reduce it
                                    mapping = (Map<String, Object>) mapping.get(entry.getKey());
                                }
                                builder.field(entry.getKey());
                                builder.map(mapping);
                            }
                            builder.endObject();

                            builder.startArray("aliases");
                            for (String alias : indexMetaData.aliases().keySet()) {
                                builder.value(alias);
                            }
                            builder.endArray();

                            builder.endObject();
                        }
                        builder.endObject();

                        builder.endObject();
                    }

                    // routing table
                    if (!clusterStateRequest.filterRoutingTable()) {
                        builder.startObject("routing_table");
                        builder.startObject("indices");
                        for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
                            builder.startObject(indexRoutingTable.index(), XContentBuilder.FieldCaseConversion.NONE);
                            builder.startObject("shards");
                            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                                builder.startArray(Integer.toString(indexShardRoutingTable.shardId().id()));
                                for (ShardRouting shardRouting : indexShardRoutingTable) {
                                    jsonShardRouting(builder, shardRouting);
                                }
                                builder.endArray();
                            }
                            builder.endObject();
                            builder.endObject();
                        }
                        builder.endObject();
                        builder.endObject();
                    }

                    // routing nodes
                    if (!clusterStateRequest.filterRoutingTable()) {
                        builder.startObject("routing_nodes");
                        builder.startArray("unassigned");
                        for (ShardRouting shardRouting : state.readOnlyRoutingNodes().unassigned()) {
                            jsonShardRouting(builder, shardRouting);
                        }
                        builder.endArray();

                        builder.startObject("nodes");
                        for (RoutingNode routingNode : state.readOnlyRoutingNodes()) {
                            builder.startArray(routingNode.nodeId(), XContentBuilder.FieldCaseConversion.NONE);
                            for (ShardRouting shardRouting : routingNode) {
                                jsonShardRouting(builder, shardRouting);
                            }
                            builder.endArray();
                        }
                        builder.endObject();

                        builder.endObject();
                    }

                    if (!clusterStateRequest.filterRoutingTable()) {
                        builder.startArray("allocations");
                        for (Map.Entry<ShardId, List<AllocationExplanation.NodeExplanation>> entry : state.allocationExplanation().explanations().entrySet()) {
                            builder.startObject();
                            builder.field("index", entry.getKey().index().name());
                            builder.field("shard", entry.getKey().id());
                            builder.startArray("explanations");
                            for (AllocationExplanation.NodeExplanation nodeExplanation : entry.getValue()) {
                                builder.field("desc", nodeExplanation.description());
                                if (nodeExplanation.node() != null) {
                                    builder.startObject("node");
                                    builder.field("id", nodeExplanation.node().id());
                                    builder.field("name", nodeExplanation.node().name());
                                    builder.endObject();
                                }
                            }
                            builder.endArray();
                            builder.endObject();
                        }
                        builder.endArray();
                    }


                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            private void jsonShardRouting(XContentBuilder builder, ShardRouting shardRouting) throws IOException {
                builder.startObject()
                        .field("state", shardRouting.state())
                        .field("primary", shardRouting.primary())
                        .field("node", shardRouting.currentNodeId())
                        .field("relocating_node", shardRouting.relocatingNodeId())
                        .field("shard", shardRouting.shardId().id())
                        .field("index", shardRouting.shardId().index().name())
                        .endObject();
            }

            @Override
            public void onFailure(Throwable e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to handle cluster state", e);
                }
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}