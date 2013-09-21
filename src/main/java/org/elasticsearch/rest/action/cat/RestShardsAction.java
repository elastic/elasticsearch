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

package org.elasticsearch.rest.action.cat;

import com.google.common.collect.Sets;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestShardsAction extends BaseRestHandler {

    @Inject
    public RestShardsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/shards", this);
        controller.registerHandler(GET, "/_cat/shards/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                final String[] concreteIndices = clusterStateResponse.getState().metaData().concreteIndicesIgnoreMissing(indices);
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                indicesStatsRequest.clear().docs(true).store(true);
                client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                    @Override
                    public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                        try {
                            channel.sendResponse(RestTable.buildResponse(buildTable(concreteIndices, clusterStateResponse, indicesStatsResponse), request, channel));
                        } catch (Throwable e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(new XContentThrowableRestResponse(request, e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response", e1);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    private Table buildTable(String[] concreteIndices, ClusterStateResponse state, IndicesStatsResponse stats) {
        Set<String> indices = Sets.newHashSet(concreteIndices);
        Table table = new Table();
        table.startHeaders()
                .addCell("index", "default:true;")
                .addCell("shard", "default:true;")
                .addCell("p/r", "default:true;")
                .addCell("state", "default:true;")
                .addCell("docs", "text-align:right;")
                .addCell("store", "text-align:right;")
                .addCell("ip", "default:true;")
                .addCell("node", "default:true;")
                .endHeaders();

        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            if (!indices.contains(shard.index())) {
                continue;
            }

            CommonStats shardStats = stats.asMap().get(shard);

            table.startRow();

            table.addCell(shard.index());
            table.addCell(shard.id());
            table.addCell(shard.primary() ? "p" : "r");
            table.addCell(shard.state());
            table.addCell(shardStats == null ? null : shardStats.getDocs().getCount());
            table.addCell(shardStats == null ? null : shardStats.getStore().getSize());
            if (shard.assignedToNode()) {
                String ip = ((InetSocketTransportAddress) state.getState().nodes().get(shard.currentNodeId()).address()).address().getAddress().getHostAddress();
                StringBuilder name = new StringBuilder();
                name.append(state.getState().nodes().get(shard.currentNodeId()).name());
                if (shard.relocating()) {
                    String reloIp = ((InetSocketTransportAddress) state.getState().nodes().get(shard.relocatingNodeId()).address()).address().getAddress().getHostAddress();
                    String reloNme = state.getState().nodes().get(shard.relocatingNodeId()).name();
                    name.append(" -> ");
                    name.append(reloIp);
                    name.append(" ");
                    name.append(reloNme);
                }
                table.addCell(ip);
                table.addCell(name);
            } else {
                table.addCell(null);
                table.addCell(null);
            }

            table.endRow();
        }

        return table;
    }
}
