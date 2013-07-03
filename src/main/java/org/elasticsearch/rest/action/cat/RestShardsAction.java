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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.table.Row;
import org.elasticsearch.common.table.Table;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestShardsAction extends BaseRestHandler {
    @Inject
    public RestShardsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/shards", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final boolean verbose = request.paramAsBoolean("verbose", false);
        final StringBuilder out = new StringBuilder();

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();                                                                                                     clusterStateRequest.listenerThreaded(false);
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(false);

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                    @Override
                    public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                        RestStatus status = RestStatus.OK;
                        try {
                            channel.sendResponse(new StringRestResponse(status, process(clusterStateResponse, indicesStatsResponse, verbose)));
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

    private String process(ClusterStateResponse state, IndicesStatsResponse stats, boolean headers) {
        Table tab = new Table();
        if (headers) {
            tab.addRow(new Row()
                    .addCell("index")
                    .addCell("shard")
                    .addCell("replica")
                    .addCell("state")
                    .addCell("docs")
                    .addCell("size")
                    .addCell("bytes")
                    .addCell("host")
                    .addCell("node"), true);
        }

        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            Row row = new Row();
            String pri = "r";
            StringBuilder host = new StringBuilder();
            String docs = "";
            String size = "";
            String bytes = "";
            String nodeName = "";

            if (shard.assignedToNode()) {
                host.append(((InetSocketTransportAddress) state.getState().nodes().get(shard.currentNodeId()).address()).address().getHostString());
                nodeName = state.getState().nodes().get(shard.currentNodeId()).name();
            }

            if (shard.relocating()) {
                host.append(" -> ");
                host.append(((InetSocketTransportAddress) state.getState().nodes().get(shard.relocatingNodeId()).address()).address().getHostString());
                host.append(state.getState().nodes().get(shard.relocatingNodeId()).name());
            }

            if (null != stats.asMap().get(shard.globalId())) {
                size = stats.asMap().get(shard.globalId()).getStore().size().toString();
                bytes = new Long(stats.asMap().get(shard.globalId()).getStore().getSizeInBytes()).toString();
                docs = new Long(stats.asMap().get(shard.globalId()).getDocs().getCount()).toString();
            }

            if (shard.primary()) {
                pri = "p";
            }

            row.addCell(shard.index())
                    .addCell(new Integer(shard.shardId().id()).toString())
                    .addCell(pri)
                    .addCell(shard.state().toString())
                    .addCell(docs)
                    .addCell(size)
                    .addCell(bytes)
                    .addCell(host.toString())
                    .addCell(nodeName);
            tab.addRow(row);
        }

        return tab.render(headers);
    }

}
