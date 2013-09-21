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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesAction extends BaseRestHandler {

    @Inject
    public RestIndicesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/indices", this);
        controller.registerHandler(GET, "/_cat/indices/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.filteredIndices(indices);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                final String[] concreteIndices = clusterStateResponse.getState().metaData().concreteIndicesIgnoreMissing(indices);
                ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest(indices);
                clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
                clusterHealthRequest.indices(indices);
                client.admin().cluster().health(clusterHealthRequest, new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(final ClusterHealthResponse clusterHealthResponse) {
                        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                        indicesStatsRequest.clear().store(true).docs(true);
                        client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                try {
                                    Table tab = buildTable(concreteIndices, clusterHealthResponse, indicesStatsResponse);
                                    channel.sendResponse(RestTable.buildResponse(tab, request, channel));
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

    private Table buildTable(String[] indices, ClusterHealthResponse health, IndicesStatsResponse stats) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("health");
        table.addCell("index");
        table.addCell("pri", "text-align:right;");
        table.addCell("rep", "text-align:right;");
        table.addCell("docs", "text-align:right;");
        table.addCell("docs/del", "text-align:right;");
        table.addCell("size/pri", "text-align:right;");
        table.addCell("size/total", "text-align:right;");
        table.endHeaders();

        for (String index : indices) {
            ClusterIndexHealth indexHealth = health.getIndices().get(index);
            IndexStats indexStats = stats.getIndices().get(index);

            table.startRow();
            table.addCell(indexHealth == null ? "red*" : indexHealth.getStatus().toString().toLowerCase(Locale.getDefault()));
            table.addCell(index);
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfShards());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfReplicas());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getDeleted());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getStore().size());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getStore().size());
            table.endRow();
        }

        return table;
    }
}
