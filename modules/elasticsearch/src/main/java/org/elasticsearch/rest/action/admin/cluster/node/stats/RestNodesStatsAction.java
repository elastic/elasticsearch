/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.rest.action.admin.cluster.node.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class RestNodesStatsAction extends BaseRestHandler {

    @Inject public RestNodesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}/stats", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = RestActions.splitNodes(request.param("nodeId"));
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.listenerThreaded(false);
        client.admin().cluster().nodesStats(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
            @Override public void onResponse(NodesStatsResponse result) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("cluster_name", result.clusterName().value());

                    builder.startObject("nodes");
                    for (NodeStats nodeStats : result) {
                        builder.startObject(nodeStats.node().id(), XContentBuilder.FieldCaseConversion.NONE);

                        builder.field("name", nodeStats.node().name(), XContentBuilder.FieldCaseConversion.NONE);

                        if (nodeStats.indices() != null) {
                            nodeStats.indices().toXContent(builder, request);
                        }

                        if (nodeStats.os() != null) {
                            nodeStats.os().toXContent(builder, request);
                        }
                        if (nodeStats.process() != null) {
                            nodeStats.process().toXContent(builder, request);
                        }
                        if (nodeStats.jvm() != null) {
                            nodeStats.jvm().toXContent(builder, request);
                        }
                        if (nodeStats.network() != null) {
                            nodeStats.network().toXContent(builder, request);
                        }
                        if (nodeStats.transport() != null) {
                            nodeStats.transport().toXContent(builder, request);
                        }
                        if (nodeStats.http() != null) {
                            nodeStats.http().toXContent(builder, request);
                        }

                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}