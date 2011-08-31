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

package org.elasticsearch.rest.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
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
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class RestNodesInfoAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject public RestNodesInfoAction(Settings settings, Client client, RestController controller,
                                       SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}", this);

        this.settingsFilter = settingsFilter;
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = RestActions.splitNodes(request.param("nodeId"));
        final boolean includeSettings = request.paramAsBoolean("settings", false);
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodesIds);
        nodesInfoRequest.listenerThreaded(false);
        client.admin().cluster().nodesInfo(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
            @Override public void onResponse(NodesInfoResponse result) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("cluster_name", result.clusterName().value());

                    builder.startObject("nodes");
                    for (NodeInfo nodeInfo : result) {
                        builder.startObject(nodeInfo.node().id(), XContentBuilder.FieldCaseConversion.NONE);

                        builder.field("name", nodeInfo.node().name(), XContentBuilder.FieldCaseConversion.NONE);
                        builder.field("transport_address", nodeInfo.node().address().toString());

                        builder.startObject("attributes");
                        for (Map.Entry<String, String> attr : nodeInfo.node().attributes().entrySet()) {
                            builder.field(attr.getKey(), attr.getValue());
                        }
                        builder.endObject();

                        for (Map.Entry<String, String> nodeAttribute : nodeInfo.attributes().entrySet()) {
                            builder.field(nodeAttribute.getKey(), nodeAttribute.getValue());
                        }

                        if (includeSettings) {
                            builder.startObject("settings");
                            Settings settings = settingsFilter.filterSettings(nodeInfo.settings());
                            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                                builder.field(entry.getKey(), entry.getValue());
                            }
                            builder.endObject();
                        }

                        if (nodeInfo.os() != null) {
                            nodeInfo.os().toXContent(builder, request);
                        }
                        if (nodeInfo.process() != null) {
                            nodeInfo.process().toXContent(builder, request);
                        }
                        if (nodeInfo.jvm() != null) {
                            nodeInfo.jvm().toXContent(builder, request);
                        }
                        if (nodeInfo.network() != null) {
                            nodeInfo.network().toXContent(builder, request);
                        }
                        if (nodeInfo.transport() != null) {
                            nodeInfo.transport().toXContent(builder, request);
                        }
                        if (nodeInfo.http() != null) {
                            nodeInfo.http().toXContent(builder, request);
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
