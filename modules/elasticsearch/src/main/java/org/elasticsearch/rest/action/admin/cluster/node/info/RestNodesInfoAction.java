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

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class RestNodesInfoAction extends BaseRestHandler {

    @Inject public RestNodesInfoAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = RestActions.splitNodes(request.param("nodeId"));
        final boolean includeSettings = request.paramAsBoolean("settings", false);
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodesIds);
        nodesInfoRequest.listenerThreaded(false);
        client.admin().cluster().execNodesInfo(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
            @Override public void onResponse(NodesInfoResponse result) {
                try {
                    JsonBuilder builder = RestJsonBuilder.restJsonBuilder(request);
                    builder.startObject();
                    builder.field("clusterName", result.clusterName().value());

                    builder.startObject("nodes");
                    for (NodeInfo nodeInfo : result) {
                        builder.startObject(nodeInfo.node().id());

                        builder.field("name", nodeInfo.node().name());
                        builder.field("transportAddress", nodeInfo.node().address().toString());
                        builder.field("dataNode", nodeInfo.node().dataNode());

                        for (Map.Entry<String, String> nodeAttribute : nodeInfo.attributes().entrySet()) {
                            builder.field(nodeAttribute.getKey(), nodeAttribute.getValue());
                        }

                        if (includeSettings) {
                            builder.startObject("settings");
                            for (Map.Entry<String, String> entry : nodeInfo.settings().getAsMap().entrySet()) {
                                builder.field(entry.getKey(), entry.getValue());
                            }
                            builder.endObject();
                        }

                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                    channel.sendResponse(new JsonRestResponse(request, RestResponse.Status.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
