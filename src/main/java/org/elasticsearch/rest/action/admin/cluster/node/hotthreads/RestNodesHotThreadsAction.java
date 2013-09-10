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

package org.elasticsearch.rest.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.*;

import java.io.IOException;


/**
 */
public class RestNodesHotThreadsAction extends BaseRestHandler {

    @Inject
    public RestNodesHotThreadsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/hotthreads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/hot_threads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}/hotthreads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}/hot_threads", this);

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/hotthreads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/hot_threads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/hotthreads", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/hot_threads", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(nodesIds);
        nodesHotThreadsRequest.threads(request.paramAsInt("threads", nodesHotThreadsRequest.threads()));
        nodesHotThreadsRequest.type(request.param("type", nodesHotThreadsRequest.type()));
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval()));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        client.admin().cluster().nodesHotThreads(nodesHotThreadsRequest, new ActionListener<NodesHotThreadsResponse>() {
            @Override
            public void onResponse(NodesHotThreadsResponse response) {
                try {
                    StringBuilder sb = new StringBuilder();
                    for (NodeHotThreads node : response) {
                        sb.append("::: ").append(node.getNode().toString()).append("\n");
                        Strings.spaceify(3, node.getHotThreads(), sb);
                        sb.append('\n');
                    }
                    channel.sendResponse(new StringRestResponse(RestStatus.OK, sb.toString()));
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
}
