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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestResponseListener;


/**
 */
public class RestNodesHotThreadsAction extends BaseRestHandler {

    @Inject
    public RestNodesHotThreadsAction(Settings settings, RestController controller) {
        super(settings);
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
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(nodesIds);
        nodesHotThreadsRequest.threads(request.paramAsInt("threads", nodesHotThreadsRequest.threads()));
        nodesHotThreadsRequest.ignoreIdleThreads(request.paramAsBoolean("ignore_idle_threads", nodesHotThreadsRequest.ignoreIdleThreads()));
        nodesHotThreadsRequest.type(request.param("type", nodesHotThreadsRequest.type()));
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval(), "interval"));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        nodesHotThreadsRequest.timeout(request.param("timeout"));
        client.admin().cluster().nodesHotThreads(nodesHotThreadsRequest, new RestResponseListener<NodesHotThreadsResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesHotThreadsResponse response) throws Exception {
                StringBuilder sb = new StringBuilder();
                for (NodeHotThreads node : response.getNodes()) {
                    sb.append("::: ").append(node.getNode().toString()).append("\n");
                    Strings.spaceify(3, node.getHotThreads(), sb);
                    sb.append('\n');
                }
                return new BytesRestResponse(RestStatus.OK, sb.toString());
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
