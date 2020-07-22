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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesHotThreadsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes/hot_threads"),
            new Route(GET, "/_nodes/{nodeId}/hot_threads")
        );
    }

    @Override
    public String getName() {
        return "nodes_hot_threads_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(nodesIds);
        nodesHotThreadsRequest.threads(request.paramAsInt("threads", nodesHotThreadsRequest.threads()));
        nodesHotThreadsRequest.ignoreIdleThreads(request.paramAsBoolean("ignore_idle_threads", nodesHotThreadsRequest.ignoreIdleThreads()));
        nodesHotThreadsRequest.type(request.param("type", nodesHotThreadsRequest.type()));
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval(), "interval"));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        nodesHotThreadsRequest.timeout(request.param("timeout"));
        return channel -> client.admin().cluster().nodesHotThreads(
                nodesHotThreadsRequest,
                new RestResponseListener<NodesHotThreadsResponse>(channel) {
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
