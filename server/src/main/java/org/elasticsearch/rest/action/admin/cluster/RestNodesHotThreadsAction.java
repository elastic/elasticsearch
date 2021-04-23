/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
