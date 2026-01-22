/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.ChunkedRestResponseBodyPart.fromTextChunks;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestUtils.getTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestNodesHotThreadsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_nodes/hot_threads"), new Route(GET, "/_nodes/{nodeId}/hot_threads"));
    }

    @Override
    public String getName() {
        return "nodes_hot_threads_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(
            nodesIds,
            new HotThreads.RequestOptions(
                request.paramAsInt("threads", HotThreads.RequestOptions.DEFAULT.threads()),
                HotThreads.ReportType.of(request.param("type", HotThreads.RequestOptions.DEFAULT.reportType().getTypeValue())),
                HotThreads.SortOrder.of(request.param("sort", HotThreads.RequestOptions.DEFAULT.sortOrder().getOrderValue())),
                request.paramAsTime("interval", HotThreads.RequestOptions.DEFAULT.interval()),
                request.paramAsInt("snapshots", HotThreads.RequestOptions.DEFAULT.snapshots()),
                request.paramAsBoolean("ignore_idle_threads", HotThreads.RequestOptions.DEFAULT.ignoreIdleThreads())
            )
        );
        nodesHotThreadsRequest.setTimeout(getTimeout(request));
        return channel -> client.execute(TransportNodesHotThreadsAction.TYPE, nodesHotThreadsRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(NodesHotThreadsResponse response) {
                response.mustIncRef();
                return RestResponse.chunked(RestStatus.OK, fromTextChunks(TEXT_CONTENT_TYPE, response.getTextChunks()), response::decRef);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
