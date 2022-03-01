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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesHotThreadsAction extends BaseRestHandler {

    private static final String formatDeprecatedMessageWithoutNodeID = "[%s] is a deprecated endpoint. "
        + "Please use [/_nodes/hot_threads] instead.";
    private static final String formatDeprecatedMessageWithNodeID = "[%s] is a deprecated endpoint. "
        + "Please use [/_nodes/{nodeId}/hot_threads] instead.";
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_cluster/nodes/hot_threads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_cluster/nodes/{nodeId}/hot_threads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_cluster/nodes/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_cluster/nodes/{nodeId}/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_NODES_HOTTHREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_nodes/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS = String.format(
        Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_nodes/{nodeId}/hotthreads"
    );

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes/hot_threads"),
            new Route(GET, "/_nodes/{nodeId}/hot_threads"),

            Route.builder(GET, "/_cluster/nodes/hot_threads")
                .deprecated(DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/_cluster/nodes/{nodeId}/hot_threads")
                .deprecated(DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/_cluster/nodes/hotthreads")
                .deprecated(DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/_cluster/nodes/{nodeId}/hotthreads")
                .deprecated(DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/_nodes/hotthreads").deprecated(DEPRECATED_MESSAGE_NODES_HOTTHREADS, RestApiVersion.V_7).build(),
            Route.builder(GET, "/_nodes/{nodeId}/hotthreads")
                .deprecated(DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS, RestApiVersion.V_7)
                .build()
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
        nodesHotThreadsRequest.type(HotThreads.ReportType.of(request.param("type", nodesHotThreadsRequest.type().getTypeValue())));
        nodesHotThreadsRequest.sortOrder(
            HotThreads.SortOrder.of(request.param("sort", nodesHotThreadsRequest.sortOrder().getOrderValue()))
        );
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval(), "interval"));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        nodesHotThreadsRequest.timeout(request.param("timeout"));
        return channel -> client.admin()
            .cluster()
            .nodesHotThreads(nodesHotThreadsRequest, new RestResponseListener<NodesHotThreadsResponse>(channel) {
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
