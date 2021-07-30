/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.TaskId;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;

public class RestRethrottleAction extends BaseRestHandler {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestRethrottleAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_update_by_query/{taskId}/_rethrottle"),
            new Route(POST, "/_delete_by_query/{taskId}/_rethrottle"),
            new Route(POST, "/_reindex/{taskId}/_rethrottle"));
    }

    @Override
    public String getName() {
        return "rethrottle_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        RethrottleRequest internalRequest = new RethrottleRequest();
        internalRequest.setTaskId(new TaskId(request.param("taskId")));
        Float requestsPerSecond = AbstractBaseReindexRestHandler.parseRequestsPerSecond(request);
        if (requestsPerSecond == null) {
            throw new IllegalArgumentException("requests_per_second is a required parameter");
        }
        internalRequest.setRequestsPerSecond(requestsPerSecond);
        final String groupBy = request.param("group_by", "nodes");
        return channel ->
            client.execute(RethrottleAction.INSTANCE, internalRequest, listTasksResponseListener(nodesInCluster, groupBy, channel));
    }
}
