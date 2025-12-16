/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.Scope.PUBLIC;

@ServerlessScope(PUBLIC)
public class RestGetReindexAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_reindex/{task_id}"));
    }

    @Override
    public String getName() {
        return "get_reindex_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        TaskId taskId = new TaskId(request.param("task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = RestUtils.getTimeout(request);
        GetReindexRequest getReindexRequest = new GetReindexRequest(taskId, waitForCompletion, timeout);

        return channel -> client.execute(TransportGetReindexAction.TYPE, getReindexRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(ReindexPlugin.CAPABILITY_REINDEX_RESILIENCE);
    }

}
