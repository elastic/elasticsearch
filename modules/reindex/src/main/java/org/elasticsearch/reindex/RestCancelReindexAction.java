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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/** REST handler for cancelling an ongoing reindex task. */
@ServerlessScope(Scope.PUBLIC)
public class RestCancelReindexAction extends BaseRestHandler {

    public RestCancelReindexAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_reindex/{taskId}/_cancel"));
    }

    @Override
    public String getName() {
        return "cancel_reindex_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String taskIdParam = request.param("taskId");
        final TaskId taskId = new TaskId(taskIdParam);
        if (taskId.isSet() == false) {
            throw new IllegalArgumentException("invalid taskId provided: " + taskIdParam);
        }

        final boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", true);
        final CancelReindexRequest cancelRequest = new CancelReindexRequest(waitForCompletion);
        cancelRequest.setTargetTaskId(taskId);
        cancelRequest.setTimeout(request.paramAsTime("timeout", TimeValue.THIRTY_SECONDS));

        return channel -> client.execute(TransportCancelReindexAction.TYPE, cancelRequest, new RestToXContentListener<>(channel));
    }
}
