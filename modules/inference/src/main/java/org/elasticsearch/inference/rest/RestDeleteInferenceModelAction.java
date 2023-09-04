/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.action.DeleteInferenceModelAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteInferenceModelAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "delete_inference_model_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_inference/{task_type}/{model_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        TaskType taskType = TaskType.fromString(restRequest.param("task_type")); // TODO better error message
        String modelId = restRequest.param("model_id");

        var request = new DeleteInferenceModelAction.Request(modelId, taskType);
        return channel -> client.execute(DeleteInferenceModelAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
