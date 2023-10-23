/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.inference.action.DeleteInferenceModelAction;

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
        String taskType = restRequest.param("task_type");
        String modelId = restRequest.param("model_id");

        var request = new DeleteInferenceModelAction.Request(modelId, taskType);
        return channel -> client.execute(DeleteInferenceModelAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
