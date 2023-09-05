/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.action.InferenceAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestInferenceAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_inference/{task_type}/{model_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String taskType = restRequest.param("task_type");
        String modelId = restRequest.param("model_id");
        var request = InferenceAction.Request.parseRequest(modelId, taskType, restRequest.contentParser());

        return channel -> client.execute(InferenceAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
