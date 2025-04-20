/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.GetInferenceServicesAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_SERVICES_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_SERVICES_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetInferenceServicesAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_inference_services_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, INFERENCE_SERVICES_PATH), new Route(GET, TASK_TYPE_INFERENCE_SERVICES_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        TaskType taskType;
        if (restRequest.hasParam(TASK_TYPE)) {
            taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE));
        } else {
            taskType = TaskType.ANY;
        }

        var request = new GetInferenceServicesAction.Request(taskType);
        return channel -> client.execute(GetInferenceServicesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
