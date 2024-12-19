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
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;

@ServerlessScope(Scope.PUBLIC)
public class RestGetInferenceModelAction extends BaseRestHandler {
    public static final String DEFAULT_ELSER_2_CAPABILITY = "default_elser_2";

    @Override
    public String getName() {
        return "get_inference_model_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "_inference"),
            new Route(GET, "_inference/_all"),
            new Route(GET, INFERENCE_ID_PATH),
            new Route(GET, TASK_TYPE_INFERENCE_ID_PATH)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String inferenceEntityId = null;
        TaskType taskType = null;
        if (restRequest.hasParam(TASK_TYPE_OR_INFERENCE_ID) == false && restRequest.hasParam(INFERENCE_ID) == false) {
            // _all models request
            inferenceEntityId = "_all";
            taskType = TaskType.ANY;
        } else if (restRequest.hasParam(INFERENCE_ID)) {
            inferenceEntityId = restRequest.param(INFERENCE_ID);
            taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE_OR_INFERENCE_ID));
        } else {
            inferenceEntityId = restRequest.param(TASK_TYPE_OR_INFERENCE_ID);
            taskType = TaskType.ANY;
        }

        var request = new GetInferenceModelAction.Request(inferenceEntityId, taskType);
        return channel -> client.execute(GetInferenceModelAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DEFAULT_ELSER_2_CAPABILITY);
    }
}
