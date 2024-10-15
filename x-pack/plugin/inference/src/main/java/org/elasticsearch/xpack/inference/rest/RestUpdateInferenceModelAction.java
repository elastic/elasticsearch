/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID_UPDATE_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_UPDATE_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateInferenceModelAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "update_inference_model_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, INFERENCE_ID_UPDATE_PATH), new Route(PUT, TASK_TYPE_INFERENCE_ID_UPDATE_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String inferenceEntityId;
        TaskType taskType;
        if (restRequest.hasParam(INFERENCE_ID)) {
            inferenceEntityId = restRequest.param(INFERENCE_ID);
            taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE_OR_INFERENCE_ID));
        } else {
            throw new ElasticsearchStatusException("Inference ID must be provided in the path", RestStatus.BAD_REQUEST);
        }

        var request = new UpdateInferenceModelAction.Request(
            inferenceEntityId,
            restRequest.requiredContent(),
            restRequest.getXContentType(),
            taskType,
            RestUtils.getMasterNodeTimeout(restRequest)
        );
        return channel -> client.execute(UpdateInferenceModelAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
