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
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteInferenceEndpointAction extends BaseRestHandler {

    private static final String FORCE_DELETE_NAME = "force";
    private static final String DRY_RUN_NAME = "dry_run";

    @Override
    public String getName() {
        return "delete_inference_model_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, INFERENCE_ID_PATH), new Route(DELETE, TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String inferenceEntityId;
        TaskType taskType;
        boolean forceDelete = false;
        boolean dryRun = false;

        if (restRequest.hasParam(INFERENCE_ID)) {
            inferenceEntityId = restRequest.param(INFERENCE_ID);
            taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE_OR_INFERENCE_ID));
        } else {
            inferenceEntityId = restRequest.param(TASK_TYPE_OR_INFERENCE_ID);
            taskType = TaskType.ANY;
        }

        forceDelete = restRequest.paramAsBoolean(FORCE_DELETE_NAME, false);

        dryRun = restRequest.paramAsBoolean(DRY_RUN_NAME, false);

        var request = new DeleteInferenceEndpointAction.Request(inferenceEntityId, taskType, forceDelete, dryRun);
        return channel -> client.execute(DeleteInferenceEndpointAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
