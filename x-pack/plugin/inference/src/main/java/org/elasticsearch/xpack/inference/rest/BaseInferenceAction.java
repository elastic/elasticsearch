/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;

abstract class BaseInferenceAction extends BaseRestHandler {
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String inferenceEntityId;
        TaskType taskType;
        if (restRequest.hasParam(INFERENCE_ID)) {
            inferenceEntityId = restRequest.param(INFERENCE_ID);
            taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE_OR_INFERENCE_ID));
        } else {
            inferenceEntityId = restRequest.param(TASK_TYPE_OR_INFERENCE_ID);
            taskType = TaskType.ANY;
        }

        InferenceAction.Request.Builder requestBuilder;
        try (var parser = restRequest.contentParser()) {
            requestBuilder = InferenceAction.Request.parseRequest(inferenceEntityId, taskType, parser);
        }

        var inferTimeout = restRequest.paramAsTime(
            InferenceAction.Request.TIMEOUT.getPreferredName(),
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        requestBuilder.setInferenceTimeout(inferTimeout);
        var request = prepareInferenceRequest(requestBuilder);
        return channel -> client.execute(InferenceAction.INSTANCE, request, listener(channel));
    }

    protected InferenceAction.Request prepareInferenceRequest(InferenceAction.Request.Builder builder) {
        return builder.build();
    }

    protected abstract ActionListener<InferenceAction.Response> listener(RestChannel channel);
}
