/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;

abstract class BaseInferenceAction extends BaseRestHandler {
    static Params parseParams(RestRequest restRequest) {
        if (restRequest.hasParam(INFERENCE_ID)) {
            var inferenceEntityId = restRequest.param(INFERENCE_ID);
            var taskType = TaskType.fromStringOrStatusException(restRequest.param(TASK_TYPE_OR_INFERENCE_ID));
            return new Params(inferenceEntityId, taskType);
        } else {
            return new Params(restRequest.param(TASK_TYPE_OR_INFERENCE_ID), TaskType.ANY);
        }
    }

    record Params(String inferenceEntityId, TaskType taskType) {}

    static TimeValue parseTimeout(RestRequest restRequest) {
        return restRequest.paramAsTime(InferenceAction.Request.TIMEOUT.getPreferredName(), InferenceAction.Request.DEFAULT_TIMEOUT);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        var params = parseParams(restRequest);
        var content = restRequest.requiredContent();
        var inferTimeout = parseTimeout(restRequest);
        var productUseCase = extractProductUseCase(restRequest);
        var context = new InferenceContext(productUseCase);

        var request = new InferenceActionProxy.Request(
            params.taskType(),
            params.inferenceEntityId(),
            content,
            restRequest.getXContentType(),
            inferTimeout,
            shouldStream(),
            context
        );

        return channel -> client.execute(InferenceActionProxy.INSTANCE, request, ActionListener.withRef(listener(channel), content));
    }

    protected abstract boolean shouldStream();

    protected abstract ActionListener<InferenceAction.Response> listener(RestChannel channel);

    private String extractProductUseCase(RestRequest restRequest) {
        var headers = restRequest.getHeaders();

        if (Objects.isNull(headers) || headers.isEmpty()) {
            return "";
        }

        var productUseCaseHeaders = headers.get(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);

        if (Objects.isNull(productUseCaseHeaders) || productUseCaseHeaders.isEmpty()) {
            return "";
        }

        // We always get the first value as the header doesn't allow multiple values
        return productUseCaseHeaders.get(0);
    }
}
