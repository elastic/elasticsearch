/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.inference.rest.Paths.STREAM_INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.STREAM_TASK_TYPE_INFERENCE_ID_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestStreamInferenceAction extends BaseInferenceAction {
    private final SetOnce<ThreadPool> threadPool;

    public RestStreamInferenceAction(SetOnce<ThreadPool> threadPool) {
        super();
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public String getName() {
        return "stream_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, STREAM_INFERENCE_ID_PATH), new Route(POST, STREAM_TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected InferenceAction.Request prepareInferenceRequest(InferenceAction.Request.Builder builder) {
        return builder.setStream(true).build();
    }

    @Override
    protected ActionListener<InferenceAction.Response> listener(RestChannel channel) {
        return new ServerSentEventsRestActionListener(channel, threadPool);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        var params = parseParams(restRequest);
        var inferTimeout = parseTimeout(restRequest);

        if (params.taskType() == TaskType.CHAT_COMPLETION) {
            UnifiedCompletionAction.Request request;
            try (var parser = restRequest.contentParser()) {
                request = UnifiedCompletionAction.Request.parseRequest(params.inferenceEntityId(), params.taskType(), inferTimeout, parser);
            }

            return channel -> client.execute(
                UnifiedCompletionAction.INSTANCE,
                request,
                new ServerSentEventsRestActionListener(channel, threadPool)
            );
        } else {
            InferenceAction.Request.Builder requestBuilder;
            try (var parser = restRequest.contentParser()) {
                requestBuilder = InferenceAction.Request.parseRequest(params.inferenceEntityId(), params.taskType(), parser);
            }

            requestBuilder.setInferenceTimeout(inferTimeout);
            var request = prepareInferenceRequest(requestBuilder);
            return channel -> client.execute(InferenceAction.INSTANCE, request, listener(channel));
        }
    }
}
