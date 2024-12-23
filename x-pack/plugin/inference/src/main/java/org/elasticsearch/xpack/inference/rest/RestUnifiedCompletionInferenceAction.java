/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.inference.rest.Paths.UNIFIED_INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.UNIFIED_TASK_TYPE_INFERENCE_ID_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestUnifiedCompletionInferenceAction extends BaseRestHandler {
    private final SetOnce<ThreadPool> threadPool;

    public RestUnifiedCompletionInferenceAction(SetOnce<ThreadPool> threadPool) {
        super();
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public String getName() {
        return "unified_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, UNIFIED_INFERENCE_ID_PATH), new Route(POST, UNIFIED_TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        var params = BaseInferenceAction.parseParams(restRequest);

        var inferTimeout = BaseInferenceAction.parseTimeout(restRequest);

        UnifiedCompletionAction.Request request;
        try (var parser = restRequest.contentParser()) {
            request = UnifiedCompletionAction.Request.parseRequest(params.inferenceEntityId(), params.taskType(), inferTimeout, parser);
        }

        return channel -> client.execute(
            UnifiedCompletionAction.INSTANCE,
            request,
            new ServerSentEventsRestActionListener(channel, threadPool)
        );
    }
}
