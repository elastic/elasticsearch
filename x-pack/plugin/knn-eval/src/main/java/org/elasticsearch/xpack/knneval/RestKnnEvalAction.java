/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/** Handles synchronous and task-backed kNN recall evaluations for DiskBBQ vector fields. */
@ServerlessScope(Scope.INTERNAL)
final class RestKnnEvalAction extends BaseRestHandler {

    private static final String ENDPOINT = "_knn_eval";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/" + ENDPOINT), new Route(POST, "/{index}/" + ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        KnnEvalSpec spec;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            spec = KnnEvalSpec.parse(parser);
        }
        KnnEvalRequest knnEvalRequest = new KnnEvalRequest(spec, Strings.splitStringByCommaToArray(request.param("index")));
        knnEvalRequest.indicesOptions(IndicesOptions.fromRequest(request, knnEvalRequest.indicesOptions()));
        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.execute(KnnEvalPlugin.KNN_EVAL_ACTION, knnEvalRequest, new RestToXContentListener<>(channel));
        }
        knnEvalRequest.setShouldStoreResult(true);
        ActionRequestValidationException validationException = knnEvalRequest.validate();
        if (validationException != null) {
            throw validationException;
        }
        Task task = client.executeAndReturnTask(KnnEvalPlugin.KNN_EVAL_ACTION, knnEvalRequest, ActionListener.noop());
        return sendTask(client.getLocalNodeId(), task);
    }

    private static RestChannelConsumer sendTask(String localNodeId, Task task) {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", localNodeId + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    @Override
    public String getName() {
        return "knn_eval_action";
    }
}
