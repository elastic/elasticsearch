/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

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
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/** REST handler for the private rank-vectors recall diagnostic. */
@ServerlessScope(Scope.PUBLIC)
public final class RestRankVectorsEvalAction extends BaseRestHandler {

    public static final String ENDPOINT = "_rank_vectors_eval";

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/" + ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RankVectorsEvalSpec spec;
        try (XContentParser parser = request.contentParser()) {
            spec = RankVectorsEvalSpec.parse(parser);
        }
        RankVectorsEvalRequest evalRequest = new RankVectorsEvalRequest(spec, Strings.splitStringByCommaToArray(request.param("index")));
        evalRequest.indicesOptions(IndicesOptions.fromRequest(request, evalRequest.indicesOptions()));
        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
                RankVectorsPlugin.RANK_VECTORS_EVAL_ACTION,
                evalRequest,
                new RestToXContentListener<>(channel)
            );
        }
        evalRequest.setShouldStoreResult(true);
        ActionRequestValidationException validationException = evalRequest.validate();
        if (validationException != null) {
            throw validationException;
        }
        Task task = client.executeAndReturnTask(RankVectorsPlugin.RANK_VECTORS_EVAL_ACTION, evalRequest, ActionListener.noop());
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", new TaskId(client.getLocalNodeId(), task.getId()).toString());
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    @Override
    public String getName() {
        return "rank_vectors_eval_action";
    }
}
