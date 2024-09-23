/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request.ALLOW_NO_MATCH;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestGetTrainedModelsStatsAction extends BaseRestHandler {

    @UpdateForV9
    // one or more routes use ".replaces" with RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // that route in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return List.of(
            // Route.builder(GET, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}/_stats")
            // .replaces(GET, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}/_stats", RestApiVersion.V_8)
            // .build(),
            // Route.builder(GET, BASE_PATH + "trained_models/_stats")
            // .replaces(GET, BASE_PATH + "inference/_stats", RestApiVersion.V_8)
            // .build()
            new Route(GET, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}/_stats"),
            Route.builder(GET, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}/_stats")
                .deprecateAndKeep("Use the trained_models API instead.")
                .build(),
            new Route(GET, BASE_PATH + "trained_models/_stats"),
            Route.builder(GET, BASE_PATH + "inference/_stats").deprecateAndKeep("Use the trained_models API instead.").build()
        );
    }

    @Override
    public String getName() {
        return "ml_get_trained_models_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (Strings.isNullOrEmpty(modelId)) {
            modelId = Metadata.ALL;
        }
        GetTrainedModelsStatsAction.Request request = new GetTrainedModelsStatsAction.Request(modelId);
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
        }
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), request.isAllowNoResources()));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetTrainedModelsStatsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
