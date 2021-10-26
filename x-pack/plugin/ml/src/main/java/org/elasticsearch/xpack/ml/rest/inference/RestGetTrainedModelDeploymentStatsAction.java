/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetTrainedModelDeploymentStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "ml_get_trained_models_deployment_stats_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(GET,
                MachineLearning.BASE_PATH + "trained_models/{" +
                    StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName() + "}/deployment/_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName());
        GetDeploymentStatsAction.Request request = new GetDeploymentStatsAction.Request(modelId);

        request.setAllowNoMatch(
            restRequest.paramAsBoolean(
                GetDeploymentStatsAction.Request.ALLOW_NO_MATCH, request.isAllowNoMatch()));

        return channel -> client.execute(GetDeploymentStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
