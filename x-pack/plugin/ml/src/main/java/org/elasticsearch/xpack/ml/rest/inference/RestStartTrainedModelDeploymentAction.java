/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartTrainedModelDeploymentAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "xpack_ml_start_trained_models_deployment_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST,
                MachineLearning.BASE_PATH + "trained_models/{" +
                    StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName() + "}/deployment/_start"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName());
        StartTrainedModelDeploymentAction.Request request = new StartTrainedModelDeploymentAction.Request(modelId);
        if (restRequest.hasParam(StartTrainedModelDeploymentAction.Request.TIMEOUT.getPreferredName())) {
            TimeValue openTimeout = restRequest.paramAsTime(StartTrainedModelDeploymentAction.Request.TIMEOUT.getPreferredName(),
                StartTrainedModelDeploymentAction.DEFAULT_TIMEOUT);
            request.setTimeout(openTimeout);
        }

        return channel -> client.execute(StartTrainedModelDeploymentAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
