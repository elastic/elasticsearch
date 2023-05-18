/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateTrainedModelDeploymentAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "xpack_ml_update_trained_model_deployment_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(
                POST,
                MachineLearning.BASE_PATH
                    + "trained_models/{"
                    + StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName()
                    + "}/deployment/_update"
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        UpdateTrainedModelDeploymentAction.Request request = UpdateTrainedModelDeploymentAction.Request.parseRequest(modelId, parser);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));

        return channel -> client.execute(UpdateTrainedModelDeploymentAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
