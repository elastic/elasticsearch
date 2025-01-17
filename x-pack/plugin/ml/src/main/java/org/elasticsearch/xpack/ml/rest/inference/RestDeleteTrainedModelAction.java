/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.TIMEOUT;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteTrainedModelAction extends BaseRestHandler {

    @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING)
    // one or more routes use ".replaces" with RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // that route in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return List.of(
            // Route.builder(DELETE, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}")
            // .replaces(DELETE, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}", RestApiVersion.V_8)
            // .build()
            new Route(DELETE, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}"),
            Route.builder(DELETE, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}")
                .deprecateAndKeep("Use the trained_models API instead.")
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_delete_trained_models_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        DeleteTrainedModelAction.Request request = new DeleteTrainedModelAction.Request(modelId);
        if (restRequest.hasParam(TIMEOUT.getPreferredName())) {
            TimeValue timeout = restRequest.paramAsTime(TIMEOUT.getPreferredName(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
            request.ackTimeout(timeout);
        }
        request.setForce(restRequest.paramAsBoolean(DeleteTrainedModelAction.Request.FORCE.getPreferredName(), request.isForce()));
        return channel -> client.execute(DeleteTrainedModelAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
