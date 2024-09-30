/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPutTrainedModelAction extends BaseRestHandler {

    @UpdateForV9
    // one or more routes use ".replaces" with RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // that route in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return List.of(
            // Route.builder(PUT, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}")
            // .replaces(PUT, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}", RestApiVersion.V_8)
            // .build()
            new Route(PUT, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}"),
            Route.builder(PUT, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}")
                .deprecateAndKeep("Use the trained_models API instead.")
                .build()
        );
    }

    @Override
    public String getName() {
        return "xpack_ml_put_trained_model_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        boolean deferDefinitionDecompression = restRequest.paramAsBoolean(PutTrainedModelAction.DEFER_DEFINITION_DECOMPRESSION, false);
        boolean waitForCompletion = restRequest.paramAsBoolean("wait_for_completion", false);
        PutTrainedModelAction.Request putRequest = PutTrainedModelAction.Request.parseRequest(
            id,
            deferDefinitionDecompression,
            waitForCompletion,
            parser
        );
        putRequest.ackTimeout(getAckTimeout(restRequest));
        return channel -> client.execute(PutTrainedModelAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
