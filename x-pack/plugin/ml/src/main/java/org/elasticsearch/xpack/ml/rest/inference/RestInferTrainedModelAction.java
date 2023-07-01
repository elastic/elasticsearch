/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestInferTrainedModelAction extends BaseRestHandler {

    static final String PATH = BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}/_infer";

    @Override
    public String getName() {
        return "xpack_ml_infer_trained_models_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (restRequest.hasContent() == false) {
            throw ExceptionsHelper.badRequestException("requires body");
        }
        InferModelAction.Request.Builder request = InferModelAction.Request.parseRequest(modelId, restRequest.contentParser());

        if (restRequest.hasParam(InferModelAction.Request.TIMEOUT.getPreferredName())) {
            TimeValue inferTimeout = restRequest.paramAsTime(
                InferModelAction.Request.TIMEOUT.getPreferredName(),
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
            );
            request.setInferenceTimeout(inferTimeout);
        }

        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            InferModelAction.EXTERNAL_INSTANCE,
            request.build(),
            new RestToXContentListener<>(channel)
        );
    }
}
