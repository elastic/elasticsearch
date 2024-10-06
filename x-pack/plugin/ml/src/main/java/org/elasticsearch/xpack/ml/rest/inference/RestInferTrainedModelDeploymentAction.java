/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestInferTrainedModelDeploymentAction extends BaseRestHandler {

    static final String PATH = BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}/deployment/_infer";

    @Override
    public String getName() {
        return "xpack_ml_infer_trained_models_deployment_action";
    }

    @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING)
    // these routes were ".deprecated" in RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // this API in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            // Route.builder(POST, PATH)
            // .deprecated(
            // "["
            // + POST.name()
            // + " "
            // + PATH
            // + "] is deprecated! Use ["
            // + POST.name()
            // + " "
            // + RestInferTrainedModelAction.PATH
            // + "] instead.",
            // RestApiVersion.V_8
            // )
            // .build()
            Route.builder(POST, PATH)
                .deprecateAndKeep(
                    "["
                        + POST.name()
                        + " "
                        + PATH
                        + "] is deprecated! Use ["
                        + POST.name()
                        + " "
                        + RestInferTrainedModelAction.PATH
                        + "] instead."
                )
                .build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (restRequest.hasContent() == false) {
            throw ExceptionsHelper.badRequestException("requires body");
        }
        InferModelAction.Request.Builder requestBuilder = InferModelAction.Request.parseRequest(modelId, restRequest.contentParser());

        if (restRequest.hasParam(InferModelAction.Request.TIMEOUT.getPreferredName())) {
            TimeValue inferTimeout = restRequest.paramAsTime(
                InferModelAction.Request.TIMEOUT.getPreferredName(),
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
            );
            requestBuilder.setInferenceTimeout(inferTimeout);
        }

        // Unlike the _infer API, deployment/_infer only accepts a single document
        var request = requestBuilder.build();
        if (request.getObjectsToInfer() != null && request.getObjectsToInfer().size() > 1) {
            ValidationException ex = new ValidationException();
            ex.addValidationError("multiple documents are not supported");
            throw ex;
        }

        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            InferModelAction.EXTERNAL_INSTANCE,
            request,
            // This API is deprecated but refactoring makes it simpler to call
            // the new replacement API and swap in the old response.
            ActionListener.wrap(response -> {
                InferTrainedModelDeploymentAction.Response oldResponse = new InferTrainedModelDeploymentAction.Response(
                    response.getInferenceResults()
                );
                new RestToXContentListener<>(channel).onResponse(oldResponse);
            }, e -> new RestToXContentListener<>(channel).onFailure(e))

        );
    }
}
