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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStatus;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.INFERENCE_THREADS;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.MODEL_THREADS;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.QUEUE_CAPACITY;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.TIMEOUT;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.WAIT_FOR;

public class RestStartTrainedModelDeploymentAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "xpack_ml_start_trained_models_deployment_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(
                POST,
                MachineLearning.BASE_PATH
                    + "trained_models/{"
                    + StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName()
                    + "}/deployment/_start"
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(StartTrainedModelDeploymentAction.Request.MODEL_ID.getPreferredName());
        StartTrainedModelDeploymentAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            request = StartTrainedModelDeploymentAction.Request.parseRequest(modelId, restRequest.contentOrSourceParamParser());
        } else {
            request = new StartTrainedModelDeploymentAction.Request(modelId);
            if (restRequest.hasParam(TIMEOUT.getPreferredName())) {
                TimeValue openTimeout = restRequest.paramAsTime(
                    TIMEOUT.getPreferredName(),
                    StartTrainedModelDeploymentAction.DEFAULT_TIMEOUT
                );
                request.setTimeout(openTimeout);
            }
            request.setWaitForState(
                AllocationStatus.State.fromString(restRequest.param(WAIT_FOR.getPreferredName(), AllocationStatus.State.STARTED.toString()))
            );
            request.setInferenceThreads(restRequest.paramAsInt(INFERENCE_THREADS.getPreferredName(), request.getInferenceThreads()));
            request.setModelThreads(restRequest.paramAsInt(MODEL_THREADS.getPreferredName(), request.getModelThreads()));
            request.setQueueCapacity(restRequest.paramAsInt(QUEUE_CAPACITY.getPreferredName(), request.getQueueCapacity()));
        }

        return channel -> client.execute(StartTrainedModelDeploymentAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
