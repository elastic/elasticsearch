/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.rest.RestCompatibilityChecker;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.CACHE_SIZE;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.QUEUE_CAPACITY;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.THREADS_PER_ALLOCATION;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.TIMEOUT;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.WAIT_FOR;

@ServerlessScope(Scope.PUBLIC)
public class RestStartTrainedModelDeploymentAction extends BaseRestHandler {

    public RestStartTrainedModelDeploymentAction(boolean disableInferenceProcessCache) {
        super();
        if (disableInferenceProcessCache) {
            this.defaultCacheSize = ByteSizeValue.ZERO;
        } else {
            // Don't set the default cache size yet
            defaultCacheSize = null;
        }
    }

    private final ByteSizeValue defaultCacheSize;

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
        String deploymentId = restRequest.param(StartTrainedModelDeploymentAction.Request.DEPLOYMENT_ID.getPreferredName(), modelId);
        StartTrainedModelDeploymentAction.Request request;

        if (restRequest.hasContentOrSourceParam()) { // request has body
            request = StartTrainedModelDeploymentAction.Request.parseRequest(
                modelId,
                deploymentId,
                restRequest.contentOrSourceParamParser()
            );
        } else {
            request = new StartTrainedModelDeploymentAction.Request(modelId, deploymentId);
        }

        if (restRequest.hasParam(TIMEOUT.getPreferredName())) {
            request.setTimeout(restRequest.paramAsTime(TIMEOUT.getPreferredName(), request.getTimeout()));
        }

        request.setWaitForState(
            AllocationStatus.State.fromString(restRequest.param(WAIT_FOR.getPreferredName(), request.getWaitForState().toString()))
        );

        RestCompatibilityChecker.checkAndSetDeprecatedParam(
            NUMBER_OF_ALLOCATIONS.getDeprecatedNames()[0],
            NUMBER_OF_ALLOCATIONS.getPreferredName(),
            RestApiVersion.V_8,
            restRequest,
            // This is to propagate a null value, which paramAsInt does not support.
            (r, s) -> r.hasParam(s) ? (Integer) r.paramAsInt(s, 0) : request.getNumberOfAllocations(),
            request::setNumberOfAllocations
        );

        RestCompatibilityChecker.checkAndSetDeprecatedParam(
            THREADS_PER_ALLOCATION.getDeprecatedNames()[0],
            THREADS_PER_ALLOCATION.getPreferredName(),
            RestApiVersion.V_8,
            restRequest,
            (r, s) -> r.paramAsInt(s, request.getThreadsPerAllocation()),
            request::setThreadsPerAllocation
        );

        request.setQueueCapacity(restRequest.paramAsInt(QUEUE_CAPACITY.getPreferredName(), request.getQueueCapacity()));

        if (restRequest.hasParam(CACHE_SIZE.getPreferredName())) {
            request.setCacheSize(
                ByteSizeValue.parseBytesSizeValue(restRequest.param(CACHE_SIZE.getPreferredName()), CACHE_SIZE.getPreferredName())
            );
        } else if (defaultCacheSize != null && request.getCacheSize() == null) {
            request.setCacheSize(defaultCacheSize);
        }

        request.setPriority(
            restRequest.param(StartTrainedModelDeploymentAction.TaskParams.PRIORITY.getPreferredName(), request.getPriority().toString())
        );

        return channel -> client.execute(StartTrainedModelDeploymentAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
