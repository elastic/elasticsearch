/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.EstimateModelMemoryAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEstimateModelMemoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, MachineLearning.BASE_PATH + "anomaly_detectors/_estimate_model_memory"));
    }

    @Override
    public String getName() {
        return "ml_estimate_model_memory_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        EstimateModelMemoryAction.Request request =
            EstimateModelMemoryAction.Request.parseRequest(restRequest.contentOrSourceParamParser());
        return channel -> client.execute(EstimateModelMemoryAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
