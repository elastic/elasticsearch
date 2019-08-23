/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.EstimateMemoryUsageAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestEstimateMemoryUsageAction extends BaseRestHandler {

    public RestEstimateMemoryUsageAction(RestController controller) {
        controller.registerHandler(
            RestRequest.Method.POST,
            MachineLearning.BASE_PATH + "data_frame/analytics/_estimate_memory_usage", this);
    }

    @Override
    public String getName() {
        return "ml_estimate_memory_usage_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutDataFrameAnalyticsAction.Request request =
            PutDataFrameAnalyticsAction.Request.parseRequestForMemoryEstimation(restRequest.contentOrSourceParamParser());
        return channel -> client.execute(EstimateMemoryUsageAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
