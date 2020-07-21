/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestForecastJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_forecast")
        );
    }

    @Override
    public String getName() {
        return "ml_forecast_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final ForecastJobAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = ForecastJobAction.Request.parseRequest(jobId, parser);
        } else {
            request = new ForecastJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
            if (restRequest.hasParam(ForecastJobAction.Request.DURATION.getPreferredName())) {
                request.setDuration(restRequest.param(ForecastJobAction.Request.DURATION.getPreferredName()));
            }
            if (restRequest.hasParam(ForecastJobAction.Request.EXPIRES_IN.getPreferredName())) {
                request.setExpiresIn(restRequest.param(ForecastJobAction.Request.EXPIRES_IN.getPreferredName()));
            }
            if (restRequest.hasParam(ForecastJobAction.Request.MAX_MODEL_MEMORY.getPreferredName())) {
                long limit = ByteSizeValue.parseBytesSizeValue(
                    restRequest.param(ForecastJobAction.Request.MAX_MODEL_MEMORY.getPreferredName()),
                    ForecastJobAction.Request.MAX_MODEL_MEMORY.getPreferredName()
                ).getBytes();
                request.setMaxModelMemory(limit);
            }
        }

        return channel -> client.execute(ForecastJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
