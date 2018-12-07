/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestForecastJobAction extends BaseRestHandler {

    public RestForecastJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_forecast", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_forecast", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_forecast_job_action";
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
        }

        return channel -> client.execute(ForecastJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
