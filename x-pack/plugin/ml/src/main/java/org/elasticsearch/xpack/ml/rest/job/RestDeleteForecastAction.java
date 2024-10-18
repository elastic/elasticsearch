/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.core.ml.job.results.Forecast.FORECAST_ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteForecastAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, BASE_PATH + "anomaly_detectors/{" + ID + "}/_forecast/"),
            new Route(DELETE, BASE_PATH + "anomaly_detectors/{" + ID + "}/_forecast/{" + FORECAST_ID + "}")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_forecast_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String forecastId = restRequest.param(Forecast.FORECAST_ID.getPreferredName(), Metadata.ALL);
        final DeleteForecastAction.Request request = new DeleteForecastAction.Request(jobId, forecastId);
        request.ackTimeout(getAckTimeout(restRequest));
        request.setAllowNoForecasts(restRequest.paramAsBoolean("allow_no_forecasts", request.isAllowNoForecasts()));
        return channel -> client.execute(DeleteForecastAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
