/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteForecastAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestDeleteForecastAction.class));

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(DELETE, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_forecast/"));
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return singletonList(
            new ReplacedRoute(DELETE, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() +
                "}/_forecast/{" + Forecast.FORECAST_ID.getPreferredName() + "}",
                DELETE, MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() +
                "}/_forecast/{" + Forecast.FORECAST_ID.getPreferredName() + "}", deprecationLogger)
        );
    }

    @Override
    public String getName() {
        return "ml_delete_forecast_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String forecastId = restRequest.param(Forecast.FORECAST_ID.getPreferredName(), MetaData.ALL);
        final DeleteForecastAction.Request request = new DeleteForecastAction.Request(jobId, forecastId);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.setAllowNoForecasts(restRequest.paramAsBoolean("allow_no_forecasts", request.isAllowNoForecasts()));
        return channel -> client.execute(DeleteForecastAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
