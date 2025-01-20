/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestStartDataFrameAnalyticsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}/_start"));
    }

    @Override
    public String getName() {
        return "xpack_ml_start_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        StartDataFrameAnalyticsAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            request = StartDataFrameAnalyticsAction.Request.parseRequest(id, restRequest.contentOrSourceParamParser());
        } else {
            request = new StartDataFrameAnalyticsAction.Request(id);
            if (restRequest.hasParam(StartDataFrameAnalyticsAction.Request.TIMEOUT.getPreferredName())) {
                TimeValue timeout = restRequest.paramAsTime(
                    StartDataFrameAnalyticsAction.Request.TIMEOUT.getPreferredName(),
                    request.getTimeout()
                );
                request.setTimeout(timeout);
            }
        }
        return channel -> client.execute(StartDataFrameAnalyticsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
