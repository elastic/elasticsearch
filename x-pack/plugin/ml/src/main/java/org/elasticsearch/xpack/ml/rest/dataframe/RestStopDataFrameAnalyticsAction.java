/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopDataFrameAnalyticsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(
                POST, MachineLearning.BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID.getPreferredName() + "}/_stop"));
    }

    @Override
    public String getName() {
        return "xpack_ml_stop_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        StopDataFrameAnalyticsAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            request = StopDataFrameAnalyticsAction.Request.parseRequest(id, restRequest.contentOrSourceParamParser());
        } else {
            request = new StopDataFrameAnalyticsAction.Request(id);
            request.setTimeout(restRequest.paramAsTime(StopDataFrameAnalyticsAction.Request.TIMEOUT.getPreferredName(),
                request.getTimeout()));
            request.setAllowNoMatch(restRequest.paramAsBoolean(StopDataFrameAnalyticsAction.Request.ALLOW_NO_MATCH.getPreferredName(),
                request.allowNoMatch()));
            request.setForce(restRequest.paramAsBoolean(StopDataFrameAnalyticsAction.Request.FORCE.getPreferredName(), request.isForce()));
        }
        return channel -> client.execute(StopDataFrameAnalyticsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
