/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestDeleteDataFrameAnalyticsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}")
        );
    }

    @Override
    public String getName() {
        return "xpack_ml_delete_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        DeleteDataFrameAnalyticsAction.Request request = new DeleteDataFrameAnalyticsAction.Request(id);
        request.setForce(restRequest.paramAsBoolean(DeleteDataFrameAnalyticsAction.Request.FORCE.getPreferredName(), request.isForce()));
        request.timeout(restRequest.paramAsTime(DeleteDataFrameAnalyticsAction.Request.TIMEOUT.getPreferredName(), request.timeout()));
        return channel -> client.execute(DeleteDataFrameAnalyticsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
