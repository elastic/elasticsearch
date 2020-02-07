/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutDataFrameAnalyticsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(PUT, MachineLearning.BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID.getPreferredName() + "}"));
    }

    @Override
    public String getName() {
        return "xpack_ml_put_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        PutDataFrameAnalyticsAction.Request putRequest = PutDataFrameAnalyticsAction.Request.parseRequest(id, parser);
        putRequest.timeout(restRequest.paramAsTime("timeout", putRequest.timeout()));

        return channel -> client.execute(PutDataFrameAnalyticsAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
