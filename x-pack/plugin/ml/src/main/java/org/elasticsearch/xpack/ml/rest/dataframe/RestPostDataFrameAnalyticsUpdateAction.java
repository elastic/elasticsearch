/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.rest.dataframe.RestPutDataFrameAnalyticsAction.MAX_REQUEST_SIZE;

@ServerlessScope(Scope.PUBLIC)
public class RestPostDataFrameAnalyticsUpdateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}/_update"));
    }

    @Override
    public String getName() {
        return "xpack_ml_post_data_frame_analytics_update_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.contentLength() > MAX_REQUEST_SIZE.getBytes()) {
            throw ExceptionsHelper.badRequestException(
                "Request is too large: was [{}b], expected at most [{}]",
                restRequest.contentLength(),
                MAX_REQUEST_SIZE
            );
        }

        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        UpdateDataFrameAnalyticsAction.Request updateRequest = UpdateDataFrameAnalyticsAction.Request.parseRequest(id, parser);
        updateRequest.timeout(restRequest.paramAsTime("timeout", updateRequest.timeout()));

        return channel -> client.execute(UpdateDataFrameAnalyticsAction.INSTANCE, updateRequest, new RestToXContentListener<>(channel));
    }
}
