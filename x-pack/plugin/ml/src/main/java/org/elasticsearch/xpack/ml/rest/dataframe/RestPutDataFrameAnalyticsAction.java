/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataFrameAnalyticsAction extends BaseRestHandler {

    /**
     * Maximum allowed size of the REST request.
     * <p>
     * It is set so that the user is not able to provide DataFrameAnalyticsConfig._meta map of arbitrary
     * size. Such configs could be a problem upon fetch so it's better to prevent them on put and update
     * actions.
     */
    static final ByteSizeValue MAX_REQUEST_SIZE = ByteSizeValue.ofMb(5);

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}"));
    }

    @Override
    public String getName() {
        return "xpack_ml_put_data_frame_analytics_action";
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
        PutDataFrameAnalyticsAction.Request putRequest;
        try (XContentParser parser = restRequest.contentParser()) {
            putRequest = PutDataFrameAnalyticsAction.Request.parseRequest(id, parser);
        }
        putRequest.timeout(restRequest.paramAsTime("timeout", putRequest.timeout()));

        return channel -> client.execute(PutDataFrameAnalyticsAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
