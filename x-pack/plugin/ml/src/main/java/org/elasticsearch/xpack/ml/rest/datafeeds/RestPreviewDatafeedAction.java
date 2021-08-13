/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestPreviewDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_preview"),
            new Route(GET, BASE_PATH + "datafeeds/_preview"),
            new Route(POST, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_preview"),
            new Route(POST, BASE_PATH + "datafeeds/_preview")
        );
    }

    @Override
    public String getName() {
        return "ml_preview_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PreviewDatafeedAction.Request request = restRequest.hasContentOrSourceParam() ?
            PreviewDatafeedAction.Request.fromXContent(
                restRequest.contentOrSourceParamParser(),
                restRequest.param(DatafeedConfig.ID.getPreferredName(), null)
            ) :
            new PreviewDatafeedAction.Request(restRequest.param(DatafeedConfig.ID.getPreferredName()));
        return channel -> client.execute(PreviewDatafeedAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
