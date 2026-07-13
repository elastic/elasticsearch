/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPreviewDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "datafeeds/{" + ID + "}/_preview"),
            new Route(GET, BASE_PATH + "datafeeds/_preview"),
            new Route(POST, BASE_PATH + "datafeeds/{" + ID + "}/_preview"),
            new Route(POST, BASE_PATH + "datafeeds/_preview")
        );
    }

    @Override
    public String getName() {
        return "ml_preview_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String startTime = restRequest.param(StartDatafeedAction.START_TIME.getPreferredName(), null);
        String endTime = restRequest.param(StartDatafeedAction.END_TIME.getPreferredName(), null);
        PreviewDatafeedAction.Request request = restRequest.hasContentOrSourceParam()
            ? PreviewDatafeedAction.Request.fromXContent(
                restRequest.contentOrSourceParamParser(),
                restRequest.param(DatafeedConfig.ID.getPreferredName(), null)
            ).setStart(startTime).setEnd(endTime).build()
            : new PreviewDatafeedAction.Request(restRequest.param(DatafeedConfig.ID.getPreferredName()), startTime, endTime);
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            PreviewDatafeedAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
