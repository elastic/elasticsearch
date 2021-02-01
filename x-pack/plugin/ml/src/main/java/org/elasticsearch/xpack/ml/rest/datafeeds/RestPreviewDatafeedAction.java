/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestPreviewDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(GET, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_preview")
        );
    }

    @Override
    public String getName() {
        return "ml_preview_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        PreviewDatafeedAction.Request request = new PreviewDatafeedAction.Request(datafeedId);
        return channel -> client.execute(PreviewDatafeedAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
