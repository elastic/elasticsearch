/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetDatafeedStatsAction extends BaseRestHandler {

   @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_stats"),
            new Route(GET, MachineLearning.BASE_PATH + "datafeeds/_stats")
        );
    }

    @Override
    public String getName() {
        return "ml_get_datafeed_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        if (Strings.isNullOrEmpty(datafeedId)) {
            datafeedId = GetDatafeedsStatsAction.ALL;
        }
        Request request = new Request(datafeedId);
        if (restRequest.hasParam(Request.ALLOW_NO_DATAFEEDS)) {
            LoggingDeprecationHandler.INSTANCE.usedDeprecatedName(null, () -> null, Request.ALLOW_NO_DATAFEEDS, Request.ALLOW_NO_MATCH);
        }
        request.setAllowNoMatch(
            restRequest.paramAsBoolean(
                Request.ALLOW_NO_MATCH,
                restRequest.paramAsBoolean(Request.ALLOW_NO_DATAFEEDS, request.allowNoMatch())));
        return channel -> client.execute(GetDatafeedsStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
