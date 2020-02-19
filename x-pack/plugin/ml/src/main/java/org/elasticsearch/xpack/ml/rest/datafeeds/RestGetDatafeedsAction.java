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
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetDatafeedsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return List.of(
            new ReplacedRoute(GET, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}",
                GET, MachineLearning.PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}"),
            new ReplacedRoute(GET, MachineLearning.BASE_PATH + "datafeeds",
                GET, MachineLearning.PRE_V7_BASE_PATH + "datafeeds")
        );
    }

    @Override
    public String getName() {
        return "ml_get_datafeeds_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        if (datafeedId == null) {
            datafeedId = GetDatafeedsAction.ALL;
        }
        GetDatafeedsAction.Request request = new GetDatafeedsAction.Request(datafeedId);
        request.setAllowNoDatafeeds(restRequest.paramAsBoolean(GetDatafeedsAction.Request.ALLOW_NO_DATAFEEDS.getPreferredName(),
                request.allowNoDatafeeds()));
        return channel -> client.execute(GetDatafeedsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
