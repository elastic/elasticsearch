/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetDatafeedStatsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestGetDatafeedStatsAction.class));

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.unmodifiableList(Arrays.asList(
            new ReplacedRoute(GET, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_stats",
                GET, MachineLearning.PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_stats",
                deprecationLogger),
            new ReplacedRoute(GET, MachineLearning.BASE_PATH + "datafeeds/_stats",
                GET, MachineLearning.PRE_V7_BASE_PATH + "datafeeds/_stats", deprecationLogger)
        ));
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
        GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
        request.setAllowNoDatafeeds(restRequest.paramAsBoolean(GetDatafeedsStatsAction.Request.ALLOW_NO_DATAFEEDS.getPreferredName(),
                request.allowNoDatafeeds()));
        return channel -> client.execute(GetDatafeedsStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
