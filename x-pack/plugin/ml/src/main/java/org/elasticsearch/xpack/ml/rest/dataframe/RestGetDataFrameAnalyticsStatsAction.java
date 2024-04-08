/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataFrameAnalyticsStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "data_frame/analytics/_stats"),
            new Route(GET, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}/_stats")
        );
    }

    @Override
    public String getName() {
        return "xpack_ml_get_data_frame_analytics_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        GetDataFrameAnalyticsStatsAction.Request request = new GetDataFrameAnalyticsStatsAction.Request();
        if (Strings.isNullOrEmpty(id) == false) {
            request.setId(id);
        }
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
        }
        request.setAllowNoMatch(
            restRequest.paramAsBoolean(GetDataFrameAnalyticsStatsAction.Request.ALLOW_NO_MATCH.getPreferredName(), request.isAllowNoMatch())
        );

        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetDataFrameAnalyticsStatsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(GetDataFrameAnalyticsStatsAction.Response.VERBOSE);
    }
}
