/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestGetDataFrameAnalyticsStatsAction extends BaseRestHandler {

    public RestGetDataFrameAnalyticsStatsAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics/_stats", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics/{"
            + DataFrameAnalyticsConfig.ID.getPreferredName() + "}/_stats", this);
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
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        request.setAllowNoMatch(restRequest.paramAsBoolean(GetDataFrameAnalyticsStatsAction.Request.ALLOW_NO_MATCH.getPreferredName(),
            request.isAllowNoMatch()));

        return channel -> client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
