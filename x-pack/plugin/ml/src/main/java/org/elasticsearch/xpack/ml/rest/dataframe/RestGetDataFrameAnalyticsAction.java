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
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestGetDataFrameAnalyticsAction extends BaseRestHandler {

    public RestGetDataFrameAnalyticsAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics/{"
            + DataFrameAnalyticsConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetDataFrameAnalyticsAction.Request request = new GetDataFrameAnalyticsAction.Request();
        String id = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        if (Strings.isNullOrEmpty(id) == false) {
            request.setResourceId(id);
        }
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        request.setAllowNoResources(restRequest.paramAsBoolean(GetDataFrameAnalyticsAction.Request.ALLOW_NO_MATCH.getPreferredName(),
                request.isAllowNoResources()));
        return channel -> client.execute(GetDataFrameAnalyticsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
