/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.ALLOW_NO_MATCH;

public class RestGetDataFrameTransformsStatsAction extends BaseRestHandler {

    public RestGetDataFrameTransformsStatsAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, DataFrameField.REST_BASE_PATH_TRANSFORMS + "_stats", this);
        controller.registerHandler(RestRequest.Method.GET, DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_stats", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        GetDataFrameTransformsStatsAction.Request request = new GetDataFrameTransformsStatsAction.Request(id);
        request.setAllowNoMatch(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), true));
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        return channel -> client.execute(GetDataFrameTransformsStatsAction.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_get_transforms_stats_action";
    }
}
