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
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.ALLOW_NO_MATCH;

public class RestGetDataFrameTransformsAction extends BaseRestHandler {

    public RestGetDataFrameTransformsAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, DataFrameField.REST_BASE_PATH_TRANSFORMS, this);
        controller.registerHandler(RestRequest.Method.GET, DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        GetDataFrameTransformsAction.Request request = new GetDataFrameTransformsAction.Request();

        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        request.setResourceId(id);
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), true));
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        return channel -> client.execute(GetDataFrameTransformsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_get_transforms_action";
    }
}
