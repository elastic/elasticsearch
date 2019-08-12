/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction;

import java.io.IOException;

public class RestUpdateDataFrameTransformAction extends BaseRestHandler {

    public RestUpdateDataFrameTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_update", this);
    }

    @Override
    public String getName() {
        return "data_frame_update_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        boolean deferValidation = restRequest.paramAsBoolean(DataFrameField.DEFER_VALIDATION.getPreferredName(), false);
        XContentParser parser = restRequest.contentParser();
        UpdateDataFrameTransformAction.Request request = UpdateDataFrameTransformAction.Request.fromXContent(parser, id, deferValidation);

        return channel -> client.execute(UpdateDataFrameTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
