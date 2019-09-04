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
import org.elasticsearch.xpack.core.dataframe.action.PreviewDataFrameTransformAction;

import java.io.IOException;

public class RestPreviewDataFrameTransformAction extends BaseRestHandler {

    public RestPreviewDataFrameTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, DataFrameField.REST_BASE_PATH + "transforms/_preview", this);
    }

    @Override
    public String getName() {
        return "data_frame_preview_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();

        PreviewDataFrameTransformAction.Request request = PreviewDataFrameTransformAction.Request.fromXContent(parser);
        return channel -> client.execute(PreviewDataFrameTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
