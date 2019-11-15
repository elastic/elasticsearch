/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;

import java.io.IOException;

public class RestUpdateTransformAction extends BaseRestHandler {

    public RestUpdateTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_update", this);
    }

    @Override
    public String getName() {
        return "transform_update_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(TransformField.ID.getPreferredName());
        boolean deferValidation = restRequest.paramAsBoolean(TransformField.DEFER_VALIDATION.getPreferredName(), false);
        XContentParser parser = restRequest.contentParser();
        UpdateTransformAction.Request request = UpdateTransformAction.Request.fromXContent(parser, id, deferValidation);

        return channel -> client.execute(UpdateTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
