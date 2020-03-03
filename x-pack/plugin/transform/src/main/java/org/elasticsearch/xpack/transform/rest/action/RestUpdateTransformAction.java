/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpdateTransformAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_update"));
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
