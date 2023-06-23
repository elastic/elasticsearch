/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.transform.rest.action.RestPutTransformAction.MAX_REQUEST_SIZE;

@ServerlessScope(Scope.PUBLIC)
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
        if (restRequest.contentLength() > MAX_REQUEST_SIZE.getBytes()) {
            throw ExceptionsHelper.badRequestException(
                "Request is too large: was [{}b], expected at most [{}]",
                restRequest.contentLength(),
                MAX_REQUEST_SIZE
            );
        }

        String id = restRequest.param(TransformField.ID.getPreferredName());
        boolean deferValidation = restRequest.paramAsBoolean(TransformField.DEFER_VALIDATION.getPreferredName(), false);
        TimeValue timeout = restRequest.paramAsTime(TransformField.TIMEOUT.getPreferredName(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);

        XContentParser parser = restRequest.contentParser();
        UpdateTransformAction.Request request = UpdateTransformAction.Request.fromXContent(parser, id, deferValidation, timeout);

        return channel -> client.execute(UpdateTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
