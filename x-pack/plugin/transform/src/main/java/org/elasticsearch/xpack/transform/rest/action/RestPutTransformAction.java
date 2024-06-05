/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutTransformAction extends BaseRestHandler {

    /**
     * Maximum allowed size of the REST request.
     *
     * It is set so that the user is able to provide elaborate painless scripts but not able to provide TransformConfig._meta map of
     * arbitrary size. Such transform configs of an arbitrary size could be a problem upon fetch so it's better to prevent them on Put and
     * Update actions.
     */
    static final ByteSizeValue MAX_REQUEST_SIZE = ByteSizeValue.ofMb(5);

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID));
    }

    @Override
    public String getName() {
        return "transform_put_transform_action";
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
        XContentParser parser = restRequest.contentParser();

        boolean deferValidation = restRequest.paramAsBoolean(TransformField.DEFER_VALIDATION.getPreferredName(), false);
        TimeValue timeout = restRequest.paramAsTime(TransformField.TIMEOUT.getPreferredName(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);

        PutTransformAction.Request request = PutTransformAction.Request.fromXContent(parser, id, deferValidation, timeout);

        return channel -> client.execute(PutTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
