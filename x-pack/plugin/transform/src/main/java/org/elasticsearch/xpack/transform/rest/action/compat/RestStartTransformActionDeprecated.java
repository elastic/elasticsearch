/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action.compat;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.compat.StartTransformActionDeprecated;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartTransformActionDeprecated extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID_DEPRECATED + "_start")
                .deprecated(TransformMessages.REST_DEPRECATED_ENDPOINT, RestApiVersion.V_8).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(TransformField.ID.getPreferredName());
        StartTransformAction.Request request = new StartTransformAction.Request(id);
        request.timeout(restRequest.paramAsTime(TransformField.TIMEOUT.getPreferredName(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT));
        return channel -> client.execute(StartTransformActionDeprecated.INSTANCE, request,
            new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_start_transform_action";
    }
}
