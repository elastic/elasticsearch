/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action.compat;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.compat.StartTransformActionDeprecated;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartTransformActionDeprecated extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(RestStartTransformActionDeprecated.class));

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return singletonList(new DeprecatedRoute(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID_DEPRECATED + "_start",
                TransformMessages.REST_DEPRECATED_ENDPOINT, deprecationLogger));
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
