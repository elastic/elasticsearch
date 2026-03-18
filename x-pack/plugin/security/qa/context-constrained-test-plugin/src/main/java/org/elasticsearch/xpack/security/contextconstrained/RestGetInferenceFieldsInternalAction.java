/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.contextconstrained;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsInternalAction;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Test-only REST handler that dispatches a context-constrained action via NodeClient
 * without calling ContextConstrainedAction.openContext(). Used to verify that the
 * AuthorizationService context constraint check blocks unauthorized local dispatch.
 */
public class RestGetInferenceFieldsInternalAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_inference_fields_internal_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_inference_fields_internal"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indices = restRequest.param("index").split(",");
        var request = new GetInferenceFieldsInternalAction.Request(indices, Map.of(), false, false, null);
        return channel -> client.execute(
            GetInferenceFieldsInternalAction.INSTANCE,
            request,
            new RestActionListener<>(channel) {
                @Override
                protected void processResponse(GetInferenceFieldsInternalAction.Response response) throws Exception {
                    channel.sendResponse(new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, "OK: got inference fields"));
                }
            }
        );
    }
}
