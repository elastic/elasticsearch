/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.transport;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestComputeAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "compute_engine";
    }

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(POST, "/_compute").build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ComputeRequest computeRequest = ComputeRequest.fromXContent(request.contentParser());
        return channel -> client.execute(ComputeAction.INSTANCE, computeRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(ComputeResponse computeResponse, XContentBuilder builder) throws Exception {
                return new RestResponse(RestStatus.OK, computeResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }
}
