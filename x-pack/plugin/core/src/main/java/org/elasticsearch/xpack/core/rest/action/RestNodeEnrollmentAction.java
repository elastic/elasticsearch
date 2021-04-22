/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.action.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.action.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.action.NodeEnrollmentResponse;

import java.io.IOException;
import java.util.List;

public final class RestNodeEnrollmentAction extends BaseRestHandler {

    @Override public String getName() {
        return "node_enroll_action";
    }

    @Override public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "_cluster/enroll_node")
        );
    }

    @Override protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannel -> client.execute(NodeEnrollmentAction.INSTANCE,
            new NodeEnrollmentRequest(),
            new RestBuilderListener<NodeEnrollmentResponse>(restChannel) {
                @Override public RestResponse buildResponse(
                    NodeEnrollmentResponse nodeEnrollmentResponse, XContentBuilder builder) throws Exception {
                    nodeEnrollmentResponse.toXContent(builder, channel.request());
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
    }
}
