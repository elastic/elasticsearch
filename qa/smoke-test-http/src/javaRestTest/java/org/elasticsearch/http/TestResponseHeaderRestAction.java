/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class TestResponseHeaderRestAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_protected"));
    }

    @Override
    public String getName() {
        return "test_response_header_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if ("password".equals(request.header("Secret"))) {
            RestResponse response = new RestResponse(RestStatus.OK, "Access granted");
            response.addHeader("Secret", "granted");
            return channel -> channel.sendResponse(response);
        } else {
            RestResponse response = new RestResponse(RestStatus.UNAUTHORIZED, "Access denied");
            response.addHeader("Secret", "required");
            return channel -> channel.sendResponse(response);
        }
    }
}
