/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.enterprisesearch;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.BaseRestHandler;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Example of adding a cat action with a plugin.
 */
public class XSearchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_xsearch"));
    }

    @Override
    public String getName() {
        return "xsearch_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String message = request.param("message", "Hello XSearch");
        return channel -> {
            try {
                channel.sendResponse(new RestResponse(RestStatus.OK, message));
            } catch (final Exception e) {
                channel.sendResponse(new RestResponse(channel, e));
            }
        };
    }
}
