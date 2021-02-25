/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestClosePointInTimeAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_pit"));
    }

    @Override
    public String getName() {
        return "close_point_in_time";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClosePointInTimeRequest clearRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            clearRequest = ClosePointInTimeRequest.fromXContent(parser);
        }
        return channel -> client.execute(ClosePointInTimeAction.INSTANCE, clearRequest, new RestStatusToXContentListener<>(channel));
    }
}
