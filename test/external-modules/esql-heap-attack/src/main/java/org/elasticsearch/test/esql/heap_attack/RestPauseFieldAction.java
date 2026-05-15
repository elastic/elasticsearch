/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestPauseFieldAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "pause_field";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_pause_field/block"), new Route(POST, "/_pause_field/unblock"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean block = request.path().endsWith("/block");
        return channel -> client.admin().cluster().execute(
            TransportPauseFieldAction.TYPE,
            new TransportPauseFieldAction.Request(block),
            new RestActionListener<>(channel) {
                @Override
                protected void processResponse(TransportPauseFieldAction.Response response) throws Exception {
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject().field("acknowledged", true).endObject();
                        channel.sendResponse(new RestResponse(RestStatus.OK, builder));
                    }
                }
            }
        );
    }
}
