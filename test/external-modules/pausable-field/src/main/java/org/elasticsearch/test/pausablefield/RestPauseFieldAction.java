/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.pausablefield;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestPauseFieldAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "pause_field";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_pause_field/block"),
            new Route(POST, "/_pause_field/stop"),
            new Route(POST, "/_pause_field/admit"),
            new Route(POST, "/_pause_field/unblock"),
            new Route(GET, "/_pause_field/status")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final TransportPauseFieldAction.Mode mode = switch (request.path()) {
            case "/_pause_field/block" -> TransportPauseFieldAction.Mode.BLOCK;
            case "/_pause_field/stop" -> TransportPauseFieldAction.Mode.STOP;
            case "/_pause_field/admit" -> TransportPauseFieldAction.Mode.ADMIT;
            case "/_pause_field/unblock" -> TransportPauseFieldAction.Mode.UNBLOCK;
            case "/_pause_field/status" -> TransportPauseFieldAction.Mode.STATUS;
            default -> throw new IllegalArgumentException("unexpected path [" + request.path() + "]");
        };
        return channel -> client.admin()
            .cluster()
            .execute(TransportPauseFieldAction.TYPE, new TransportPauseFieldAction.Request(mode), new RestActionListener<>(channel) {
                @Override
                protected void processResponse(TransportPauseFieldAction.Response response) throws Exception {
                    int parked = response.getNodes().stream().mapToInt(TransportPauseFieldAction.NodeResponse::parked).sum();
                    int gated = response.getNodes().stream().mapToInt(TransportPauseFieldAction.NodeResponse::gated).sum();
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject().field("acknowledged", true).field("parked", parked).field("gated", gated).endObject();
                        channel.sendResponse(new RestResponse(RestStatus.OK, builder));
                    }
                }
            });
    }
}
