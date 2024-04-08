/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetShutdownStatusAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_shutdown_status";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_nodes/{nodeId}/shutdown"),
            new Route(RestRequest.Method.GET, "/_nodes/shutdown")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodeIds = Strings.commaDelimitedListToStringArray(request.param("nodeId"));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeIds),
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
