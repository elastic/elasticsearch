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
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.util.List;
import java.util.Set;

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
        final var actionRequest = new GetShutdownStatusAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            Strings.commaDelimitedListToStringArray(request.param("nodeId"))
        );
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetShutdownStatusAction.INSTANCE,
            actionRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(RestUtils.REST_MASTER_TIMEOUT_PARAM, "nodeId");

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }
}
