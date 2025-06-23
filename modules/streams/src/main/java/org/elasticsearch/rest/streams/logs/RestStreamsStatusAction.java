/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestStreamsStatusAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "streams_status_action";
    }

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(new RestHandler.Route(GET, "/_streams/status"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        StreamsStatusAction.Request statusRequest = new StreamsStatusAction.Request();
        return restChannel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            StreamsStatusAction.INSTANCE,
            statusRequest,
            new RestToXContentListener<>(restChannel)
        );
    }

}
