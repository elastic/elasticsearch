/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutViewAction extends BaseRestHandler {
    private static final String VIEW_INDEX_ABSTRACTION = "view_index_abstraction";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/view/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_view";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            PutViewAction.Request req = new PutViewAction.Request(
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                View.parser(request.param("name")).parse(parser, null)
            );
            return channel -> client.execute(PutViewAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(VIEW_INDEX_ABSTRACTION);
    }
}
