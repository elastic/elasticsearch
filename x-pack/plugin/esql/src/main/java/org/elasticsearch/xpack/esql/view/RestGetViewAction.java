/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetViewAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/view/{name}"), new Route(GET, "/_query/view"));
    }

    @Override
    public String getName() {
        return "esql_get_view";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetViewAction.Request req = new GetViewAction.Request(RestUtils.getMasterNodeTimeout(request));
        var requestedViews = Strings.splitStringByCommaToArray(request.param("name"));
        req.indices(isGetAllViews(requestedViews) ? new String[] { "*" } : requestedViews);

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetViewAction.INSTANCE,
            req,
            new RestToXContentListener<>(channel)
        );
    }

    private boolean isGetAllViews(String[] requestedViews) {
        return requestedViews.length == 0 || requestedViews[0].equals(Metadata.ALL);
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("view_index_abstraction");
    }
}
