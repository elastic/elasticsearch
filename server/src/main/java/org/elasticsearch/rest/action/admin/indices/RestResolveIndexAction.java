/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestResolveIndexAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "resolve_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_resolve/index/{name}"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("name"));
        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            indices,
            IndicesOptions.fromRequest(request, ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS)
        );
        return channel -> client.admin().indices().resolveIndex(resolveRequest, new RestToXContentListener<>(channel));
    }
}
