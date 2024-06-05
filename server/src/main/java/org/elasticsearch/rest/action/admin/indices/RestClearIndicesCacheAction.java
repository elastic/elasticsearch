/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(value = Scope.INTERNAL)
public class RestClearIndicesCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_cache/clear"), new Route(POST, "/{index}/_cache/clear"));
    }

    @Override
    public String getName() {
        return "clear_indices_cache_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        clearIndicesCacheRequest.indicesOptions(IndicesOptions.fromRequest(request, clearIndicesCacheRequest.indicesOptions()));
        fromRequest(request, clearIndicesCacheRequest);
        return channel -> client.admin().indices().clearCache(clearIndicesCacheRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    public static ClearIndicesCacheRequest fromRequest(final RestRequest request, ClearIndicesCacheRequest clearIndicesCacheRequest) {
        clearIndicesCacheRequest.queryCache(request.paramAsBoolean("query", clearIndicesCacheRequest.queryCache()));
        clearIndicesCacheRequest.requestCache(request.paramAsBoolean("request", clearIndicesCacheRequest.requestCache()));
        clearIndicesCacheRequest.fieldDataCache(request.paramAsBoolean("fielddata", clearIndicesCacheRequest.fieldDataCache()));
        clearIndicesCacheRequest.fields(request.paramAsStringArray("fields", clearIndicesCacheRequest.fields()));
        return clearIndicesCacheRequest;
    }
}
