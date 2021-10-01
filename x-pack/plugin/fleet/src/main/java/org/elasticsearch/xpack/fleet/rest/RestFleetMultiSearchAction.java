/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFleetMultiSearchAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    public RestFleetMultiSearchAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "fleet_msearch_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_fleet/_msearch"), new Route(POST, "/{index}/_fleet/_msearch"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MultiSearchRequest multiSearchRequest = RestMultiSearchAction.parseRequest(
            request,
            client.getNamedWriteableRegistry(),
            allowExplicitIndex
        );

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        if (indices.length > 1) {
            throw new IllegalArgumentException(
                "Fleet search API only supports searching a single index. Found: [" + Arrays.toString(indices) + "]."
            );
        }

        return channel -> {
            final RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(MultiSearchAction.INSTANCE, multiSearchRequest, new RestToXContentListener<>(channel));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RestSearchAction.RESPONSE_PARAMS;
    }
}
