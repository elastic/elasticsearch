/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
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
import java.util.Collections;
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

        String[] stringWaitForCheckpointsLists = Strings.tokenizeToStringArray(request.param("wait_for_checkpoints", ""), ";");
        if (stringWaitForCheckpointsLists.length > 0 && stringWaitForCheckpointsLists.length != multiSearchRequest.requests().size()) {
            throw new IllegalArgumentException(
                "Must provide same number of wait_for_checkpoints as requests provided [number of wait_for_checkpoints="
                    + stringWaitForCheckpointsLists.length
                    + ", number of requests="
                    + multiSearchRequest.requests().size()
                    + "]."
            );
        }

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        if (indices.length > 1) {
            throw new IllegalArgumentException(
                "Fleet search API only supports searching a single index. Found: [" + Arrays.toString(indices) + "]."
            );
        }

        int j = 0;
        for (SearchRequest searchRequest : multiSearchRequest.requests()) {
            String[] stringWaitForCheckpoints = Strings.splitStringByCommaToArray(stringWaitForCheckpointsLists[j]);
            final long[] waitForCheckpoints = new long[stringWaitForCheckpoints.length];
            for (int i = 0; i < stringWaitForCheckpoints.length; ++i) {
                waitForCheckpoints[i] = Long.parseLong(stringWaitForCheckpoints[i]);
            }
            searchRequest.setWaitForCheckpoints(Collections.singletonMap(indices[0], waitForCheckpoints));
            ++j;
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
