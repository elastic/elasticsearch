/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRollupSearchAction extends BaseRestHandler {

    private static final Set<String> RESPONSE_PARAMS = Set.of(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestRollupSearchAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "_rollup_search"),
            new Route(POST, "_rollup_search"),
            new Route(GET, "{index}/_rollup_search"),
            new Route(POST, "{index}/_rollup_search")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        restRequest.withContentOrSourceParamParserOrNull(
            parser -> RestSearchAction.parseSearchRequest(
                searchRequest,
                restRequest,
                parser,
                clusterSupportsFeature,
                size -> searchRequest.source().size(size)
            )
        );
        RestSearchAction.validateSearchRequest(restRequest, searchRequest);
        return channel -> client.execute(
            RollupSearchAction.INSTANCE,
            searchRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    @Override
    public String getName() {
        return "rollup_search_action";
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
