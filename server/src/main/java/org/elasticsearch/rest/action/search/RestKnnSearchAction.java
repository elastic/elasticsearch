/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.vectors.KnnSearchRequestBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * The REST action for handling kNN searches. Currently, it just parses
 * the REST request into a search request and calls the search action.
 */
public class RestKnnSearchAction extends BaseRestHandler {

    public RestKnnSearchAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_knn_search"), new Route(POST, "{index}/_knn_search"));
    }

    @Override
    public String getName() {
        return "knn_search_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        // This will allow to cancel the search request if the http channel is closed
        RestCancellableNodeClient cancellableNodeClient = new RestCancellableNodeClient(client, restRequest.getHttpChannel());
        KnnSearchRequestBuilder request = KnnSearchRequestBuilder.parseRestRequest(restRequest);

        SearchRequestBuilder searchRequestBuilder = cancellableNodeClient.prepareSearch();
        request.build(searchRequestBuilder);

        return channel -> searchRequestBuilder.execute(new RestStatusToXContentListener<>(channel));
    }
}
