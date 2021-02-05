/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search.persistent;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchAction;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.search.RestSearchAction.parseSearchRequest;

public class RestSubmitPersistentSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "submit_persistent_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_persistent_search"),
            new Route(POST, "/{index}/_persistent_search")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser ->
            parseSearchRequest(searchRequest, request, parser, client.getNamedWriteableRegistry(), setSize));

        return channel -> {
            RestStatusToXContentListener<SubmitPersistentSearchResponse> listener = new RestStatusToXContentListener<>(channel);
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SubmitPersistentSearchAction.INSTANCE, searchRequest, listener);
        };
    }
}
