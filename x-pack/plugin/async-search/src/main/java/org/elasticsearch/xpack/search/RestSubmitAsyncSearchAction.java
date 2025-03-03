/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.search.RestSearchAction.parseSearchRequest;

@ServerlessScope(Scope.PUBLIC)
public final class RestSubmitAsyncSearchAction extends BaseRestHandler {
    static final String TYPED_KEYS_PARAM = "typed_keys";
    static final Set<String> RESPONSE_PARAMS = Collections.singleton(TYPED_KEYS_PARAM);

    private final SearchUsageHolder searchUsageHolder;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestSubmitAsyncSearchAction(SearchUsageHolder searchUsageHolder, Predicate<NodeFeature> clusterSupportsFeature) {
        this.searchUsageHolder = searchUsageHolder;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_async_search"), new Route(POST, "/{index}/_async_search"));
    }

    @Override
    public String getName() {
        return "async_search_submit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (client.threadPool() != null && client.threadPool().getThreadContext() != null) {
            client.threadPool().getThreadContext().setErrorTraceTransportHeader(request);
        }
        SubmitAsyncSearchRequest submit = new SubmitAsyncSearchRequest();
        IntConsumer setSize = size -> submit.getSearchRequest().source().size(size);
        // for simplicity, we share parsing with ordinary search. That means a couple of unsupported parameters, like scroll
        // and pre_filter_shard_size get set to the search request although the REST spec don't list
        // them as supported. We rely on SubmitAsyncSearchRequest#validate to fail in case they are set.
        // Note that ccs_minimize_roundtrips is also set this way, which is a supported option.
        request.withContentOrSourceParamParserOrNull(
            parser -> parseSearchRequest(submit.getSearchRequest(), request, parser, clusterSupportsFeature, setSize, searchUsageHolder)
        );

        if (request.hasParam("wait_for_completion_timeout")) {
            submit.setWaitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout", submit.getWaitForCompletionTimeout()));
        }
        if (request.hasParam("keep_alive")) {
            submit.setKeepAlive(request.paramAsTime("keep_alive", submit.getKeepAlive()));
        }
        if (request.hasParam("keep_on_completion")) {
            submit.setKeepOnCompletion(request.paramAsBoolean("keep_on_completion", submit.isKeepOnCompletion()));
        }
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SubmitAsyncSearchAction.INSTANCE, submit, new RestRefCountedChunkedToXContentListener<>(channel) {
                @Override
                protected RestStatus getRestStatus(AsyncSearchResponse asyncSearchResponse) {
                    return asyncSearchResponse.status();
                }
            });
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
