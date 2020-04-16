/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.SubmitAsyncEqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.SubmitAsyncEqlSearchRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.eql.plugin.RestEqlSearchAction.prepareEqlSearchRequest;

public final class RestSubmitAsyncEqlSearchAction extends BaseRestHandler {
    static final String TYPED_KEYS_PARAM = "typed_keys";
    static final Set<String> RESPONSE_PARAMS = Collections.singleton(TYPED_KEYS_PARAM);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_eql/async_search"));
    }

    @Override
    public String getName() {
        return "async_eql_search_submit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SubmitAsyncEqlSearchRequest submit = new SubmitAsyncEqlSearchRequest(
            prepareEqlSearchRequest(request)
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
            RestStatusToXContentListener<AsyncEqlSearchResponse> listener = new RestStatusToXContentListener<>(channel);
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SubmitAsyncEqlSearchAction.INSTANCE, submit, listener);
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
