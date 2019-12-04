/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.io.IOException;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.search.RestSearchAction.parseSearchRequest;

public final class RestSubmitAsyncSearchAction extends BaseRestHandler {
    RestSubmitAsyncSearchAction(RestController controller) {
        controller.registerHandler(POST, "/_async_search", this);
        controller.registerHandler(GET, "/_async_search", this);
        controller.registerHandler(POST, "/{index}/_async_search", this);
        controller.registerHandler(GET, "/{index}/_async_search", this);
    }

    @Override
    public String getName() {
        return "async_search_submit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SubmitAsyncSearchRequest searchRequest = new SubmitAsyncSearchRequest();
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser -> parseSearchRequest(searchRequest, request, parser, setSize));
        searchRequest.setWaitForCompletion(request.paramAsTime("wait_for_completion", TimeValue.timeValueSeconds(1)));

        ActionRequestValidationException validationException = searchRequest.validate();
        if (validationException != null) {
            throw validationException;
        }
        return channel -> {
            RestStatusToXContentListener<AsyncSearchResponse> listener = new RestStatusToXContentListener<>(channel);
            client.executeLocally(SubmitAsyncSearchAction.INSTANCE, searchRequest, listener);
        };
    }
}
