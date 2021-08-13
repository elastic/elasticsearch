/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.RequestConverters.Params;
import org.elasticsearch.client.asyncsearch.DeleteAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.GetAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;

final class AsyncSearchRequestConverters {

    static Request submitAsyncSearch(SubmitAsyncSearchRequest asyncSearchRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addCommaSeparatedPathParts(
                asyncSearchRequest.getIndices())
                .addPathPartAsIs("_async_search").build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new RequestConverters.Params();
        // add all typical search params and search request source as body
        addSearchRequestParams(params, asyncSearchRequest);
        if (asyncSearchRequest.getSearchSource() != null) {
            request.setEntity(RequestConverters.createEntity(asyncSearchRequest.getSearchSource(), REQUEST_BODY_CONTENT_TYPE));
        }
        // set async search submit specific parameters
        if (asyncSearchRequest.isKeepOnCompletion() != null) {
            params.putParam("keep_on_completion", asyncSearchRequest.isKeepOnCompletion().toString());
        }
        if (asyncSearchRequest.getKeepAlive() != null) {
            params.putParam("keep_alive", asyncSearchRequest.getKeepAlive().getStringRep());
        }
        if (asyncSearchRequest.getWaitForCompletionTimeout() != null) {
            params.putParam("wait_for_completion_timeout", asyncSearchRequest.getWaitForCompletionTimeout().getStringRep());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static void addSearchRequestParams(Params params, SubmitAsyncSearchRequest request) {
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withRouting(request.getRouting());
        params.withPreference(request.getPreference());
        params.withIndicesOptions(request.getIndicesOptions());
        params.withSearchType(request.getSearchType().name().toLowerCase(Locale.ROOT));
        params.withMaxConcurrentShardRequests(request.getMaxConcurrentShardRequests());
        if (request.getRequestCache() != null) {
            params.withRequestCache(request.getRequestCache());
        }
        if (request.getAllowPartialSearchResults() != null) {
            params.withAllowPartialResults(request.getAllowPartialSearchResults());
        }
        if (request.getBatchedReduceSize() != null) {
            params.withBatchedReduceSize(request.getBatchedReduceSize());
        }
    }

    static Request getAsyncSearch(GetAsyncSearchRequest asyncSearchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_async_search")
                .addPathPart(asyncSearchRequest.getId())
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        Params params = new RequestConverters.Params();
        if (asyncSearchRequest.getKeepAlive() != null) {
            params.putParam("keep_alive", asyncSearchRequest.getKeepAlive().getStringRep());
        }
        if (asyncSearchRequest.getWaitForCompletion() != null) {
            params.putParam("wait_for_completion_timeout", asyncSearchRequest.getWaitForCompletion().getStringRep());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteAsyncSearch(DeleteAsyncSearchRequest deleteAsyncSearchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_async_search")
                .addPathPart(deleteAsyncSearchRequest.getId())
                .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }
}
