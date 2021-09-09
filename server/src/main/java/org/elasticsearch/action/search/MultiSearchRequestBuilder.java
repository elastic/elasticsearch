/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request builder for multiple search requests.
 */
public class MultiSearchRequestBuilder extends ActionRequestBuilder<MultiSearchRequest, MultiSearchResponse> {


    public MultiSearchRequestBuilder(ElasticsearchClient client, MultiSearchAction action) {
        super(client, action, new MultiSearchRequest());
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     * <p>
     * If ignoreIndices has been set on the search request, then the indicesOptions of the multi search request
     * will not be used (if set).
     */
    public MultiSearchRequestBuilder add(SearchRequest request) {
        if (request.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed()
            && request().indicesOptions() != IndicesOptions.strictExpandOpenAndForbidClosed()) {
            request.indicesOptions(request().indicesOptions());
        }

        super.request.add(request);
        return this;
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequestBuilder add(SearchRequestBuilder request) {
        if (request.request().indicesOptions().equals(SearchRequest.DEFAULT_INDICES_OPTIONS)
                && request().indicesOptions().equals(SearchRequest.DEFAULT_INDICES_OPTIONS) == false) {
            request.request().indicesOptions(request().indicesOptions());
        }

        super.request.add(request);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard indices expressions.
     * For example indices that don't exist.
     * <p>
     * Invoke this method before invoking {@link #add(SearchRequestBuilder)}.
     */
    public MultiSearchRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchRequestBuilder setMaxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        request().maxConcurrentSearchRequests(maxConcurrentSearchRequests);
        return this;
    }
}
