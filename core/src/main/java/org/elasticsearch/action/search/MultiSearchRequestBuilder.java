/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request builder for multiple search requests.
 */
public class MultiSearchRequestBuilder extends ActionRequestBuilder<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> {

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
        if (request.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed() && request().indicesOptions() != IndicesOptions.strictExpandOpenAndForbidClosed()) {
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
        if (request.request().indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed() && request().indicesOptions() != IndicesOptions.strictExpandOpenAndForbidClosed()) {
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
