/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

/**
 * A request builder for multiple search requests.
 */
public class MultiSearchRequestBuilder extends ActionRequestBuilder<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> {

    public MultiSearchRequestBuilder(Client client) {
        super((InternalClient) client, new MultiSearchRequest());
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     * <p/>
     * If ignoreIndices has been set on the search request, then the ignoreIndices of the multi search request
     * will not be used (if set).
     */
    public MultiSearchRequestBuilder add(SearchRequest request) {
        if (request.ignoreIndices() == IgnoreIndices.DEFAULT && request().ignoreIndices() != IgnoreIndices.DEFAULT) {
            request.ignoreIndices(request().ignoreIndices());
        }

        super.request.add(request);
        return this;
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequestBuilder add(SearchRequestBuilder request) {
        if (request.request().ignoreIndices() == IgnoreIndices.DEFAULT && request().ignoreIndices() != IgnoreIndices.DEFAULT) {
            request.request().ignoreIndices(request().ignoreIndices());
        }

        super.request.add(request);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore. For example indices that don't exist.
     * Invoke this method before invoking {@link #add(SearchRequestBuilder)}.
     */
    public MultiSearchRequestBuilder setIgnoreIndices(IgnoreIndices ignoreIndices) {
        request().ignoreIndices(ignoreIndices);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<MultiSearchResponse> listener) {
        ((Client) client).multiSearch(request, listener);
    }
}
