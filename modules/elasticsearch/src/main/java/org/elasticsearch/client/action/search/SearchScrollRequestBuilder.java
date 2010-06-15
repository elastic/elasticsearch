/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.client.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;

/**
 * A search scroll action request builder.
 *
 * @author kimchy (shay.banon)
 */
public class SearchScrollRequestBuilder {

    private final InternalClient client;

    private final SearchScrollRequest request;

    public SearchScrollRequestBuilder(InternalClient client, String scrollId) {
        this.client = client;
        this.request = new SearchScrollRequest(scrollId);
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchScrollRequestBuilder setOperationThreading(SearchOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public SearchScrollRequestBuilder listenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchScrollRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * Executes the operation asynchronously and returns a future.
     */
    public ListenableActionFuture<SearchResponse> execute() {
        PlainListenableActionFuture<SearchResponse> future = new PlainListenableActionFuture<SearchResponse>(request.listenerThreaded(), client.threadPool());
        client.searchScroll(request, future);
        return future;
    }

    /**
     * Executes the operation asynchronously with the provided listener.
     */
    public void execute(ActionListener<SearchResponse> listener) {
        client.searchScroll(request, listener);
    }
}
