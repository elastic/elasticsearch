/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.client.asyncsearch.DeleteAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.GetAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class AsyncSearchClient {
    private final RestHighLevelClient restHighLevelClient;

    AsyncSearchClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Submit a new async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AsyncSearchResponse submit(SubmitAsyncSearchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, AsyncSearchRequestConverters::submitAsyncSearch, options,
                AsyncSearchResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously submit a new async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-get-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable submitAsync(SubmitAsyncSearchRequest request, RequestOptions options,
                                               ActionListener<AsyncSearchResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, AsyncSearchRequestConverters::submitAsyncSearch, options,
                AsyncSearchResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get an async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     *
     */
    public AsyncSearchResponse get(GetAsyncSearchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, AsyncSearchRequestConverters::getAsyncSearch, options,
                AsyncSearchResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get an async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAsync(GetAsyncSearchRequest request, RequestOptions options,
            ActionListener<AsyncSearchResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, AsyncSearchRequestConverters::getAsyncSearch, options,
                AsyncSearchResponse::fromXContent, listener, emptySet());
    }

    /**
     * Delete an async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse delete(DeleteAsyncSearchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, AsyncSearchRequestConverters::deleteAsyncSearch, options,
                AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete an async search request.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/async-search.html"> the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteAsync(DeleteAsyncSearchRequest request, RequestOptions options,
                                               ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, AsyncSearchRequestConverters::deleteAsyncSearch, options,
                AcknowledgedResponse::fromXContent, listener, emptySet());
    }

}
