/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlStatsRequest;
import org.elasticsearch.client.eql.EqlStatsResponse;

import java.io.IOException;
import java.util.Collections;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic EQL related functions
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/eql.html">
 * EQL APIs on elastic.co</a> for more information.
 */
public final class EqlClient {

    private final RestHighLevelClient restHighLevelClient;

    EqlClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Executes the eql search query.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-search.html">
     * the docs</a> for more.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public EqlSearchResponse search(EqlSearchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EqlRequestConverters::search,
            options,
            EqlSearchResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the eql search query.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-search.html">
     * the docs</a> for more.
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable searchAsync(EqlSearchRequest request,
                              RequestOptions options,
                              ActionListener<EqlSearchResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EqlRequestConverters::search,
            options,
            EqlSearchResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Get the eql stats
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-stats.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public EqlStatsResponse stats(EqlStatsRequest request, RequestOptions options)  throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EqlRequestConverters::stats,
            options,
            EqlStatsResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously get the eql stats
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-stats.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable statsAsync(EqlStatsRequest request, RequestOptions options, ActionListener<EqlStatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            EqlRequestConverters::stats,
            options,
            EqlStatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }
}
