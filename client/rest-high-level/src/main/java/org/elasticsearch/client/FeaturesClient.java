/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.snapshots.GetFeaturesRequest;
import org.elasticsearch.client.snapshots.GetFeaturesResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Snapshot API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/features-apis.html">Snapshot API on elastic.co</a>
 */
public class FeaturesClient {
    private final RestHighLevelClient restHighLevelClient;

    FeaturesClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Get a list of features which can be included in a snapshot as feature states.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-features-api.html"> Get Snapshottable
     * Features API on elastic.co</a>
     *
     * @param getFeaturesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetFeaturesResponse getFeatures(GetFeaturesRequest getFeaturesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getFeaturesRequest,
            FeaturesRequestConverters::getFeatures,
            options,
            GetFeaturesResponse::parse,
            emptySet()
        );
    }

    /**
     * Asynchronously get a list of features which can be included in a snapshot as feature states.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-features-api.html"> Get Snapshottable
     * Features API on elastic.co</a>
     *
     * @param getFeaturesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getFeaturesAsync(
        GetFeaturesRequest getFeaturesRequest, RequestOptions options,
        ActionListener<GetFeaturesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getFeaturesRequest,
            FeaturesRequestConverters::getFeatures,
            options,
            GetFeaturesResponse::parse,
            listener,
            emptySet()
        );
    }
}
