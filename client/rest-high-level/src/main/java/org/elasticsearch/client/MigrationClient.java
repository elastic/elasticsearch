/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.DeprecationInfoResponse;

import java.io.IOException;
import java.util.Collections;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic License-related methods
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/migration-api.html">
 * X-Pack Migration APIs on elastic.co</a> for more information.
 */
public final class MigrationClient {

    private final RestHighLevelClient restHighLevelClient;

    MigrationClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Get deprecation info for one or more indices
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeprecationInfoResponse getDeprecationInfo(DeprecationInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, MigrationRequestConverters::getDeprecationInfo, options,
            DeprecationInfoResponse::fromXContent, Collections.emptySet());
    }

    /**
     * Asynchronously get deprecation info for one or more indices
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDeprecationInfoAsync(DeprecationInfoRequest request, RequestOptions options,
                                               ActionListener<DeprecationInfoResponse> listener)  {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, MigrationRequestConverters::getDeprecationInfo, options,
            DeprecationInfoResponse::fromXContent, listener, Collections.emptySet());
    }
}
