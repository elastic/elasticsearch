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
