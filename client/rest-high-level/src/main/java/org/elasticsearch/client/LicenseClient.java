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
import org.elasticsearch.protocol.xpack.license.PutLicenseRequest;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic License-related methods
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/licensing-apis.html">
 * X-Pack Licensing APIs on elastic.co</a> for more information.
 */
public class LicenseClient {

    private final RestHighLevelClient restHighLevelClient;

    LicenseClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Updates license for the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutLicenseResponse putLicense(PutLicenseRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::putLicense, options,
            PutLicenseResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously updates license for the cluster cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putLicenseAsync(PutLicenseRequest request, RequestOptions options, ActionListener<PutLicenseResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::putLicense, options,
            PutLicenseResponse::fromXContent, listener, emptySet());
    }

}
