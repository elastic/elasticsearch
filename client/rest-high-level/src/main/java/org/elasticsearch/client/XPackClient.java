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
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.client.xpack.XPackInfoResponse;
import org.elasticsearch.client.xpack.XPackUsageRequest;
import org.elasticsearch.client.xpack.XPackUsageResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic Licensed X-Pack APIs that are shipped with the
 * default distribution of Elasticsearch. All of these APIs will 404 if run
 * against the OSS distribution of Elasticsearch.
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html">
 * REST APIs on elastic.co</a> for more information.
 */
public final class XPackClient {

    private final RestHighLevelClient restHighLevelClient;

    XPackClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Fetch information about X-Pack from the cluster.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/info-api.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public XPackInfoResponse info(XPackInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, XPackRequestConverters::info, options,
            XPackInfoResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously fetch information about X-Pack from the cluster.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/info-api.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable infoAsync(XPackInfoRequest request, RequestOptions options,
                                 ActionListener<XPackInfoResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, XPackRequestConverters::info, options,
            XPackInfoResponse::fromXContent, listener, emptySet());
    }

    /**
     * Fetch usage information about X-Pack features from the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public XPackUsageResponse usage(XPackUsageRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, XPackRequestConverters::usage, options,
            XPackUsageResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously fetch usage information about X-Pack features from the cluster.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable usageAsync(XPackUsageRequest request, RequestOptions options, ActionListener<XPackUsageResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, XPackRequestConverters::usage, options,
            XPackUsageResponse::fromXContent, listener, emptySet());
    }
}
