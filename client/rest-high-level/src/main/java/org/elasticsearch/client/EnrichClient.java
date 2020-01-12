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
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.enrich.DeletePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyResponse;
import org.elasticsearch.client.enrich.GetPolicyRequest;
import org.elasticsearch.client.enrich.GetPolicyResponse;
import org.elasticsearch.client.enrich.PutPolicyRequest;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;

import java.io.IOException;
import java.util.Collections;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for
 * accessing the Elastic enrich related methods
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-apis.html">
 * X-Pack Enrich Policy APIs on elastic.co</a> for more information.
 */
public final class EnrichClient {

    private final RestHighLevelClient restHighLevelClient;

    EnrichClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Executes the put policy api, which stores an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link PutPolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putPolicy(PutPolicyRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EnrichRequestConverters::putPolicy,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the put policy api, which stores an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/put-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link PutPolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putPolicyAsync(PutPolicyRequest request,
                                      RequestOptions options,
                                      ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EnrichRequestConverters::putPolicy,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Executes the delete policy api, which deletes an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link DeletePolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deletePolicy(DeletePolicyRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EnrichRequestConverters::deletePolicy,
            options,
            AcknowledgedResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the delete policy api, which deletes an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link DeletePolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deletePolicyAsync(DeletePolicyRequest request,
                                         RequestOptions options,
                                         ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EnrichRequestConverters::deletePolicy,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Executes the get policy api, which retrieves an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link PutPolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetPolicyResponse getPolicy(GetPolicyRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EnrichRequestConverters::getPolicy,
            options,
            GetPolicyResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the get policy api, which retrieves an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/get-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link PutPolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getPolicyAsync(GetPolicyRequest request,
                               RequestOptions options,
                               ActionListener<GetPolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EnrichRequestConverters::getPolicy,
            options,
            GetPolicyResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Executes the enrich stats api, which retrieves enrich related stats.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-stats-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link StatsRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public StatsResponse stats(StatsRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EnrichRequestConverters::stats,
            options,
            StatsResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the enrich stats api, which retrieves enrich related stats.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-stats-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link StatsRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable statsAsync(StatsRequest request,
                                  RequestOptions options,
                                  ActionListener<StatsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EnrichRequestConverters::stats,
            options,
            StatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Executes the execute policy api, which executes an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/execute-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link ExecutePolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ExecutePolicyResponse executePolicy(ExecutePolicyRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            EnrichRequestConverters::executePolicy,
            options,
            ExecutePolicyResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the execute policy api, which executes an enrich policy.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/execute-enrich-policy-api.html">
     * the docs</a> for more.
     *
     * @param request the {@link ExecutePolicyRequest}
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable executePolicyAsync(ExecutePolicyRequest request,
                                          RequestOptions options,
                                          ActionListener<ExecutePolicyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            EnrichRequestConverters::executePolicy,
            options,
            ExecutePolicyResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }
}
